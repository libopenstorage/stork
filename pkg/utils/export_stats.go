package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/sirupsen/logrus"
)

const (
	AetosStatsURL = "https://aetos.pwx.purestorage.com/dashboard/stats?stats_type=MigrationDemoFinal&limit=100"
)

var StorkVersion string
var PortworxVersion string

type StatsExportType struct {
	//	id        OidType            `json: "_id",omitempty`
	Name      string             `json: "name",omitempty`
	Product   string             `json: "product",omitempty`
	Version   string             `json: "version",omitempty`
	StatsType string             `json: "statsType",omitempty`
	Data      MigrationStatsType `json: "data",omitempty`
}

type MigrationStatsType struct {
	CreatedOn                       string      `json: "created",omitempty`
	TotalNumberOfVolumes            json.Number `json: "totalNumberOfVolumes",omitempty`
	NumOfMigratedVolumes            json.Number `json: "numOfMigratedVolumes",omitempty`
	TotalNumberOfResources          json.Number `json: "totalNumberOfResources",omitempty`
	NumOfMigratedResources          json.Number `json: "numOfMigratedResources",omitempty`
	TotalBytesMigrated              json.Number `json: "totalBytesMigrated",omitempty`
	ElapsedTimeForVolumeMigration   string      `json: "elapsedTimeForVolumeMigration",omitempty`
	ElapsedTimeForResourceMigration string      `json: "elapsedTimeForResourceMigration",omitempty`
	Application                     string      `json: "application",omitempty`
	StorkVersion                    string      `json: "storkVersion",omitempty`
	PortworxVersion                 string      `json: "portworxVersion",omitempty`
}

//type OidType struct {
//	oid string `json: "$oid"`
//}

type AllStats []StatsExportType

func GetMigrationStatsFromAetos(url string) ([]StatsExportType, error) {
	client := http.Client{Timeout: time.Second * 3}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error querying Aetos for stats: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")

	q := req.URL.Query()
	q.Add("format", "json")
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error querying Aetos metadata: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("error querying Aetos metadata: Code %d returned for url %s", resp.StatusCode, req.URL)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error querying Aetos metadata: %v", err)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("error querying Aetos metadata: Empty response")
	}

	//fmt.Printf("\nBody of response: %v\n\n", string(body))
	data := AllStats{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, fmt.Errorf("error parsing Aetos metadata: %v", err)
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			logrus.Errorf("Error closing body when getting Aetos data: %v", err)
		}
	}()
	return data, nil
}

func WriteMigrationStatsToAetos(data StatsExportType) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transCfg}

	resp, err := client.Post("https://aetos.pwx.purestorage.com/dashboard/stats", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("post request to Aetos failed: %v", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		//Failed to read response.
		return fmt.Errorf("response from Aetos failed: %v", err)
	}
	jsonStr := string(respBody)

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		logrus.Infof("Stats successfully pushed to DB. Response: ", jsonStr)
	} else {
		return fmt.Errorf("post failed with Reponse status: %s, %s", resp.Status, jsonStr)
	}

	return nil
}

func MockStat() StatsExportType {
	mockStat := StatsExportType{
		Name:      "stork_integration_test",
		Product:   "stork",
		StatsType: "migration_stats_mock",
		Version:   "v1alpha1",
		Data: MigrationStatsType{
			TotalNumberOfVolumes:            "1",
			NumOfMigratedVolumes:            "1",
			TotalNumberOfResources:          "5",
			NumOfMigratedResources:          "1",
			TotalBytesMigrated:              "12345",
			ElapsedTimeForVolumeMigration:   "11.1111s",
			ElapsedTimeForResourceMigration: "12321.3453s",
			Application:                     "mock_app",
			StorkVersion:                    StorkVersion,
			PortworxVersion:                 PortworxVersion,
		},
	}
	return mockStat
}

func NewStat() StatsExportType {
	newStat := StatsExportType{
		Name:      "stork_integration_test",
		Product:   "stork",
		StatsType: "MigrationDemoFinal",
		Version:   "v1alpha1",
		Data: MigrationStatsType{
			TotalNumberOfVolumes:            "",
			Application:                     "",
			NumOfMigratedVolumes:            "",
			TotalNumberOfResources:          "",
			NumOfMigratedResources:          "",
			TotalBytesMigrated:              "",
			ElapsedTimeForVolumeMigration:   "",
			ElapsedTimeForResourceMigration: "",
			StorkVersion:                    StorkVersion,
			PortworxVersion:                 PortworxVersion,
		},
	}
	return newStat
}

func GetExportableStatsFromMigrationObject(mig *storkv1.Migration, statArgs ...string) StatsExportType {
	exportStats := NewStat()

	for i := 0; i < len(statArgs); i++ {
		switch i {
		case 0:
			exportStats.Name = statArgs[0]
		case 1:
			exportStats.Product = statArgs[1]
		case 2:
			exportStats.StatsType = statArgs[2]
		case 3:
			exportStats.Version = statArgs[3]
		}
	}
	exportStats.Data.Application = getResourceNamesFromMigration(mig)

	exportStats.Data.CreatedOn = (mig.GetCreationTimestamp()).Format("2006-01-02 15:04:05")

	exportStats.Data.TotalNumberOfVolumes = json.Number(strconv.FormatUint(mig.Status.Summary.TotalNumberOfVolumes, 10))

	exportStats.Data.NumOfMigratedVolumes = json.Number(strconv.FormatUint(mig.Status.Summary.NumberOfMigratedVolumes, 10))

	exportStats.Data.TotalNumberOfResources = json.Number(strconv.FormatUint(mig.Status.Summary.TotalNumberOfResources, 10))

	exportStats.Data.NumOfMigratedResources = json.Number(strconv.FormatUint(mig.Status.Summary.NumberOfMigratedResources, 10))

	exportStats.Data.TotalBytesMigrated = json.Number(strconv.FormatUint(mig.Status.Summary.TotalBytesMigrated, 10))

	exportStats.Data.ElapsedTimeForVolumeMigration = mig.Status.Summary.ElapsedTimeForVolumeMigration

	exportStats.Data.ElapsedTimeForResourceMigration = mig.Status.Summary.ElapsedTimeForResourceMigration

	PrettyStruct(exportStats)

	return exportStats

}

func PrettyStruct(data interface{}) {
	val, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		logrus.Panicf("Failed to prettify data")
	}
	fmt.Printf("Exportable migration data: %v", string(val))
}

func getResourceNamesFromMigration(mig *storkv1.Migration) string {
	var resourceList []string
	for _, resource := range mig.Status.Resources {
		if resource.Kind == "Deployment" || resource.Kind == "StatefulSet" {
			resourceList = append(resourceList, resource.Name)
		}
	}
	if len(resourceList) > 1 {
		// return comma separated list of apps if there are multiple apps
		return strings.Join(resourceList, ",")
	} else if len(resourceList) == 1 {
		return resourceList[0]
	}

	logrus.Info("App name not found for pushing to DB.")
	return ""
}
