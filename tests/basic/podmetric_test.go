package tests

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	rest "github.com/portworx/torpedo/pkg/restutil"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	logglyIterateUrl = "https://pxlite.loggly.com/apiv2/events/iterate"
)

var _ = Describe("{PodMetricFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context
	var namespacePrefix string

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)

		StartTorpedoTest("PodMetricFunctional", "Functional Tests for Pod Metrics", nil, testrailID)
	})

	Context("{PodMetricsSample}", func() {
		namespacePrefix = "podmetricsample"

		// shared test function for pod metric functional tests
		sharedTestFunction := func() {
			It("has to fetch the logs from loggly", func() {
				meteringInterval := 1 * time.Minute
				log.InfoD("Testing with metering interval %v", meteringInterval)

				log.InfoD("Getting cluster ID")
				clusterUUID, err := getClusterID()
				log.FailOnError(err, "Failed to get cluster id data")

				log.InfoD("Fetching logs from loggly")
				meteringData, err := getMeteringData(clusterUUID, meteringInterval)
				log.FailOnError(err, "Failed to get metering data")

				existsData := len(meteringData) > 0
				dash.VerifyFatal(existsData, true, "there should be metering data originally in loggly")
				initialPodHours := getLatestPodHours(meteringData)
				log.InfoD("Latest pod hours before starting app: %v", initialPodHours)

				log.InfoD("Deploy applications")
				contexts = make([]*scheduler.Context, 0)
				for i := 0; i < Inst().GlobalScaleFactor; i++ {
					contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
				}

				log.InfoD("Validate applications")
				ValidateApplications(contexts)

				waitDuration := meteringInterval + 30*time.Second
				log.InfoD("Wait %v for previous interval to go through", waitDuration)
				time.Sleep(waitDuration)

				log.InfoD("Wait %v for a new interval to go through", waitDuration)
				time.Sleep(waitDuration)

				log.InfoD("Check metering data is accurate")
				meteringData, err = getMeteringData(clusterUUID, meteringInterval)
				log.FailOnError(err, "Failed to get metering data")
				existsData = len(meteringData) > 0
				dash.VerifyFatal(existsData, true, "there should be metering data in loggly")
				for _, md := range meteringData {
					dash.VerifyFatal(md.ClusterUUID, clusterUUID, "this cluster should have data now")
				}

				log.InfoD("Check pod hours is correct")
				expectedAppPodHours, err := getExpectedPodHours(contexts, meteringInterval)
				log.FailOnError(err, "Failed to get expectedAppPodHours")
				log.InfoD("Estimated pod hours for this app is %v", expectedAppPodHours)

				expectedPodHours := float64(expectedAppPodHours) + initialPodHours
				log.InfoD("Estimated total pod hours is %v", expectedPodHours)

				actualPodHours := getLatestPodHours(meteringData)
				log.InfoD("Actual total pod hours is %v", actualPodHours)
				err = verifyPodHourWithError(actualPodHours, expectedPodHours, 0.01)
				log.FailOnError(err, "Failed to verify pod hours")
			})
		}

		// Sample pod metric tests
		Describe("{SamplePodMetricTest}", func() {
			JustBeforeEach(func() {
				// testrailID =
			})
			sharedTestFunction()
		})

	})

	AfterEach(func() {
		Step("destroy apps", func() {
			log.InfoD("destroying apps")
			if CurrentGinkgoTestDescription().Failed {
				log.InfoD("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})

	AfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
		defer EndTorpedoTest()
	})
})

// CallhomeData is the latest json format for parsing loggly callhome data
type CallhomeData struct {
	ClusterUUID             string  `json:"cluster_uuid"`
	UsageType               string  `json:"usage_type"`
	StorageNodeCount        int     `json:"storage_node_count"`
	StoragelessNodeCount    int     `json:"storageless_node_count"`
	BaremetalNodeCount      int     `json:"baremetal_node_count"`
	VirtualMachineNodeCount int     `json:"virtual_machine_node_count"`
	VolumeCount             int     `json:"volume_count"`
	PodHour                 float64 `json:"pod_hour"`
	Volumes                 []struct {
		ID        string `json:"id"`
		SizeBytes int    `json:"size_bytes"`
		UsedBytes int    `json:"used_bytes,omitempty"`
		Shared    string `json:"shared"`
	} `json:"volumes"`
	SentToPure1  bool `json:"SentToPure1"`
	SentToLoggly bool `json:"SentToLoggly"`
}

// LogglyPayload is the payload we receive from loggly calls
type LogglyPayload struct {
	Events []*LogglyEvent `json:"events"`
}

// LogglyEvent is an individual metering event
type LogglyEvent struct {
	ID        string   `json:"id"`
	Timestamp int64    `json:"timestamp"`
	Raw       string   `json:"raw"`
	Tags      []string `json:"tags"`
}

func getLogglyData(clusterUUID string, fromTime string) ([]byte, int, error) {
	query := fmt.Sprintf("q=%s&from=%s&until=now", clusterUUID, fromTime)

	logglyToken, ok := os.LookupEnv("LOGGLY_API_TOKEN")
	dash.VerifyFatal(ok, true, "failed to fetch loggly api token")
	headers := make(map[string]string)
	headers["Authorization"] = fmt.Sprintf("Bearer %v", logglyToken)
	return rest.Get(fmt.Sprintf("%v?%v", logglyIterateUrl, query), nil, headers)
}

func getClusterID() (string, error) {
	workerNode := node.GetWorkerNodes()[0]
	clusterID, err := Inst().N.RunCommand(workerNode, fmt.Sprintf("cat %s", "/etc/pwx/cluster_uuid"), node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get pxctl status, Err: %v", err)
	}

	return clusterID, nil
}

func getMeteringData(clusterUUID string, meteringInterval time.Duration) ([]*CallhomeData, error) {
	log.InfoD("Fetching logs from loggly")

	lookbackInterval := meteringInterval + 1*time.Minute
	data, code, err := getLogglyData(clusterUUID, fmt.Sprintf("-%vm", math.Round(lookbackInterval.Minutes())))
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("failed to get loggly data. status code: %v", code)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("loggy return empty response")
	}

	log.InfoD("Parsing logs from loggly")
	var logglyPayload LogglyPayload
	err = json.Unmarshal(data, &logglyPayload)
	if err != nil {
		return nil, err
	}

	var callhomeEvents []*CallhomeData
	for _, e := range logglyPayload.Events {
		chd := CallhomeData{}
		err = json.Unmarshal([]byte(e.Raw), &chd)
		if err != nil {
			return nil, err
		}
		callhomeEvents = append(callhomeEvents, &chd)
	}

	var meteringData []*CallhomeData
	for _, d := range callhomeEvents {
		if d.UsageType == "meteringData" {
			meteringData = append(meteringData, d)
		}
	}

	return meteringData, nil
}

// getExpectedPodHours returns the estimate pod hour given that the metering interval
func getExpectedPodHours(contexts []*scheduler.Context, meteringInterval time.Duration) (float64, error) {
	totalPods := make(map[string]bool)
	for _, ctx := range contexts {
		log.InfoD("Getting pod hour for context %v", ctx.App.Key)
		vols, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return 0, err
		}

		for _, vol := range vols {
			pods, err := Inst().S.GetPodsForPVC(vol.Name, vol.Namespace)
			if err != nil {
				return 0, err
			}
			for _, p := range pods {
				uidStr := string(p.GetUID())
				totalPods[uidStr] = true
			}
		}
	}

	// Count one minute per pod using a PX volume
	return float64(len(totalPods)*int(meteringInterval.Minutes())) / 60, nil
}

func getLatestPodHours(meteringData []*CallhomeData) float64 {
	return meteringData[0].PodHour
}

func verifyPodHourWithError(actualPodHours, expectedPodHours, reasonableErrorPercent float64) error {
	errorRate := math.Abs(expectedPodHours-actualPodHours) / actualPodHours
	log.InfoD("Acceptable error rate for this app: %v. actual error rate: %v", reasonableErrorPercent, errorRate)

	actualValueAcceptable := errorRate < reasonableErrorPercent
	if !actualValueAcceptable {
		return fmt.Errorf("error rate for pod hours should be within %v percentage. Actual: %v", reasonableErrorPercent, errorRate)
	}

	return nil
}