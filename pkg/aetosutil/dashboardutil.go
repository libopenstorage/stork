package aetosutil

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/gomega"
	rest "github.com/portworx/torpedo/pkg/restutil"
	"github.com/sirupsen/logrus"
)

var (
	testCasesStack    = make([]int, 0)
	testCaseStartTime time.Time
	testCase          TestCase

	dash         *Dashboard
	lock         = &sync.Mutex{}
	expect       = gomega.Expect
	haveOccurred = gomega.HaveOccurred
)

const (
	//DashBoardBaseURL for posting logs
	DashBoardBaseURL = "http://aetos.pwx.purestorage.com/dashboard" //"http://aetos-dm.pwx.purestorage.com:3939/dashboard"
	AetosBaseURL     = "http://aetos.pwx.purestorage.com"
)

const (
	//PASS status for testset/testcase
	PASS = "PASS"
	//FAIL status for testset/testcase
	FAIL = "FAIL"
	//ABORT status for testset/testcase
	ABORT = "ABORT"
	//TIMEOUT status for testset/testcase
	TIMEOUT = "TIMEOUT"
	//ERROR status for testset/testcase
	ERROR = "ERROR"
	// NOTSTARTED  status for testset/testcase
	NOTSTARTED = "NOT_STARTED"
	// INPROGRESS  status for testset/testcase
	INPROGRESS = "IN_PROGRESS"
)

type testCaseUpdateResponse struct {
	Status         string `json:"status"`
	TestCaseStatus string `json:"testCaseStatus"`
}

var workflowStatuses = []string{PASS, FAIL, ABORT, ERROR, TIMEOUT, NOTSTARTED, INPROGRESS}

// Dashboard aetos dashboard structure
type Dashboard struct {
	//IsEnabled enable/disable dashboard logging
	IsEnabled bool
	//TestSetID test set ID to post the test logs and results
	TestSetID int
	//TestSet object created during initialization
	TestSet           *TestSet
	testcaseID        int
	testSetStartTime  time.Time
	testCaseStartTime time.Time
}

// TestSet struct
type TestSet struct {
	CommitID    string            `json:"commitId"`
	User        string            `json:"user"`
	Product     string            `json:"product"`
	Description string            `json:"description"`
	HostOs      string            `json:"hostOs"`
	Branch      string            `json:"branch"`
	TestType    string            `json:"testType"`
	Tags        map[string]string `json:"nTags"`
	Status      string            `json:"status"`
}

// TestCase struct
type TestCase struct {
	Name       string `json:"name"`
	ShortName  string `json:"shortName"`
	ModuleName string `json:"moduleName"`

	Status      string            `json:"status"`
	Errors      []string          `json:"errors"`
	LogFile     string            `json:"logFile"`
	Description string            `json:"description"`
	Command     string            `json:"command"`
	HostOs      string            `json:"hostOs"`
	Tags        map[string]string `json:"nTags"`
	TestSetID   int               `json:"testSetID"`
	TestRailID  string            `json:"testRepoID"`
	Duration    string            `json:"duration"`
	TestType    string            `json:"testType"`
}

type result struct {
	TestCaseID   int    `json:"testCaseID"`
	Description  string `json:"description"`
	Actual       string `json:"actual"`
	Expected     string `json:"expected"`
	ResultType   string `json:"type"`
	ResultStatus bool   `json:"result"`
}

type comment struct {
	TestCaseID  int    `json:"testCaseID"`
	Description string `json:"description"`
	ResultType  string `json:"type"`
}

type stats struct {
	Name      string            `json:"name"`
	Product   string            `json:"product"`
	StatsType string            `json:"statsType"`
	Version   string            `json:"version"`
	Data      map[string]string `json:"data"`
}

// TestSetBegin start testset and push data to dashboard DB
func (d *Dashboard) TestSetBegin(testSet *TestSet) {
	dashURL := "Dash is disabled"
	if d.IsEnabled && d.TestSetID == 0 {

		if testSet.Description == "" {
			testSet.Description = "Torpedo Workflows"
		}

		if testSet.User == "" {
			testSet.User = "nouser"
		}

		if testSet.TestType == "" {
			testSet.TestType = "SystemTest"
		}

		if testSet.Product == "" {
			testSet.Product = "Portworx Enterprise"
		}

		if testSet.HostOs == "" {
			testSet.HostOs = runtime.GOOS
		}

		createTestSetURL := fmt.Sprintf("%s/testset", DashBoardBaseURL)
		resp, respStatusCode, err := rest.POST(createTestSetURL, testSet, nil, nil)
		if err != nil {
			logrus.Errorf("error in starting TestSet, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("failed to create TestSet, resp : %s", string(resp))
		} else {
			d.TestSetID, err = strconv.Atoi(string(resp))
			if err != nil {
				logrus.Errorf("TestSetId creation failed. Cause : %v", err)
			}
			dashURL = fmt.Sprintf("Dashboard URL : %s/resultSet/testSetID/%d", AetosBaseURL, d.TestSetID)
		}
	}
	logrus.Info(dashURL)
}

// TestSetEnd  end testset and update  to dashboard DB
func (d *Dashboard) TestSetEnd() {

	if d.IsEnabled {
		if d.TestSetID == 0 {
			return
		}
		if len(testCasesStack) > 0 {
			for _, v := range testCasesStack {
				d.testcaseID = v
				d.TestCaseEnd()
			}
			testCasesStack = nil
		}

		updateTestSetURL := fmt.Sprintf("%s/testset/%d/end", DashBoardBaseURL, d.TestSetID)
		resp, respStatusCode, err := rest.PUT(updateTestSetURL, nil, nil, nil)

		if err != nil {
			logrus.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Failed to end TestSet, Resp : %s", string(resp))
		}
		logrus.Infof("Dashboard URL : %s", fmt.Sprintf("%s/resultSet/testSetID/%d", AetosBaseURL, d.TestSetID))
	}
}

// TestCaseEnd update testcase  to dashboard DB
func (d *Dashboard) TestCaseEnd() {
	if d.IsEnabled {

		if d.testcaseID == 0 {
			return
		}

		url := fmt.Sprintf("%s/testcase/%d/end", DashBoardBaseURL, d.testcaseID)
		resp, respStatusCode, err := rest.PUT(url, nil, nil, nil)

		if err != nil {
			logrus.Errorf("Error in updating TestCase, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Failed to end TestCase, Resp : %s", string(resp))
		}

		removeTestCaseFromStack(d.testcaseID)

		var updateResponse testCaseUpdateResponse
		err = json.Unmarshal(resp, &updateResponse)
		if err != nil {
			logrus.Errorf("Error parsing update test output, %v", err)
		}
		testCaseResult := updateResponse.TestCaseStatus
		d.VerifySafely(testCaseResult, "PASS", "Test completed successfully ?")
	}

	logrus.Info("--------Test End------")
	logrus.Infof("#Test: %s ", testCase.Name)
	logrus.Infof("#Description: %s ", testCase.Description)
	logrus.Info("------------------------")
}

func removeTestCaseFromStack(testcaseID int) {

	removeIndex := -1
	for i, v := range testCasesStack {
		if v == testcaseID {
			removeIndex = i
			break
		}
	}

	if removeIndex != -1 {
		testCasesStack = append(testCasesStack[:removeIndex], testCasesStack[removeIndex+1:]...)
	}

}

// TestSetUpdate update test set  to dashboard DB
func (d *Dashboard) TestSetUpdate(testSet *TestSet) {

	if d.IsEnabled {

		if d.TestSetID == 0 {
			return
		}

		updateTestSetURL := fmt.Sprintf("%s/testset/%d", DashBoardBaseURL, d.TestSetID)
		resp, respStatusCode, err := rest.PUT(updateTestSetURL, testSet, nil, nil)

		if err != nil {
			logrus.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Failed to update TestSet, Resp : %s", string(resp))
		}
	}
}

// TestCaseBegin start the test case and push data to dashboard DB
func (d *Dashboard) TestCaseBegin(testName, description, testRailID string, tags map[string]string) {

	logrus.Info("--------Test Start------")
	logrus.Infof("#Test: %s ", testName)
	logrus.Infof("#Description: %s ", description)
	logrus.Info("------------------------")
	if d.IsEnabled {
		if d.TestSetID == 0 {
			return
		}

		testCase = TestCase{}
		testCase.Tags = make(map[string]string)
		testCase.Name = testName

		_, file, _, ok := runtime.Caller(1)
		if ok {

			m := regexp.MustCompile(`torpedo`)

			r := m.FindStringIndex(file)
			if r != nil {
				fp := file[r[0]:]
				testCase.ModuleName = fp
				files := strings.Split(fp, "/")
				testCase.ShortName = files[len(files)-1]

				logrus.Infof("Running test from file %s, module: %s", fp, testName)

			}

		}
		//t.StartTime = time.Now().Format(time.RFC3339)
		testCase.Status = INPROGRESS
		testCase.Description = description
		testCase.HostOs = runtime.GOOS
		testCase.TestType = "TEST"

		testCase.TestSetID = d.TestSetID
		testCase.TestRailID = testRailID

		// Check for common env variables and add as tags
		if os.Getenv("JOB_NAME") != "" {
			testCase.Tags["JOB_NAME"] = os.Getenv("JOB_NAME")
		}
		if os.Getenv("BUILD_URL") != "" {
			testCase.Tags["BUILD_URL"] = os.Getenv("BUILD_URL")
		}

		if tags != nil {
			for key, val := range tags {
				testCase.Tags[key] = val
			}
		}
		testCaseStartTime = time.Now()

		createTestCaseURL := fmt.Sprintf("%s/testcase", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(createTestCaseURL, testCase, nil, nil)
		if err != nil {
			logrus.Errorf("Error in starting TesteCase, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Error creating test case, resp :%s", string(resp))
		} else {
			d.testcaseID, err = strconv.Atoi(string(resp))
			if err != nil {
				logrus.Errorf("TestCase creation failed. Cause : %v", err)
			}
		}
		d.Infof("Torpedo Command: %s", os.Args)
		testCasesStack = append(testCasesStack, d.testcaseID)

	}
}

func (d *Dashboard) verify(r result) {
	if d.IsEnabled {

		if r.TestCaseID == 0 {
			return
		}

		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(commentURL, r, nil, nil)
		if err != nil {
			logrus.Errorf("Error in updating verification to dashboard, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Error updating the verify comment, resp : %s", string(resp))
		}
	}
}

// VerifySafely verify test without aborting the execution
func (d *Dashboard) VerifySafely(actual, expected interface{}, description string) {
	if actual == nil && expected == nil {
		actual = true
		expected = true
	}

	actualVal := fmt.Sprintf("%v", actual)
	expectedVal := fmt.Sprintf("%v", expected)
	res := result{}

	res.Actual = actualVal
	res.Expected = expectedVal
	res.Description = description
	res.TestCaseID = d.testcaseID

	logrus.Infof("Verifying : Description : %s", description)
	if actualVal == expectedVal {
		res.ResultType = "info"
		res.ResultStatus = true
		logrus.Infof("Actual:%v, Expected: %v", actual, expected)
	} else {
		res.ResultType = "error"
		res.ResultStatus = false
		if actual != nil && reflect.TypeOf(actual).String() == "*errors.errorString" {
			d.Errorf(fmt.Sprintf("%v", actual))
			logrus.Errorf(fmt.Sprintf("%v", actual))
			res.Actual = "Error"
			res.Expected = "nil"
		} else {
			logrus.Errorf("Actual:%v, Expected: %v", actual, expected)
		}
	}
	if d.IsEnabled {
		d.verify(res)
	}
}

func (d *Dashboard) Fatal(description string, args ...interface{}) {
	res := result{}
	res.Actual = "false"
	res.Expected = "true"
	res.Description = fmt.Sprintf(description, args...)
	res.TestCaseID = d.testcaseID
	res.ResultStatus = false
	res.ResultType = "error"
	if d.IsEnabled {
		d.verify(res)
	}
}

// VerifyFatal verify test and abort operation upon failure
func (d *Dashboard) VerifyFatal(actual, expected interface{}, description string) {

	d.VerifySafely(actual, expected, description)
	var err error
	if actual != expected {
		err = fmt.Errorf(description)
	}
	expect(err).NotTo(haveOccurred())
}

// Info logging info message
func (d *Dashboard) Info(message string) {
	if d.IsEnabled {
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = message
		res.ResultType = "info"
		d.addComment(res)
	}
}

// Infof logging info with formated message
func (d *Dashboard) Infof(message string, args ...interface{}) {
	if d.IsEnabled {
		fmtMsg := fmt.Sprintf(message, args...)
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = fmtMsg
		res.ResultType = "info"
		d.addComment(res)
	}
}

// Warnf logging formatted warn message
func (d *Dashboard) Warnf(message string, args ...interface{}) {
	if d.IsEnabled {
		fmtMsg := fmt.Sprintf(message, args...)
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = fmtMsg
		res.ResultType = "warning"
		d.addComment(res)
	}
}

// Warn logging warn message
func (d *Dashboard) Warn(message string) {
	if d.IsEnabled {
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = message
		res.ResultType = "warning"
		d.addComment(res)
	}
}

// Error logging error message
func (d *Dashboard) Error(message string) {
	if d.IsEnabled {
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = message
		res.ResultType = "error"
		d.addComment(res)
	}
}

// Errorf logging formatted error message
func (d *Dashboard) Errorf(message string, args ...interface{}) {
	if d.IsEnabled {
		fmtMsg := fmt.Sprintf(message, args...)
		res := comment{}
		res.TestCaseID = d.testcaseID
		res.Description = fmtMsg
		res.ResultType = "error"
		d.addComment(res)
	}
}

func (d *Dashboard) addComment(c comment) {
	if d.IsEnabled {

		if c.TestCaseID == 0 {
			return
		}

		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(commentURL, c, nil, nil)
		if err != nil {
			logrus.Errorf("Error in adding log message to dashboard, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Error updating the vrify comment, resp : %s", string(resp))
		}
	}
}

// Get returns the dashboard struct instance
func Get() *Dashboard {
	if dash == nil {
		lock.Lock()
		defer lock.Unlock()
		if dash == nil {
			fmt.Println("Creating new Dashboard instance.")
			dash = &Dashboard{}
		}
	}
	return dash
}

func (d *Dashboard) UpdateStats(name, product, statType, version string, dashStats map[string]string) {
	if d.IsEnabled {

		dashStats["dash-url"] = fmt.Sprintf("%s/resultSet/testSetID/%d", AetosBaseURL, d.TestSetID)
		st := stats{
			Name:      name,
			Product:   product,
			StatsType: statType,
			Version:   version,
			Data:      dashStats,
		}

		statsURL := fmt.Sprintf("%s/stats", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(statsURL, st, nil, nil)
		if err != nil {
			logrus.Errorf("Error in updating stats to dashboard, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			logrus.Errorf("Error updating the stats, resp : %s", string(resp))
		}
	}
}
