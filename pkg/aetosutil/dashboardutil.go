package aetosutil

import (
	"fmt"
	"github.com/onsi/gomega"
	rest "github.com/portworx/torpedo/pkg/restutil"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	testCasesStack    = make([]int, 0)
	verifications     = make([]result, 0)
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

var workflowStatuses = []string{PASS, FAIL, ABORT, ERROR, TIMEOUT, NOTSTARTED, INPROGRESS}

//Dashboard aetos dashboard structure
type Dashboard struct {
	//IsEnabled enable/disable dashboard logging
	IsEnabled bool
	//TestSetID test set ID to post the test logs and results
	TestSetID int
	//TestSet object created during initialization
	TestSet           *TestSet
	testcaseID        int
	verifications     []result
	testSetStartTime  time.Time
	testCaseStartTime time.Time
	Log               *logrus.Logger
}

//TestSet struct
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

//TestCase struct
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

// TestSetBegin start testset and push data to dashboard DB
func (d *Dashboard) TestSetBegin(testSet *TestSet) {
	if d.IsEnabled && d.TestSetID == 0 {

		if testSet.Branch == "" {
			d.Log.Warn("Branch should not be empty")
		}

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
			d.Log.Errorf("Error in starting TestSet, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Failed to create TestSet, resp : %s", string(resp))
		} else {
			d.TestSetID, err = strconv.Atoi(string(resp))
			if err == nil {
				d.Log.Infof("TestSetId created : %d", d.TestSetID)
			} else {
				d.Log.Errorf("TestSetId creation failed. Cause : %v", err)
			}
			d.Log.Infof("Dashboard URL : %s", fmt.Sprintf("http://aetos.pwx.purestorage.com/resultSet/testSetID/%d", d.TestSetID))
			os.Setenv("TESTSET-ID", fmt.Sprint(d.TestSetID))

		}
	}

}

// TestSetEnd  end testset and update  to dashboard DB
func (d *Dashboard) TestSetEnd() {

	if d.IsEnabled {
		if d.TestSetID == 0 {
			d.Log.Errorf("TestSetID is empty")
			return
		}

		updateTestSetURL := fmt.Sprintf("%s/testset/%d/end", DashBoardBaseURL, d.TestSetID)
		resp, respStatusCode, err := rest.PUT(updateTestSetURL, nil, nil, nil)

		if err != nil {
			d.Log.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Failed to end TestSet, Resp : %s", string(resp))
		} else {
			d.Log.Infof("TestSetId %d update successfully", d.TestSetID)
		}

		if len(testCasesStack) > 0 {
			for _, v := range testCasesStack {
				d.testcaseID = v
				d.TestCaseEnd()
			}
			testCasesStack = nil

		}
	}
}

// TestCaseEnd update testcase  to dashboard DB
func (d *Dashboard) TestCaseEnd() {
	result := "PASS"

	for _, v := range verifications {

		if !v.ResultStatus {
			result = "FAIL"
			break
		}
	}
	if d.IsEnabled {

		if d.testcaseID == 0 {
			d.Log.Error("TestCaseID is empty")
			return
		}

		url := fmt.Sprintf("%s/testcase/%d/end", DashBoardBaseURL, d.testcaseID)
		resp, respStatusCode, err := rest.PUT(url, nil, nil, nil)

		if err != nil {
			d.Log.Errorf("Error in updating TestCase, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Failed to end TestCase, Resp : %s", string(resp))
		} else {
			d.Log.Infof("TestCase %d ended successfully", d.testcaseID)
		}

		verifications = nil
		removeTestCaseFromStack(d.testcaseID)

	}

	d.Log.Info("--------Test End------")
	d.Log.Infof("#Test: %s ", testCase.ShortName)
	d.Log.Infof("#Description: %s ", testCase.Description)
	d.Log.Infof("#Result: %s ", result)
	d.Log.Info("------------------------")
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
			d.Log.Error("TestSetID is empty")
			return
		}

		updateTestSetURL := fmt.Sprintf("%s/testset/%d", DashBoardBaseURL, d.TestSetID)
		resp, respStatusCode, err := rest.PUT(updateTestSetURL, testSet, nil, nil)

		if err != nil {
			d.Log.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Failed to update TestSet, Resp : %s", string(resp))
		} else {
			d.Log.Infof("TestSetId %d update successfully", d.TestSetID)

		}
	}
}

// TestCaseBegin start the test case and push data to dashboard DB
func (d *Dashboard) TestCaseBegin(testName, description, testRailID string, tags map[string]string) {

	d.Log.Info("--------Test Start------")
	d.Log.Infof("#Test: %s ", testName)
	d.Log.Infof("#Description: %s ", description)
	d.Log.Info("------------------------")
	if d.IsEnabled {
		if d.TestSetID == 0 {
			d.Log.Errorf("TestSetID is empty, skipping begin testcase")
			return
		}

		testCase = TestCase{}
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

				d.Log.Infof("Running test from file %s, module: %s", fp, testName)

			}

		}
		//t.StartTime = time.Now().Format(time.RFC3339)
		testCase.Status = INPROGRESS
		testCase.Description = description
		testCase.HostOs = runtime.GOOS

		testCase.TestSetID = d.TestSetID

		testCase.TestRailID = testRailID
		if tags != nil {
			testCase.Tags = tags
		}
		testCaseStartTime = time.Now()

		createTestCaseURL := fmt.Sprintf("%s/testcase", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(createTestCaseURL, testCase, nil, nil)
		if err != nil {
			d.Log.Infof("Error in starting TesteCase, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Error creating test case, resp :%s", string(resp))
		} else {
			d.testcaseID, err = strconv.Atoi(string(resp))
			if err == nil {
				d.Log.Infof("TestCaseID created : %d", d.testcaseID)
			} else {
				d.Log.Errorf("TestCase creation failed. Cause : %v", err)
			}
		}

		testCasesStack = append(testCasesStack, d.testcaseID)

	}
}

func (d *Dashboard) verify(r result) {
	if d.IsEnabled {

		if r.TestCaseID == 0 {
			d.Log.Errorf("TestcaseId should not be empty for updating result")
			return
		}

		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(commentURL, r, nil, nil)
		if err != nil {
			d.Log.Errorf("Error in updating verification to dashboard, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Error updating the verify comment, resp : %s", string(resp))
		} else {
			d.Log.Tracef("verify response : %s", string(resp))

		}
	}
}

//VerifySafely verify test without aborting the execution
func (d *Dashboard) VerifySafely(actual, expected interface{}, description string) {

	actualVal := fmt.Sprintf("%v", actual)
	expectedVal := fmt.Sprintf("%v", expected)
	res := result{}

	res.Actual = actualVal
	res.Expected = expectedVal
	res.Description = description
	res.TestCaseID = d.testcaseID

	d.Log.Infof("Verifying : Description : %s", description)
	d.Log.Infof("Actual: %v, Expected : %v", actualVal, expectedVal)

	if actualVal == expectedVal {
		res.ResultType = "info"
		res.ResultStatus = true
		d.Log.Infof("Actual:%v, Expected: %v, Description: %v", actual, expected, description)
	} else {
		res.ResultType = "error"
		res.ResultStatus = false
		d.Log.Errorf("Actual:%v, Expected: %v, Description: %v", actual, expected, description)
	}
	verifications = append(verifications, res)

	if d.IsEnabled {
		d.verify(res)
	}

}

//VerifyFatal verify test and abort operation upon failure
func (d *Dashboard) VerifyFatal(actual, expected interface{}, description string) {

	d.VerifySafely(actual, expected, description)
	var err error
	if actual != expected {
		err = fmt.Errorf("Actual:%v, Expected: %v, Description: %v", actual, expected, description)
	}
	expect(err).NotTo(haveOccurred())
}

// Info logging info message
func (d *Dashboard) Info(message string) {
	d.Log.Infof(message)
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
	d.Log.Infof(message, args...)
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
	d.Log.Warnf(message, args...)
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
	d.Log.Warn(message)
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
	d.Log.Error(message)
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
	d.Log.Errorf(message, args...)
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
			d.Log.Errorf("TestcaseId should not be empty for updating result")
			return
		}

		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(commentURL, c, nil, nil)
		if err != nil {
			d.Log.Errorf("Error in adding log message to dashboard, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.Log.Errorf("Error updating the vrify comment, resp : %s", string(resp))
		} else {
			d.Log.Tracef("verify response : %s", string(resp))
		}
	}
}

//Get returns the dashboard struct instance
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
