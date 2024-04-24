package aetosutils

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
	"testing"

	"github.com/libopenstorage/stork/pkg/buildinfo"
	rest "github.com/libopenstorage/stork/pkg/restutil"
	"github.com/sirupsen/logrus"
)

var (
	testCasesStack  = make([]int, 0)
	testCase        TestCase
	TestSetId       int
	TestTags        string
	EnableDashboard bool
	FailureCause    string

	dash *Dashboard
	lock = &sync.Mutex{}
)

const (
	//DashBoardBaseURL for posting logs
	AetosUrl         = "https://aetos.pwx.purestorage.com"
	DashBoardBaseURL = AetosUrl + "/dashboard"
	BuildProperties  = "build.properties"
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

// Dashboard aetos dashboard structure
type Dashboard struct {
	//IsEnabled enable/disable dashboard logging
	IsEnabled bool
	//TestSetID test set ID to post the test logs and results
	TestSetID int
	//TestSet object created during initialization
	TestSet    *TestSet
	testcaseID int
	TestLog    *logrus.Logger
	LogUrl     string
	LogDir     string
}

type testCaseUpdateResponse struct {
	Status         string `json:"status"`
	TestCaseStatus string `json:"testCaseStatus"`
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
	TestRepoID  string            `json:"testRepoID"`
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

// TestSetBegin start testset and push data to dashboard DB
func (d *Dashboard) TestSetBegin(testSet *TestSet) {
	if TestSetId != 0 {
		d.TestLog.Infof("Testset ID already passed")
		d.TestSetID = TestSetId
		return
	}
	if d.IsEnabled && d.TestSetID == 0 {
		d.TestLog.Infof("Generating new tetsset id")
		if buildinfo.GetBuildInfo().Job != "" &&
			(testSet.Description == "" || testSet.Description == "Stork-Test workflows") {
			j := strings.Split(buildinfo.GetBuildInfo().Job, "/")
			testSet.Description = j[len(j)-1]
		}
		if testSet.Description == "" {
			d.TestLog.Warn("Description should not be empty")
		}

		if testSet.Product == "" {
			testSet.Product = "Stork Test"
		}

		if testSet.HostOs == "" {
			testSet.HostOs = runtime.GOOS
		}

		if d.TestSetID == 0 {
			createTestSetURL := fmt.Sprintf("%s/testset", DashBoardBaseURL)
			testSet.Tags["startedFromStork"] = "true"
			resp, respStatusCode, err := rest.POST(createTestSetURL, testSet, nil, nil)
			if err != nil {
				d.TestLog.Errorf("Error in starting TestSet, Cause: %v", err)
			} else if respStatusCode != http.StatusOK {
				d.TestLog.Errorf("Failed to create TestSet, resp : %s", string(resp))
			} else {
				d.TestSetID, err = strconv.Atoi(string(resp))
				if err == nil {
					d.TestLog.Infof("TestSetId created : %d", d.TestSetID)
				} else {
					d.TestLog.Errorf("TestSetId creation failed. Cause : %v", err)
				}
				err := os.Setenv("DASH_UID", fmt.Sprintf("%d", d.TestSetID))
				d.VerifySafely(err, nil, "DASH_UID set in env variables. ")
				curDir, err := os.Getwd()
				d.VerifySafely(err, nil, "DASH_UID set in env variables. ")
				buildPropertiesFile := fmt.Sprintf("%s/%s", curDir, BuildProperties)
				logrus.Infof("Writing the build.properties to %s", buildPropertiesFile)
				f, err := os.OpenFile(buildPropertiesFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				d.VerifySafely(err, nil, "Created build.properties file?")
				_, err = f.WriteString(fmt.Sprintf("DASH_UID=%d\n", d.TestSetID))
				d.VerifySafely(err, nil, "test set id written to build.properties file?")
			}
		}
		d.TestLog.Infof("Dashboard URL : %s", fmt.Sprintf("https://aetos.pwx.purestorage.com/resultSet/testSetID/%d", d.TestSetID))
	} else {
		d.TestLog.Infof("Dashboard logging is disabled")
	}

}

// TestSetEnd  end testset and update  to dashboard DB
func (d *Dashboard) TestSetEnd() {

	if d.IsEnabled {
		if d.TestSetID == 0 {
			d.TestLog.Errorf("TestSetID is empty")
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
			d.TestLog.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Failed to end TestSet, Resp : %s", string(resp))
		} else {
			d.TestLog.Infof("TestSetId %d update successfully", d.TestSetID)
		}
		d.TestLog.Infof("Dashboard URL : %s", fmt.Sprintf("https://aetos.pwx.purestorage.com/resultSet/testSetID/%d", d.TestSetID))
	}
}

// TestCaseEnd update testcase  to dashboard DB
func (d *Dashboard) TestCaseEnd() {
	if d.IsEnabled {
		var url string
		if d.testcaseID == 0 {
			d.TestLog.Error("TestCaseID is empty")
			return
		}
		if FailureCause != "" {
			url = fmt.Sprintf("%s/testcase/%d/end?cause=%s", DashBoardBaseURL, d.testcaseID, FailureCause)
		} else {
			url = fmt.Sprintf("%s/testcase/%d/end", DashBoardBaseURL, d.testcaseID)
		}
		resp, respStatusCode, err := rest.PUT(url, nil, nil, nil)
		if err != nil {
			d.TestLog.Errorf("Error in updating TestCase, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Failed to end TestCase, Resp : %s", string(resp))
		} else {
			d.TestLog.Infof("TestCase %d ended successfully", d.testcaseID)
		}

		var updateResponse testCaseUpdateResponse
		err = json.Unmarshal(resp, &updateResponse)
		if err != nil {
			d.TestLog.Errorf("Error parsing update test output, %v", err)
		}
		result := updateResponse.TestCaseStatus
		d.VerifySafely(result, "PASS", "Workflow completed successfully ?")
		d.TestLog.Info("--------Test End------")
		d.TestLog.Infof("#Test: %s ", testCase.ShortName)
		d.TestLog.Infof("#Description: %s ", testCase.Description)
		d.TestLog.Infof("#Result: %s ", result)
		d.TestLog.Info("------------------------")
		removeTestCaseFromStack(d.testcaseID)
		if len(testCasesStack) > 0 {
			d.testcaseID = testCasesStack[len(testCasesStack)-1]
		}
	}
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
			d.TestLog.Error("TestSetID is empty")
		}

		updateTestSetURL := fmt.Sprintf("%s/testset/%d", DashBoardBaseURL, d.TestSetID)
		resp, respStatusCode, err := rest.PUT(updateTestSetURL, testSet, nil, nil)

		if err != nil {
			d.TestLog.Errorf("Error in updating TestSet, Caose: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Failed to update TestSet, Resp : %s", string(resp))
		} else {
			d.TestLog.Infof("TestSetId %d update successfully", d.TestSetID)

		}
	}
}

// TestCaseBegin start the test case and push data to dashboard DB
func (d *Dashboard) TestCaseBegin(testName, description, testRepoID string, tags map[string]string) {
	if d.IsEnabled {
		if d.TestSetID == 0 {
			d.TestLog.Errorf("In testset begin, TestSetID is empty, cannot update update testcase")
			return
		}

		testCase = TestCase{}
		_, file, _, ok := runtime.Caller(1)
		if ok {
			m := regexp.MustCompile(`stork`)
			r := m.FindStringIndex(file)
			if r != nil {
				fp := file[r[0]:]
				testCase.ModuleName = fp
				files := strings.Split(fp, "/")
				testCase.ShortName = files[len(files)-1]

				d.TestLog.Infof("Running test from file %s, module: %s", fp, testName)
			}
			testCase.Name = testName
		}
		testCase.Status = INPROGRESS
		testCase.Description = description
		testCase.HostOs = runtime.GOOS
		testCase.TestSetID = d.TestSetID
		testCase.TestRepoID = testRepoID
		testCase.Tags = make(map[string]string)
		testCase.TestType = "TEST"

		// Check for common env variables and add as tags
		if buildinfo.OnJenkins() {
			if buildinfo.GetBuildInfo().Job != "" {
				testCase.Tags["JOB_NAME"] = buildinfo.GetBuildInfo().Job
			}
			if buildinfo.GetBuildInfo().URL != "" {
				testCase.Tags["BUILD_URL"] = buildinfo.GetBuildInfo().URL
			}
		}
		if TestTags != "" {
			tags := strings.Split(TestTags, ",")
			for _, tag := range tags {
				var key, val string
				if !strings.Contains(tag, ":") {
					logrus.Infof("Invalid tag %s. Please provide tag in key:value format skipping provided tag", tag)
				} else {
					key = strings.SplitN(tag, ":", 2)[0]
					val = strings.SplitN(tag, ":", 2)[1]
					testCase.Tags[key] = val
				}
			}
		}

		for key, val := range tags {
			testCase.Tags[key] = val
		}

		createTestCaseURL := fmt.Sprintf("%s/testcase", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(createTestCaseURL, testCase, nil, nil)
		if err != nil {
			d.TestLog.Infof("Error in starting TesteCase, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Error creating test case, resp :%s", string(resp))
		} else {
			d.testcaseID, err = strconv.Atoi(string(resp))
			if err == nil {
				d.TestLog.Infof("TestCaseID created : %d", d.testcaseID)
			} else {
				d.TestLog.Errorf("TestCase creation failed. Cause : %v", err)
			}
		}
		d.TestLog.Info("--------Test Start------")
		d.TestLog.Infof("#Test: %s ", testCase.ShortName)
		d.TestLog.Infof("#Description: %s ", description)
		d.Infof("Log URL: %s", d.LogUrl)
		d.TestLog.Info("------------------------")
		d.Infof("Stork command to be executed is: %s", os.Args)
		testCasesStack = append(testCasesStack, d.testcaseID)

	} else {
		d.TestLog.Errorf("Not enabled")
	}
}

func (d *Dashboard) verify(r result) {
	if d.IsEnabled {
		if r.TestCaseID == 0 {
			d.TestLog.Errorf("TestcaseId should not be empty for updating result")
		}
		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)
		resp, respStatusCode, err := rest.POST(commentURL, r, nil, nil)
		if err != nil {
			d.TestLog.Errorf("Error in verifying, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Error updating the verify comment, resp : %s", string(resp))
		}
	}
}

// VerifySafely verify test without aborting the execution
func (d *Dashboard) VerifySafely(actual interface{}, expected interface{}, description string) {
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

	d.TestLog.Infof("VerfySafely: Desc: %s", description)
	if actualVal == expectedVal {
		res.ResultType = "info"
		res.ResultStatus = true
		d.TestLog.Infof("Actual:%v, Expected: %v", actual, expected)
	} else {
		res.ResultType = "error"
		res.ResultStatus = false
		if actual != nil && reflect.TypeOf(actual).String() == "*errors.errorString" {
			d.Errorf(fmt.Sprintf("%v", actual))
			d.TestLog.Errorf(fmt.Sprintf("%v", actual))
			res.Actual = "Error"
			res.Expected = "nil"
		} else {
			d.TestLog.Errorf("Actual:%v, Expected: %v", actual, expected)
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
	res.Description = description
	res.TestCaseID = d.testcaseID
	res.ResultStatus = false
	res.ResultType = "error"
	d.Errorf(fmt.Sprintf("%v: %v", description, args))
	if d.IsEnabled {
		d.verify(res)
	}
	d.TestSetEnd()
}

// VerifyFatal verify test and abort operation upon failure
func (d *Dashboard) VerifyFatal(t *testing.T, actual interface{}, expected interface{}, description string) {
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

	d.TestLog.Infof("VerfyFatal: Desc: %s", description)
	if actualVal == expectedVal {
		res.ResultType = "info"
		res.ResultStatus = true
		d.TestLog.Infof("Actual:%v, Expected: %v", actual, expected)
	} else {
		res.ResultType = "error"
		res.ResultStatus = false
		if actual != nil && reflect.TypeOf(actual).String() == "*errors.errorString" {
			d.Errorf(fmt.Sprintf("%v", actual))
			d.TestLog.Errorf(fmt.Sprintf("%v", actual))
			res.Actual = "Error"
			res.Expected = "nil"
		} else {
			d.TestLog.Errorf("Actual:%v, Expected: %v", actual, expected)
		}
	}
	if d.IsEnabled {
		d.verify(res)
	}
	if actualVal != expectedVal {
		d.TestSetEnd()
		d.TestLog.Error(fmt.Sprintf("Fatal error occurred in %v", description))
		t.FailNow()
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
			d.TestLog.Errorf("TestcaseId should not be empty for updating result")
		}

		commentURL := fmt.Sprintf("%s/result", DashBoardBaseURL)

		resp, respStatusCode, err := rest.POST(commentURL, c, nil, nil)
		if err != nil {
			d.TestLog.Errorf("Error in verifying, Cause: %v", err)
		} else if respStatusCode != http.StatusOK {
			d.TestLog.Errorf("Error updating the vrify comment, resp : %s", string(resp))
		}
	}
}

// Get returns the dashboard struct instance
func Get() *Dashboard {
	if dash == nil {
		lock.Lock()
		defer lock.Unlock()
		if dash == nil {
			dash = &Dashboard{}
		}
	}
	return dash
}
