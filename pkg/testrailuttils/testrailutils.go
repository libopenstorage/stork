package testrailuttils

import (
	"fmt"
	"github.com/educlos/testrail"
	"github.com/sirupsen/logrus"
)

var (
	milestoneID                  int
	client                       *testrail.Client
	runID                        int
	testRailConnectionSuccessful bool
	// MilestoneName for testrail
	MilestoneName                string
	// RunName for testrail, should be jenkins job name
	RunName                      string
	// JobRunID for testrail
	JobRunID                     string
	// JenkinsBuildURL to be passed to store in testrail
	JenkinsBuildURL              string
)

const (
	pwxProjectID           = 1
	testRailPassStatusCode = 1
	testRailFailStatusCode = 5
)

// Testrail object
type Testrail struct {
	Status        string
	TestID        int
	RunID         int
	DriverVersion string
}

// CreateMilestone Creates a milestone if it does not exists, else creates it
func CreateMilestone() {
	logrus.Infof("Create Testrail milestone")
	milestoneID = getMilestoneByProjectID(pwxProjectID, MilestoneName)
	if milestoneID == 0 {
		logrus.Debugf("Creating milesone, since it does not exists")
		sendatbleMs := testrail.SendableMilestone{
			Name:        MilestoneName,
			Description: "Created from Automation",
		}
		msCreated, err := client.AddMilestone(pwxProjectID, sendatbleMs)
		if err != nil {
			logrus.Errorf("error in creating milestone: %s", MilestoneName)
		}
		milestoneID = msCreated.ID
		logrus.Debugf("Milestone %s created successfully", MilestoneName)
	} else {
		logrus.Debugf("Milestone %s already exists\n", MilestoneName)
	}
}

func getMilestoneByProjectID(projectID int, milestonename string) int {
	logrus.Infof("Getting the milestone ID for projectID: %d", projectID)
	milestones, err := client.GetMilestones(projectID)
	if err != nil {
		logrus.Warningf("Error in getting milestone: %s", err)
		return 0
	}
	for _, ms := range milestones {
		if ms.Name == milestonename {
			return ms.ID
		}
	}
	return 0
}

// AddRunsToMilestone Adds a run to the milestone, if it does not exists
func AddRunsToMilestone(testrailID int) int {
	if !testRailConnectionSuccessful {
		return 0
	}
	logrus.Infof("Importing run to milestone %s, testrailID %d \n", MilestoneName, testrailID)
	filter := testrail.RequestFilterForRun{
		MilestoneID: []int{milestoneID},
	}
	runID = getRunID(pwxProjectID, filter)
	if runID == 0 {
		logrus.Debugf("Creating run %s for milestone", RunName)
		includeAll := false
		sendableRun := testrail.SendableRun{
			SuiteID:     pwxProjectID,
			Name:        RunName,
			Description: "Test run created from Jenkins trigger",
			MilestoneID: milestoneID,
			CaseIDs:     []int{testrailID},
			IncludeAll:  &includeAll,
		}
		createdRun, err := client.AddRun(pwxProjectID, sendableRun)
		if err != nil {
			logrus.Errorf("Unable to add the run %s", err)
		}
		runID = createdRun.ID
	} else {
		logrus.Debugf("Run already exists for Job: %s - %d , checking for tests\n", RunName, runID)
		testID := getTestIDForRunID(runID, testrailID)
		logrus.Debugf("Test id received is %d", testID)
		if testID == 0 {
			logrus.Debugf("Adding new test to the test run %s\n", RunName)
			var existingCases []int
			allTests, err := client.GetTests(runID)
			if err != nil {
				logrus.Errorf("Error in getting tests for runid: %d", runID)
			}
			for _, test := range allTests {
				existingCases = append(existingCases, test.CaseID)
			}
			existingCases = append(existingCases, testrailID)
			updateableRun := testrail.UpdatableRun{
				CaseIDs: existingCases,
			}
			_, err = client.UpdateRun(runID, updateableRun)
			if err != nil {
				logrus.Errorf("Error updating the run with new case %s \n", err)
			}
		} else {
			logrus.Debugf("Test Already exists, reusing the test")
		}
	}
	return runID
}

// AddTestEntry adds a test entry with values passed in testrail object.
func AddTestEntry(testrailObject Testrail) {
	if !testRailConnectionSuccessful {
		return
	}
	logrus.Infof("Adding test entry testID %d, run Id is %d, status %s", testrailObject.TestID, testrailObject.RunID, testrailObject.Status)
	tests, err := client.GetTests(runID)
	if err != nil {
		logrus.Errorf("Error getting tests for %d", runID)
	}
	for _, test := range tests {
		if test.CaseID == testrailObject.TestID {
			logrus.Debugf("Found test to update %d", testrailObject.TestID)
			var statusID int
			if testrailObject.Status == "Pass" {
				statusID = testRailPassStatusCode
			} else {
				statusID = testRailFailStatusCode
			}
			sendableResult := testrail.SendableResult{
				StatusID: statusID,
				Comment:  fmt.Sprintf("This is updated from Jenkins run of job %s, \n Job Run ID: %s, \n BuildUrl: %s, \n pxctl version: %s", RunName, JobRunID, JenkinsBuildURL, testrailObject.DriverVersion),
				Defects:  "",
			}
			_, err = client.AddResult(test.ID, sendableResult)
			if err != nil {
				logrus.Errorf("Error in adding result to %d - %s", testrailObject.TestID, err)
			} else {
				logrus.Debugf("testrail update succcesful")
			}
		}
	}
}

func getRunID(projectID int, filter testrail.RequestFilterForRun) int {
	logrus.Infof("Getting the run details for project Id %d", projectID)
	allRuns, err := client.GetRuns(projectID, filter)
	if err != nil {
		logrus.Warningf("Error in getting all jobs from milestone %s ", err)
	}
	for _, run := range allRuns {
		if run.Name == RunName {
			logrus.Debugf("Run %s already exsits in milestone not adding it\n", RunName)
			return run.ID
		}
	}
	return 0
}

func getTestIDForRunID(runID int, testrailID int) int {
	logrus.Infof("Getting the Tests Id for run: %d, testrailID %d", runID, testrailID)
	tests, err := client.GetTests(runID)
	if err != nil {
		logrus.Warningf("Error getting tests for existing run %d", runID)
	}
	for _, test := range tests {
		if test.CaseID == testrailID {
			logrus.Debugf("Test already exists in the run, not adding new test")
			return test.CaseID
		}
	}
	return 0
}

// Init function for the testrail, should be called everytime to setup values
func Init(hostname string, username string, password string) error {
	client = testrail.NewClient(hostname, username, password, true)
	_, err := client.GetProjects()
	if err != nil {
		testRailConnectionSuccessful = false
		logrus.Errorf("Testrail connection not successful")
		return err
	}
	testRailConnectionSuccessful = true
	return nil
}
