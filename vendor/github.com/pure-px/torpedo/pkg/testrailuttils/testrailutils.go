package testrailuttils

import (
	"fmt"
	"github.com/educlos/testrail"
	"github.com/pure-px/torpedo/pkg/log"
	"strings"
)

var (
	client                       *testrail.Client
	testRailConnectionSuccessful bool
	// MilestoneName for testrail
	MilestoneName string
	// MilestoneName to ID mapping
	MilestoneNameToID map[string]int
	// RunName for testrail, should be jenkins job name
	RunName string
	// JobRunID for testrail
	JobRunID string
	// JenkinsBuildURL to be passed to store in testrail
	JenkinsBuildURL string
)

const (
	PwxProjectID           = 1
	testRailPassStatusCode = 1
	testRailFailStatusCode = 5
)

// Testrail object
type Testrail struct {
	Status          string
	TestID          int
	RunID           int
	DriverVersion   string
	PxBackupVersion string
}

// CreateMilestone Creates a milestone if it does not exists, else creates it
func CreateMilestone() {
	milestones := strings.Split(MilestoneName, ",")
	MilestoneNameToID = make(map[string]int)
	for _, milestone := range milestones {
		log.Infof("Create Testrail milestone - [%s]", milestone)
		var err error
		milestoneID, err := getMilestoneByProjectID(PwxProjectID, milestone)
		if err != nil {
			log.Errorf("error in getting milestone: %s", milestone)
			return
		}
		if milestoneID == 0 {
			log.Debugf("Creating milesone, since it does not exists")
			sendatbleMs := testrail.SendableMilestone{
				Name:        milestone,
				Description: "Created from Automation",
			}
			msCreated, err := client.AddMilestone(PwxProjectID, sendatbleMs)
			if err != nil {
				log.Errorf("error in creating milestone: %s", milestone)
			}
			MilestoneNameToID[milestone] = msCreated.ID
			log.Debugf("Milestone %s created successfully ID - %d", milestone, msCreated.ID)
		} else {
			MilestoneNameToID[milestone] = milestoneID
			log.Debugf("Milestone %s already exists ID - %d \n", milestone, milestoneID)
		}
	}
}

func getMilestoneByProjectID(projectID int, milestonename string) (int, error) {
	log.Infof("Getting the milestone ID for projectID: %d", projectID)
	milestones, err := client.GetMilestones(projectID)
	if err != nil {
		log.Warnf("Error in getting milestone: %s", err)
		testRailConnectionSuccessful = false
		return 0, fmt.Errorf("Error in getting milestone %s", err)
	}
	for _, ms := range milestones {
		if ms.Name == milestonename {
			return ms.ID, nil
		}
	}
	return 0, nil
}

// AddRunsToMilestone Adds a run to the milestone, if it does not exists
func AddRunsToMilestone(testrailID int) int {
	if !testRailConnectionSuccessful {
		return 0
	}
	log.Infof("Importing run to milestone %s, testrailID %d \n", MilestoneName, testrailID)
	filter := testrail.RequestFilterForRun{
		MilestoneID: []int{MilestoneNameToID[MilestoneName]},
	}
	runID := getRunID(PwxProjectID, RunName, filter)
	if runID == 0 {
		log.Debugf("Creating run %s for milestone", RunName)
		includeAll := false
		sendableRun := testrail.SendableRun{
			SuiteID:     PwxProjectID,
			Name:        RunName,
			Description: "Test run created from Jenkins trigger",
			MilestoneID: MilestoneNameToID[MilestoneName],
			CaseIDs:     []int{testrailID},
			IncludeAll:  &includeAll,
		}
		createdRun, err := client.AddRun(PwxProjectID, sendableRun)
		if err != nil {
			log.Errorf("Unable to add the run %s", err)
		}
		runID = createdRun.ID
	} else {
		log.Debugf("Run already exists for Job: %s - %d , checking for tests\n", RunName, runID)
		testID := getTestIDForRunID(runID, testrailID)
		log.Debugf("Test id received is %d", testID)
		if testID == 0 {
			log.Debugf("Adding new test to the test run %s\n", RunName)
			var existingCases []int
			allTests, err := client.GetTests(runID)
			if err != nil {
				log.Errorf("Error in getting tests for runid: %d", runID)
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
				log.Errorf("Error updating the run with new case %s \n", err)
			}
		} else {
			log.Debugf("Test Already exists, reusing the test")
		}
	}
	return runID
}

// AddRunsToMilestones Adds a run to the milestones provided as comma separated values
func AddRunsToMilestones(testrailID int) []int {
	runIDs := make([]int, 0)
	if !testRailConnectionSuccessful {
		return nil
	}
	milestones := strings.Split(MilestoneName, ",")
	testruns := strings.Split(RunName, ",")

	// Handle the scenario where milestones are more than testruns
	if len(milestones) > len(testruns) {
		// Get the first value of testruns
		firstRun := testruns[0]
		// Append the first run to testruns until lengths match
		for len(testruns) < len(milestones) {
			testruns = append(testruns, firstRun)
		}
	}

	// Handle the scenario where testruns are more than milestones
	if len(testruns) > len(milestones) {
		// Get the first value of milestones
		firstMilestone := milestones[0]
		// Append the first milestone to milestones until lengths match
		for len(milestones) < len(testruns) {
			milestones = append(milestones, firstMilestone)
		}
	}

	for i, milestone := range milestones {
		milestoneID := MilestoneNameToID[milestone]
		log.Infof("Importing run to milestone %s with id %d, testrailID %d \n", milestone, milestoneID, testrailID)
		filter := testrail.RequestFilterForRun{
			MilestoneID: []int{milestoneID},
		}
		runID := getRunID(PwxProjectID, testruns[i], filter)
		if runID == 0 {
			log.Debugf("Creating run %s for milestone %s", testruns[i], milestone)
			includeAll := false
			sendableRun := testrail.SendableRun{
				SuiteID:     PwxProjectID,
				Name:        testruns[i],
				Description: "Test run created from Jenkins trigger",
				MilestoneID: milestoneID,
				CaseIDs:     []int{testrailID},
				IncludeAll:  &includeAll,
			}
			createdRun, err := client.AddRun(PwxProjectID, sendableRun)
			if err != nil {
				log.Errorf("Unable to add the run %s", err)
			}
			runID = createdRun.ID
			runIDs = append(runIDs, runID)
		} else {
			log.Debugf("Run already exists for Job: %s - %d , checking for tests\n", testruns[i], runID)
			testID := getTestIDForRunID(runID, testrailID)
			log.Debugf("Test id received is %d", testID)
			if testID == 0 {
				log.Debugf("Adding new test to the test run %s\n", testruns[i])
				var existingCases []int
				allTests, err := client.GetTests(runID)
				if err != nil {
					log.Errorf("Error in getting tests for runid: %d", runID)
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
					log.Errorf("Error updating the run with new case %s \n", err)
				}
			} else {
				log.Debugf("Test Already exists, reusing the test")
			}
			runIDs = append(runIDs, runID)
		}
	}
	return runIDs
}

// AddTestEntry adds a test entry with values passed in testrail object.
func AddTestEntry(testrailObject Testrail) {
	if !testRailConnectionSuccessful {
		return
	}
	log.Infof("Adding test entry testID %d, run Id is %d, status %s", testrailObject.TestID, testrailObject.RunID, testrailObject.Status)
	tests, err := client.GetTests(testrailObject.RunID)
	if err != nil {
		log.Errorf("Error getting tests for %d", testrailObject.RunID)
	}
	for _, test := range tests {
		if test.CaseID == testrailObject.TestID {
			log.Debugf("Found test to update %d", testrailObject.TestID)
			var statusID int
			if testrailObject.Status == "Pass" {
				statusID = testRailPassStatusCode
			} else {
				statusID = testRailFailStatusCode
			}
			sendableResult := testrail.SendableResult{
				StatusID: statusID,
				Comment: fmt.Sprintf("This is updated from Jenkins job for test runs %s, \n Job Run ID: %s, \n BuildUrl: %s, \n pxctl version: %s \n (Optional) Px Backup Version: %s",
					RunName, JobRunID, JenkinsBuildURL, testrailObject.DriverVersion, testrailObject.PxBackupVersion),
				Defects: "",
			}
			_, err = client.AddResult(test.ID, sendableResult)
			if err != nil {
				log.Errorf("Error in adding result to %d - %s", testrailObject.TestID, err)
			} else {
				log.Debugf("testrail update successful")
			}
		}
	}
}

func getRunID(projectID int, testrunName string, filter testrail.RequestFilterForRun) int {
	log.Infof("Getting the run details for project Id %d", projectID)
	allRuns, err := client.GetRuns(projectID, filter)
	if err != nil {
		log.Warnf("Error in getting all jobs from milestone %s ", err)
	}
	for _, run := range allRuns {
		if run.Name == testrunName {
			log.Debugf("Run %s already exsits in milestone not adding it\n", testrunName)
			return run.ID
		}
	}
	return 0
}

func getTestIDForRunID(runID int, testrailID int) int {
	log.Infof("Getting the Tests Id for run: %d, testrailID %d", runID, testrailID)
	tests, err := client.GetTests(runID)
	if err != nil {
		log.Warnf("Error getting tests for existing run %d", runID)
	}
	for _, test := range tests {
		if test.CaseID == testrailID {
			log.Debugf("Test already exists in the run, not adding new test")
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
		log.Errorf("Testrail connection not successful")
		return err
	}
	testRailConnectionSuccessful = true
	log.Infof("Connection to testrail [%s] with provided credentials is successful", hostname)
	return nil
}
