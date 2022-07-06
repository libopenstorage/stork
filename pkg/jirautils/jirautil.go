package jirautils

import (
	"bytes"

	jira "github.com/andygrunwald/go-jira"
	"github.com/sirupsen/logrus"
	"github.com/trivago/tgo/tcontainer"
)

var (
	client                     *jira.Client
	isJiraConnectionSuccessful bool

	//AccountID for bug assignment
	AccountID string
)

const (
	jiraURL = "https://portworx.atlassian.net/"
)

// Init function for the Jira
func Init(username, token string) {
	httpClient := jira.BasicAuthTransport{
		Username: username,
		Password: token,
	}
	var err error

	client, err = jira.NewClient(httpClient.Client(), jiraURL)
	if err != nil {
		isJiraConnectionSuccessful = false
		logrus.Errorf("Jira connection not successful, Cause: %v", err)

	} else {
		logrus.Info("Jira connection is successful")
	}

	isJiraConnectionSuccessful = true

}

// CreateIssue creates issue in jira
func CreateIssue(issueDesription, issueSummary string) (string, error) {

	issueKey := ""
	var err error

	if isJiraConnectionSuccessful {

		issueKey, err = createPTX(issueDesription, issueSummary)
	} else {
		logrus.Warn("Skipping issue creation as jira connection is not successful")
	}
	return issueKey, err

}

func getPTX(issueID string) {

	issue, _, err := client.Issue.Get(issueID, nil)
	logrus.Infof("Error: %v", err)

	logrus.Infof("%s: %+v\n", issue.Key, issue.Fields.Summary)

	logrus.Infof("%s: %s\n", issue.ID, issue.Fields.Summary)
	logrus.Info(issue.Fields.FixVersions[0].Name)

}

func createPTX(description, summary string) (string, error) {

	//Hardcoding the Priority to P1
	customFieldsMap := tcontainer.NewMarshalMap()
	customFieldsMap["customfield_11115"] = map[string]interface{}{
		"id":    "10936",
		"value": "P1 (High)",
	}

	i := jira.Issue{
		Fields: &jira.IssueFields{
			Assignee: &jira.User{
				AccountID: AccountID,
			},
			Description: description,
			Type: jira.IssueType{
				Name: "Bug",
			},
			Project: jira.Project{
				Key: "PTX",
			},
			FixVersions: []*jira.FixVersion{
				{
					Name: "master",
				},
			},
			AffectsVersions: []*jira.AffectsVersion{
				{
					Name: "master",
				},
			},
			Summary:  summary,
			Unknowns: customFieldsMap,
		},
	}
	issue, resp, err := client.Issue.Create(&i)

	logrus.Infof("Resp: %v", resp.StatusCode)
	issueKey := ""
	if resp.StatusCode == 201 {
		logrus.Info("Successfully created new jira issue.")
		logrus.Infof("Jira Issue: %+v\n", issue.Key)
		issueKey = issue.Key

	} else {
		logrus.Infof("Error while creating jira issue: %v", err)
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		newStr := buf.String()
		logrus.Infof(newStr)

	}
	return issueKey, err

}

func getProjects() {
	req, _ := client.NewRequest("GET", "rest/api/3/project/recent", nil)

	projects := new([]jira.Project)
	_, err := client.Do(req, projects)
	if err != nil {
		logrus.Info("Error while getting project")
		logrus.Error(err)
		return
	}

	for _, project := range *projects {

		logrus.Infof("%s: %s\n", project.Key, project.Name)
	}
}
