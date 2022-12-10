package bitbucket

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type PullRequests struct {
	c *Client
}

// APIv1Merge implements Pull Request Merge API for BitBucket server API 1.0
func (p *PullRequests) APIv1Merge(po *PullRequestsOptions) (interface{}, error) {
	data, err := p.buildPullRequestBody(po)
	if err != nil {
		return nil, err
	}
	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests/%s/merge?version=%d", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug, po.ID, po.Version)
	return p.c.execute("POST", urlStr, data)
}

// APIv1Create implements BitBucket Server API
func (p *PullRequests) APIv1Create(po *PullRequestsOptions) (interface{}, error) {
	// more details here https://developer.atlassian.com/server/bitbucket/rest/v803/api-group-projects/#api-projects-projectkey-repos-repositoryslug-pull-requests-post
	reviewers := buildReviewers(po.Reviewers)
	body := map[string]interface{}{
		"title":          po.Title,
		"projectKey":     po.ProjectKey,
		"repositorySlug": po.RepoSlug,
		"fromRef": map[string]interface{}{
			"id":   po.SourceBranch,
			"type": "BRANCH",
			"repository": map[string]interface{}{
				"scmId": po.ScmID,
				"slug":  po.RepoSlug,
				"name":  po.FromRefName,
				"project": map[string]interface{}{
					"key": po.ProjectKey,
				},
			},
		},
		"toRef": map[string]interface{}{
			"id":   po.DestinationBranch,
			"type": "BRANCH",
			"repository": map[string]interface{}{
				"scmId": po.ScmID,
				"slug":  po.RepoSlug,
				"name":  po.ToRefName,
				"project": map[string]interface{}{
					"key": po.ProjectKey,
				},
			},
		},
		"reviewers":   reviewers,
		"type":        "BRANCH",
		"description": po.Description,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug)

	return p.c.execute("POST", urlStr, string(data))
}

func (p *PullRequests) Create(po *PullRequestsOptions) (interface{}, error) {
	data, err := p.buildPullRequestBody(po)
	if err != nil {
		return nil, err
	}
	urlStr := p.c.requestUrl("/repositories/%s/%s/pullrequests/", po.Owner, po.RepoSlug)
	return p.c.execute("POST", urlStr, data)
}

func (p *PullRequests) Update(po *PullRequestsOptions) (interface{}, error) {
	data, err := p.buildPullRequestBody(po)
	if err != nil {
		return nil, err
	}
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID
	return p.c.execute("PUT", urlStr, data)
}

func (p *PullRequests) Gets(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/"

	if po.States != nil && len(po.States) != 0 {
		parsed, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		query := parsed.Query()
		for _, state := range po.States {
			query.Set("state", state)
		}
		parsed.RawQuery = query.Encode()
		urlStr = parsed.String()
	}

	if po.Query != "" {
		parsed, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		query := parsed.Query()
		query.Set("q", po.Query)
		parsed.RawQuery = query.Encode()
		urlStr = parsed.String()
	}

	if po.Sort != "" {
		parsed, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		query := parsed.Query()
		query.Set("sort", po.Sort)
		parsed.RawQuery = query.Encode()
		urlStr = parsed.String()
	}

	return p.c.executePaginated("GET", urlStr, "")
}

// APIv1Get implements Pull Request Get API for BitBucket server API 1.0
func (p *PullRequests) APIv1Get(po *PullRequestsOptions) (interface{}, error) {
	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests/?state=ALL", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug)
	return p.c.execute("GET", urlStr, "")
}

// APIv1Delete implements Pull Request Delete API for BitBucket server API 1.0
func (p *PullRequests) APIv1Delete(po *PullRequestsOptions) (interface{}, error) {
	body := map[string]interface{}{}
	body["version"] = po.Version

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests/%s", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug, po.ID)
	return p.c.execute("DELETE", urlStr, string(data))
}

func (p *PullRequests) API1v1GetActivities(po *PullRequestsOptions) (interface{}, error) {
	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests/%s/activities", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug, po.ID)
	return p.c.execute("GET", urlStr, "")
}

func (p *PullRequests) APIv1Decline(po *PullRequestsOptions) (interface{}, error) {
	body := map[string]interface{}{}
	body["version"] = po.Version
	body["state"] = "DECLINED"
	body["comment"] = po.Comment

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	urlStr := fmt.Sprintf("%s/rest/api/1.0/projects/%s/repos/%s/pull-requests/%s/decline", p.c.GetApiBaseURL(), po.ProjectKey, po.RepoSlug, po.ID)
	return p.c.execute("POST", urlStr, string(data))
}

func (p *PullRequests) Get(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID
	return p.c.execute("GET", urlStr, "")
}

func (p *PullRequests) Activities(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/activity"
	return p.c.executePaginated("GET", urlStr, "")
}

func (p *PullRequests) Activity(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/activity"
	return p.c.execute("GET", urlStr, "")
}

func (p *PullRequests) Commits(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/commits"
	return p.c.executePaginated("GET", urlStr, "")
}

func (p *PullRequests) Patch(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/patch"
	return p.c.executeRaw("GET", urlStr, "")
}

func (p *PullRequests) Diff(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/diff"
	return p.c.executeRaw("GET", urlStr, "")
}

func (p *PullRequests) Merge(po *PullRequestsOptions) (interface{}, error) {
	data, err := p.buildPullRequestBody(po)
	if err != nil {
		return nil, err
	}
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/merge"
	return p.c.execute("POST", urlStr, data)
}

func (p *PullRequests) Decline(po *PullRequestsOptions) (interface{}, error) {
	data, err := p.buildPullRequestBody(po)
	if err != nil {
		return nil, err
	}
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/decline"
	return p.c.execute("POST", urlStr, data)
}

func (p *PullRequests) Approve(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/approve"
	return p.c.execute("POST", urlStr, "")
}

func (p *PullRequests) UnApprove(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/approve"
	return p.c.execute("DELETE", urlStr, "")
}

func (p *PullRequests) RequestChanges(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/request-changes"
	return p.c.execute("POST", urlStr, "")
}

func (p *PullRequests) UnRequestChanges(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/request-changes"
	return p.c.execute("DELETE", urlStr, "")
}

func (p *PullRequests) AddComment(co *PullRequestCommentOptions) (interface{}, error) {
	data, err := p.buildPullRequestCommentBody(co)
	if err != nil {
		return nil, err
	}

	urlStr := p.c.requestUrl("/repositories/%s/%s/pullrequests/%s/comments", co.Owner, co.RepoSlug, co.PullRequestID)
	return p.c.execute("POST", urlStr, data)
}

func (p *PullRequests) UpdateComment(co *PullRequestCommentOptions) (interface{}, error) {
	data, err := p.buildPullRequestCommentBody(co)
	if err != nil {
		return nil, err
	}

	urlStr := p.c.requestUrl("/repositories/%s/%s/pullrequests/%s/comments/%s", co.Owner, co.RepoSlug, co.PullRequestID, co.CommentId)
	return p.c.execute("PUT", urlStr, data)
}

func (p *PullRequests) GetComments(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/comments/"
	return p.c.executePaginated("GET", urlStr, "")
}

func (p *PullRequests) GetComment(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/comments/" + po.CommentID
	return p.c.execute("GET", urlStr, "")
}

func (p *PullRequests) Statuses(po *PullRequestsOptions) (interface{}, error) {
	urlStr := p.c.GetApiBaseURL() + "/repositories/" + po.Owner + "/" + po.RepoSlug + "/pullrequests/" + po.ID + "/statuses"
	if po.Query != "" {
		parsed, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		query := parsed.Query()
		query.Set("q", po.Query)
		parsed.RawQuery = query.Encode()
		urlStr = parsed.String()
	}

	if po.Sort != "" {
		parsed, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		query := parsed.Query()
		query.Set("sort", po.Sort)
		parsed.RawQuery = query.Encode()
		urlStr = parsed.String()
	}
	return p.c.executePaginated("GET", urlStr, "")
}

func (p *PullRequests) buildPullRequestBody(po *PullRequestsOptions) (string, error) {
	body := map[string]interface{}{}
	body["source"] = map[string]interface{}{}
	body["destination"] = map[string]interface{}{}
	body["reviewers"] = []map[string]string{}
	body["title"] = ""
	body["description"] = ""
	body["message"] = ""
	body["close_source_branch"] = false

	if n := len(po.Reviewers); n > 0 {
		body["reviewers"] = make([]map[string]string, n)
		for i, uuid := range po.Reviewers {
			body["reviewers"].([]map[string]string)[i] = map[string]string{"uuid": uuid}
		}
	}

	if po.SourceBranch != "" {
		body["source"].(map[string]interface{})["branch"] = map[string]string{"name": po.SourceBranch}
	}

	if po.SourceRepository != "" {
		body["source"].(map[string]interface{})["repository"] = map[string]interface{}{"full_name": po.SourceRepository}
	}

	if po.DestinationBranch != "" {
		body["destination"].(map[string]interface{})["branch"] = map[string]interface{}{"name": po.DestinationBranch}
	}

	if po.DestinationCommit != "" {
		body["destination"].(map[string]interface{})["commit"] = map[string]interface{}{"hash": po.DestinationCommit}
	}

	if po.Title != "" {
		body["title"] = po.Title
	}

	if po.Description != "" {
		body["description"] = po.Description
	}

	if po.Message != "" {
		body["message"] = po.Message
	}

	if po.CloseSourceBranch == true || po.CloseSourceBranch == false {
		body["close_source_branch"] = po.CloseSourceBranch
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (p *PullRequests) buildPullRequestCommentBody(co *PullRequestCommentOptions) (string, error) {
	body := map[string]interface{}{}
	body["content"] = map[string]interface{}{
		"raw": co.Content,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func buildReviewers(defaultReviewers []string) []map[string]interface{} {
	var reviewers []map[string]interface{}
	for _, reviewer := range defaultReviewers {
		map1 := map[string]interface{}{
			"approved": true,
			"status":   "UNAPPROVED",
			"role":     "AUTHOR",
			"user": map[string]interface{}{
				"slug":        reviewer,
				"active":      true,
				"name":        reviewer,
				"type":        "NORMAL",
				"displayName": reviewer,
			}}
		reviewers = append(reviewers, map1)
	}
	return reviewers
}
