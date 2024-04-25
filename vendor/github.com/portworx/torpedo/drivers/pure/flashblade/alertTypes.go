package flashblade

type AlertItem struct {
	Name             string `json:"name"`
	ID               string `json:"id"`
	Action           string `json:"action"`
	Code             int    `json:"code"`
	ComponentName    string `json:"component_name"`
	ComponentType    string `json:"component_type"`
	Created          int    `json:"created"`
	Description      string `json:"description"`
	Flagged          bool   `json:"flagged"`
	Index            int    `json:"index"`
	KnowledgeBaseURL string `json:"knowledge_base_url"`
	Notified         int    `json:"notified"`
	Severity         string `json:"severity"`
	State            string `json:"state"`
	Summary          string `json:"summary"`
	Updated          int    `json:"updated"`
	Variables        struct {
		Property1 string `json:"property1"`
		Property2 string `json:"property2"`
	} `json:"variables"`
}

type AlertResponse struct {
	ContinuationToken string      `json:"continuation_token"`
	TotalItemCount    int         `json:"total_item_count"`
	Items             []AlertItem `json:"items"`
}
