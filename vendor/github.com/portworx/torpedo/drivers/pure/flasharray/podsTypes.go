package flasharray

type Member struct {
	ResourceType string `json:"resource_type"`
	Name         string `json:"name"`
	ID           string `json:"id"`
}

type PodSpace struct {
	DataReduction    float64 `json:"data_reduction"`
	Snapshots        int     `json:"snapshots"`
	System           *string `json:"system"`
	ThinProvisioning float64 `json:"thin_provisioning"`
	TotalPhysical    int64   `json:"total_physical"`
	TotalUsed        int64   `json:"total_used"`
	TotalProvisioned int64   `json:"total_provisioned"`
	TotalReduction   float64 `json:"total_reduction"`
	Unique           int64   `json:"unique"`
	Virtual          int64   `json:"virtual"`
	UsedProvisioned  int64   `json:"used_provisioned"`
	Footprint        int64   `json:"footprint"`
	Shared           int     `json:"shared"`
	Replication      int     `json:"replication"`
}

type Array struct {
	Name           string  `json:"name"`
	ID             string  `json:"id"`
	Member         Member  `json:"member"`
	Status         string  `json:"status"`
	Progress       *string `json:"progress"`
	FrozenAt       *string `json:"frozen_at"`
	MediatorStatus string  `json:"mediator_status"`
	PreElected     bool    `json:"pre_elected"`
}
type PodItems struct {
	Name                    string            `json:"name"`
	ID                      string            `json:"id"`
	Members                 []Member          `json:"members"`
	Space                   PodSpace          `json:"space"`
	Source                  Source            `json:"source"`
	Destroyed               bool              `json:"destroyed"`
	Arrays                  []Array           `json:"arrays"`
	Footprint               int64             `json:"footprint"`
	TimeRemaining           int64             `json:"time_remaining"`
	PromotionStatus         string            `json:"promotion_status"`
	QuotaLimit              int               `json:"quota_limit"`
	EradicationConfig       EradicationConfig `json:"eradication_config"`
	RequestedPromotionState string            `json:"requested_promotion_state"`
	MediatorVersion         *string           `json:"mediator_version"`
	Mediator                string            `json:"mediator"`
	FailoverPreferences     []interface{}     `json:"failover_preferences"`
	LinkSourceCount         int               `json:"link_source_count"`
	LinkTargetCount         int               `json:"link_target_count"`
	ArrayCount              int               `json:"array_count"`
}

type PodTotal struct {
	Name                    *string           `json:"name"`
	ID                      *string           `json:"id"`
	Members                 []Member          `json:"members"`
	Space                   PodSpace          `json:"space"`
	Source                  Source            `json:"source"`
	Destroyed               *bool             `json:"destroyed"`
	Arrays                  []Array           `json:"arrays"`
	Footprint               int64             `json:"footprint"`
	TimeRemaining           int64             `json:"time_remaining"`
	PromotionStatus         *string           `json:"promotion_status"`
	QuotaLimit              *int              `json:"quota_limit"`
	EradicationConfig       EradicationConfig `json:"eradication_config"`
	RequestedPromotionState *string           `json:"requested_promotion_state"`
	MediatorVersion         *string           `json:"mediator_version"`
	Mediator                *string           `json:"mediator"`
	FailoverPreferences     []interface{}     `json:"failover_preferences"`
	LinkSourceCount         *int              `json:"link_source_count"`
	LinkTargetCount         *int              `json:"link_target_count"`
	ArrayCount              *int              `json:"array_count"`
}

type PodResponse struct {
	ContinuationToken  *string    `json:"continuation_token"`
	Items              []PodItems `json:"items"`
	MoreItemsRemaining bool       `json:"more_items_remaining"`
	Total              []PodTotal `json:"total"`
	TotalItemCount     *int       `json:"total_item_count"`
}
