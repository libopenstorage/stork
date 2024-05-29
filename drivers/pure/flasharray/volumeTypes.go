package flasharray

type Space struct {
	DataReduction    float64 `json:"data_reduction"`
	Shared           *string `json:"shared"`
	Snapshots        int     `json:"snapshots"`
	System           *string `json:"system"`
	ThinProvisioning float64 `json:"thin_provisioning"`
	TotalPhysical    int64   `json:"total_physical"`
	TotalProvisioned int64   `json:"total_provisioned"`
	TotalReduction   float64 `json:"total_reduction"`
	Unique           int64   `json:"unique"`
	Virtual          int64   `json:"virtual"`
}

type Source struct {
	Name *string `json:"name"`
	ID   *string `json:"id"`
}

type Pod struct {
	Name *string `json:"name"`
	ID   *string `json:"id"`
}

type QoS struct {
	BandwidthLimit uint64 `json:"bandwidth_limit"`
	IopsLimit      uint64 `json:"iops_limit"`
}

type VolumeGroup struct {
	Name *string `json:"name"`
	ID   *string `json:"id"`
}

type Item struct {
	Space                   Space       `json:"space"`
	ConnectionCount         int         `json:"connection_count"`
	Provisioned             int64       `json:"provisioned"`
	Created                 int64       `json:"created"`
	Source                  Source      `json:"source"`
	Name                    string      `json:"name"`
	ID                      string      `json:"id"`
	Serial                  string      `json:"serial"`
	Destroyed               bool        `json:"destroyed"`
	TimeRemaining           *string     `json:"time_remaining"`
	HostEncryptionKeyStatus string      `json:"host_encryption_key_status"`
	RequestedPromotionState string      `json:"requested_promotion_state"`
	PromotionStatus         string      `json:"promotion_status"`
	Pod                     Pod         `json:"pod"`
	QoS                     QoS         `json:"qos"`
	Subtype                 string      `json:"subtype"`
	VolumeGroup             VolumeGroup `json:"volume_group"`
}

type Total struct {
	Space                   Space       `json:"space"`
	ConnectionCount         *int        `json:"connection_count"`
	Provisioned             int64       `json:"provisioned"`
	Created                 *int64      `json:"created"`
	Source                  Source      `json:"source"`
	Name                    *string     `json:"name"`
	ID                      *string     `json:"id"`
	Serial                  *string     `json:"serial"`
	Destroyed               *bool       `json:"destroyed"`
	TimeRemaining           *string     `json:"time_remaining"`
	HostEncryptionKeyStatus *string     `json:"host_encryption_key_status"`
	RequestedPromotionState *string     `json:"requested_promotion_state"`
	PromotionStatus         *string     `json:"promotion_status"`
	Pod                     Pod         `json:"pod"`
	QoS                     QoS         `json:"qos"`
	Subtype                 *string     `json:"subtype"`
	VolumeGroup             VolumeGroup `json:"volume_group"`
}

type VolResponse struct {
	ContinuationToken  *string `json:"continuation_token"`
	Items              []Item  `json:"items"`
	MoreItemsRemaining bool    `json:"more_items_remaining"`
	Total              []Total `json:"total"`
	TotalItemCount     *int    `json:"total_item_count"`
}
