package flasharray

type Spaces struct {
	DataReduction    float64 `json:"data_reduction"`
	Shared           int     `json:"shared"`
	Snapshots        int     `json:"snapshots"`
	System           *string `json:"system"`
	ThinProvisioning float64 `json:"thin_provisioning"`
	TotalUsed        int64   `json:"total_used"`
	TotalPhysical    int64   `json:"total_physical"`
	TotalProvisioned int64   `json:"total_provisioned"`
	TotalReduction   float64 `json:"total_reduction"`
	Unique           int64   `json:"unique"`
	Virtual          int64   `json:"virtual"`
	FootPrint        int64   `json:"footprint"`
}

type Qos struct {
	BandwidthLimit uint64 `json:"bandwidth_limit"`
	IopsLimit      uint64 `json:"iops_limit"`
}

type EradicationConfig struct {
	ManualEradication *string `json:"manual_eradication"`
}

type Items struct {
	Space             Spaces            `json:"space"`
	QuotaLimit        int               `json:"quota_limit"`
	Name              string            `json:"name"`
	ID                string            `json:"id"`
	Destroyed         bool              `json:"destroyed"`
	TimeRemaining     int64             `json:"time_remaining"`
	EradicationConfig EradicationConfig `json:"eradication_config"`
	QoS               Qos               `json:"qos"`
}

type Totals struct {
	Space             Spaces            `json:"space"`
	QuotaLimit        int               `json:"quota_limit"`
	Name              string            `json:"name"`
	ID                string            `json:"id"`
	Destroyed         bool              `json:"destroyed"`
	TimeRemaining     int64             `json:"time_remaining"`
	EradicationConfig EradicationConfig `json:"eradication_config"`
	QoS               Qos               `json:"qos"`
}

type RealmResponse struct {
	ContinuationToken  *string  `json:"continuation_token"`
	Items              []Items  `json:"items"`
	MoreItemsRemaining bool     `json:"more_items_remaining"`
	Total              []Totals `json:"total"`
	TotalItemCount     *int     `json:"total_item_count"`
}
