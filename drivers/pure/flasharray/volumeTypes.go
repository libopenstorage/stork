package flasharray

type PriorityAdjustment struct {
	PriorityAdjustmentOperator string `json:"priority_adjustment_operator"`
	PriorityAdjustmentValue    int    `json:"priority_adjustment_value"`
}

type Space struct {
	DataReduction      int   `json:"data_reduction"`
	Shared             int64 `json:"shared"`
	Snapshots          int   `json:"snapshots"`
	System             int   `json:"system"`
	ThinProvisioning   int   `json:"thin_provisioning"`
	TotalPhysical      int   `json:"total_physical"`
	TotalProvisioned   int64 `json:"total_provisioned"`
	TotalReduction     int   `json:"total_reduction"`
	Unique             int   `json:"unique"`
	Virtual            int   `json:"virtual"`
	UsedProvisioned    int64 `json:"used_provisioned"`
	TotalUsed          int   `json:"total_used"`
	SnapshotsEffective int   `json:"snapshots_effective"`
	UniqueEffective    int   `json:"unique_effective"`
	TotalEffective     int   `json:"total_effective"`
}

type Pod struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Source struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type VolumeGroup struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type Qos struct {
	BandwidthLimit int `json:"bandwidth_limit"`
	IopsLimit      int `json:"iops_limit"`
}

type VolItems struct {
	Id                      string              `json:"id"`
	Name                    string              `json:"name"`
	ConnectionCount         int                 `json:"connection_count"`
	Created                 int                 `json:"created"`
	Destroyed               bool                `json:"destroyed"`
	HostEncryptionKeyStatus string              `json:"host_encryption_key_status"`
	Provisioned             int                 `json:"provisioned"`
	Qos                     *Qos                `json:"qos"`
	PriorityAdjustment      *PriorityAdjustment `json:"priority_adjustment"`
	Serial                  string              `json:"serial"`
	Space                   *Space              `json:"space"`
	TimeRemaining           int                 `json:"time_remaining"`
	Pod                     *Pod                `json:"pod"`
	Source                  *Source             `json:"source"`
	Subtype                 string              `json:"subtype"`
	VolumeGroup             *VolumeGroup        `json:"volume_group"`
	RequestedPromotionState string              `json:"requested_promotion_state"`
	PromotionStatus         string              `json:"promotion_status"`
	Priority                int                 `json:"priority"`
}

type Volumes struct {
	Volumes []Volumes `json:"total"`
}
