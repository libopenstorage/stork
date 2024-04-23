package flashblade

// NFS represents NFS configuration.
type NFS struct {
	Rules      string `json:"rules"`
	V3Enabled  bool   `json:"v3_enabled"`
	V41Enabled bool   `json:"v4_1_enabled"`
}

// SMB represents SMB configuration.
type SMB struct {
	Enabled bool `json:"enabled"`
}

// HTTP represents HTTP configuration.
type HTTP struct {
	Enabled bool `json:"enabled"`
}

// MultiProtocol represents multi-protocol configuration.
type MultiProtocol struct {
	SafeguardACLs      bool   `json:"safeguard_acls"`
	AccessControlStyle string `json:"access_control_style"`
}

// Space represents space information.
type Space struct {
	Virtual       int64   `json:"virtual"`
	Unique        int64   `json:"unique"`
	Snapshots     int64   `json:"snapshots"`
	DataReduction float64 `json:"data_reduction"`
	TotalPhysical int64   `json:"total_physical"`
}

// Source represents source information.
type Source struct {
	Name         interface{} `json:"name"`
	ID           interface{} `json:"id"`
	ResourceType interface{} `json:"resource_type"`
	Location     struct {
		*FSResourceGroup
	} `json:"location"`
	IsLocal     interface{} `json:"is_local"`
	DisplayName interface{} `json:"display_name"`
}

// Item represents each item in the JSON array.
type FsItem struct {
	Name                    string        `json:"name"`
	Created                 int64         `json:"created"`
	FastRemoveDirectory     bool          `json:"fast_remove_directory_enabled"`
	SnapshotDirectory       bool          `json:"snapshot_directory_enabled"`
	Destroyed               bool          `json:"destroyed"`
	PromotionStatus         string        `json:"promotion_status"`
	RequestedPromotionState string        `json:"requested_promotion_state"`
	Writable                bool          `json:"writable"`
	TimeRemaining           interface{}   `json:"time_remaining"`
	NFS                     NFS           `json:"nfs"`
	SMB                     SMB           `json:"smb"`
	HTTP                    HTTP          `json:"http"`
	MultiProtocol           MultiProtocol `json:"multi_protocol"`
	HardLimitEnabled        bool          `json:"hard_limit_enabled"`
	DefaultUserQuota        int64         `json:"default_user_quota"`
	DefaultGroupQuota       int64         `json:"default_group_quota"`
	Space                   Space         `json:"space"`
	Source                  Source        `json:"source"`
}

// Response represents the entire JSON response.
type FSResponse struct {
	Total struct {
		Name                    interface{} `json:"name"`
		Created                 interface{} `json:"created"`
		FastRemoveDirectory     interface{} `json:"fast_remove_directory_enabled"`
		SnapshotDirectory       interface{} `json:"snapshot_directory_enabled"`
		Destroyed               interface{} `json:"destroyed"`
		PromotionStatus         interface{} `json:"promotion_status"`
		RequestedPromotionState interface{} `json:"requested_promotion_state"`
		Writable                interface{} `json:"writable"`
		TimeRemaining           interface{} `json:"time_remaining"`
		NFS                     interface{} `json:"nfs"`
		SMB                     interface{} `json:"smb"`
		HTTP                    interface{} `json:"http"`
		MultiProtocol           interface{} `json:"multi_protocol"`
		HardLimitEnabled        interface{} `json:"hard_limit_enabled"`
		DefaultUserQuota        interface{} `json:"default_user_quota"`
		DefaultGroupQuota       interface{} `json:"default_group_quota"`
		Space                   Space       `json:"space"`
		Source                  Source      `json:"source"`
		ID                      interface{} `json:"id"`
		Provisioned             interface{} `json:"provisioned"`
	} `json:"total"`
	ContinuationToken interface{} `json:"continuation_token"`
	TotalItemCount    int         `json:"total_item_count"`
	Items             []FsItem    `json:"items"`
}

// policy Structs
type FSResourceGroup struct {
	Name         interface{} `json:"name"`
	ID           interface{} `json:"id"`
	ResourceType interface{} `json:"resource_type"`
}

type FSMembers struct {
	Member FSResourceGroup `json:"member"`
}

type FSPolicy struct {
	Policy FSResourceGroup `json:"policy"`
}

type Policies struct {
	FSMembers
	FSPolicy
}

type PolicyResponse struct {
	ContinuationToken interface{} `json:"continuation_token"`
	TotalItemCount    int         `json:"total_item_count"`
	Items             []Policies  `json:"items"`
}
