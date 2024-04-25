package flashblade

type NetItem struct {
	Name     string           `json:"name"`
	Address  string           `json:"address"`
	Enabled  bool             `json:"enabled"`
	Gateway  string           `json:"gateway"`
	Mtu      int              `json:"mtu"`
	Netmask  string           `json:"netmask"`
	Services []string         `json:"services"`
	Subnet   NetworkInterface `json:"subnet"`
	Type     string           `json:"type"`
	Vlan     int              `json:"vlan"`
	ID       string           `json:"id"`
}

type NetResponse struct {
	ContinuationToken interface{} `json:"continuation_token"`
	TotalItemCount    int         `json:"total_item_count"`
	Items             []NetItem   `json:"items"`
}

type NetworkInterface struct {
	Name         string `json:"name"`
	ID           string `json:"id"`
	ResourceType string `json:"resource_type"`
}

type SubNetItem struct {
	Name                 string             `json:"name"`
	Enabled              bool               `json:"enabled"`
	Gateway              string             `json:"gateway"`
	Interfaces           []NetworkInterface `json:"interfaces"`
	LinkAggregationGroup NetworkInterface   `json:"link_aggregation_group"`
	Mtu                  int                `json:"mtu"`
	Prefix               string             `json:"prefix"`
	Services             []string           `json:"services"`
	Vlan                 int                `json:"vlan"`
	ID                   string             `json:"id"`
}

type SubNetResponse struct {
	ContinuationToken interface{}  `json:"continuation_token"`
	TotalItemCount    int          `json:"total_item_count"`
	Items             []SubNetItem `json:"items"`
}
