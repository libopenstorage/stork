package flasharray

type Eth struct {
	Subtype       string `json:"subtype"`
	Subinterfaces []int  `json:"subinterfaces"`
	Address       string `json:"address"`
	MacAddress    string `json:"mac_address"`
	Mtu           int    `json:"mtu"`
	Gateway       string `json:"gateway"`
	Vlan          *int   `json:"vlan"`
	Netmask       string `json:"netmask"`
	Subnet        Subnet `json:"subnet"`
}
type Subnet struct {
	IP   string `json:"ip"`
	Mask string `json:"mask"`
}
type Fc struct {
	Wwn *string `json:"wwn"`
}

type NetworkInterface struct {
	Eth           Eth      `json:"eth"`
	Fc            Fc       `json:"fc"`
	Services      []string `json:"services"`
	Name          string   `json:"name"`
	Enabled       bool     `json:"enabled"`
	InterfaceType string   `json:"interface_type"`
	Speed         int      `json:"speed"`
}

type NetworkInterfaceResponse struct {
	ContinuationToken  interface{}        `json:"continuation_token"`
	MoreItemsRemaining bool               `json:"more_items_remaining"`
	TotalItemCount     *int               `json:"total_item_count"`
	Items              []NetworkInterface `json:"items"`
}
