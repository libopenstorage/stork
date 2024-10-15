package flasharray

// NetworkInterface struct for object returned by array
type NetworkServices struct {
	client *Client
}

// SetNetworkInterface modifies network interface attributes
func (n *NetworkServices) SetNetworkInterface(params map[string]string, data interface{}) ([]NetworkInterface, error) {

	req, _ := n.client.NewRequest("PATCH", "network-interfaces", params, data)
	m := []NetworkInterface{}
	_, err := n.client.Do(req, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListNetworkInterfaces list the attributes of the network interfaces
func (n *NetworkServices) ListNetworkInterfaces() ([]NetworkInterfaceResponse, error) {

	req, _ := n.client.NewRequest("GET", "network-interfaces", nil, nil)
	m := []NetworkInterfaceResponse{}
	_, err := n.client.Do(req, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetNetworkInterfaceEnabled enables or disables a network interface.
// Param: Takes interface name and enabled boolean parameter which means if enabled is true then interface will be enabled
// and if enabled is false then interface will be disabled.
// Returns an object describing the interface.
func (n *NetworkServices) SetNetworkInterfaceEnabled(iface string, enabled bool) ([]NetworkInterface, error) {

	params := make(map[string]string)
	params["names"] = iface
	data := map[string]bool{"enabled": enabled}
	m, err := n.SetNetworkInterface(params, data)
	if err != nil {
		return nil, err
	}

	return m, err
}
