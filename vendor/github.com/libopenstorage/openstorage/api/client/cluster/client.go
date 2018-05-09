package cluster

import (
	"errors"
	"strconv"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/api/client"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/secrets"
)

const (
	clusterPath     = "/cluster"
	secretPath      = "/secrets"
	loggingurl      = "/loggingurl"
	managementurl   = "/managementurl"
	fluentdhost     = "/fluentdconfig"
	tunnelconfigurl = "/tunnelconfig"
)

type clusterClient struct {
	c *client.Client
}

func newClusterClient(c *client.Client) cluster.Cluster {
	return &clusterClient{c: c}
}

// String description of this driver.
func (c *clusterClient) Name() string {
	return "ClusterManager"
}

func (c *clusterClient) Enumerate() (api.Cluster, error) {
	clusterInfo := api.Cluster{}

	if err := c.c.Get().Resource(clusterPath + "/enumerate").Do().Unmarshal(&clusterInfo); err != nil {
		return clusterInfo, err
	}
	return clusterInfo, nil
}

func (c *clusterClient) SetSize(size int) error {
	resp := api.ClusterResponse{}

	request := c.c.Get().Resource(clusterPath + "/setsize")
	request.QueryOption("size", strconv.FormatInt(int64(size), 16))
	if err := request.Do().Unmarshal(&resp); err != nil {
		return err
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *clusterClient) Inspect(nodeID string) (api.Node, error) {
	var resp api.Node
	request := c.c.Get().Resource(clusterPath + "/inspect/" + nodeID)
	if err := request.Do().Unmarshal(&resp); err != nil {
		return api.Node{}, err
	}
	return resp, nil
}

func (c *clusterClient) AddEventListener(cluster.ClusterListener) error {
	return nil
}

func (c *clusterClient) UpdateData(nodeData map[string]interface{}) error {
	return nil
}

func (c *clusterClient) UpdateLabels(nodeLabels map[string]string) error {
	return nil
}

func (c *clusterClient) GetData() (map[string]*api.Node, error) {
	return nil, nil
}

func (c *clusterClient) GetNodeIdFromIp(idIp string) (string, error) {
	var resp string
	request := c.c.Get().Resource(clusterPath + "/getnodeidfromip/" + idIp)
	if err := request.Do().Unmarshal(&resp); err != nil {
		return idIp, err
	}
	return resp, nil
}

func (c *clusterClient) NodeStatus() (api.Status, error) {
	var resp api.Status
	request := c.c.Get().Resource(clusterPath + "/nodestatus")
	if err := request.Do().Unmarshal(&resp); err != nil {
		return api.Status_STATUS_NONE, err
	}
	return resp, nil
}

func (c *clusterClient) PeerStatus(listenerName string) (map[string]api.Status, error) {
	var resp map[string]api.Status
	request := c.c.Get().Resource(clusterPath + "/peerstatus")
	request.QueryOption("name", listenerName)
	if err := request.Do().Unmarshal(&resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *clusterClient) Remove(nodes []api.Node, forceRemove bool) error {
	resp := api.ClusterResponse{}

	request := c.c.Delete().Resource(clusterPath + "/")

	for _, n := range nodes {
		request.QueryOption("id", n.Id)
	}
	request.QueryOption("forceRemove", strconv.FormatBool(forceRemove))

	if err := request.Do().Unmarshal(&resp); err != nil {
		return err
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}

	return nil
}

func (c *clusterClient) NodeRemoveDone(nodeID string, result error) {
}

func (c *clusterClient) Shutdown() error {
	return nil
}

func (c *clusterClient) Start(int, bool, string) error {
	return nil
}

func (c *clusterClient) DisableUpdates() error {
	c.c.Put().Resource(clusterPath + "/disablegossip").Do()
	return nil
}

func (c *clusterClient) EnableUpdates() error {
	c.c.Put().Resource(clusterPath + "/enablegossip").Do()
	return nil
}

func (c *clusterClient) GetGossipState() *cluster.ClusterState {
	var status *cluster.ClusterState

	if err := c.c.Get().Resource(clusterPath + "/gossipstate").Do().Unmarshal(&status); err != nil {
		return nil
	}
	return status
}

func (c *clusterClient) EnumerateAlerts(ts, te time.Time, resource api.ResourceType) (*api.Alerts, error) {
	a := api.Alerts{}
	request := c.c.Get().Resource(clusterPath + "/alerts/" + strconv.FormatInt(int64(resource), 10))
	if !te.IsZero() {
		request.QueryOption("timestart", ts.Format(api.TimeLayout))
		request.QueryOption("timeend", te.Format(api.TimeLayout))
	}
	if err := request.Do().Unmarshal(&a); err != nil {
		return nil, err
	}
	return &a, nil
}

func (c *clusterClient) ClearAlert(resource api.ResourceType, alertID int64) error {
	path := clusterPath + "/alerts/" + strconv.FormatInt(int64(resource), 10) + "/" + strconv.FormatInt(alertID, 10)
	request := c.c.Put().Resource(path)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}

func (c *clusterClient) EraseAlert(resource api.ResourceType, alertID int64) error {
	path := clusterPath + "/alerts/" + strconv.FormatInt(int64(resource), 10) + "/" + strconv.FormatInt(alertID, 10)
	request := c.c.Delete().Resource(path)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}

// SecretSetDefaultSecretKey sets the cluster wide secret key
func (c *clusterClient) SecretSetDefaultSecretKey(secretKey string, override bool) error {
	reqBody := &secrets.DefaultSecretKeyRequest{
		DefaultSecretKey: secretKey,
		Override:         override,
	}
	path := clusterPath + secretPath + "/defaultsecretkey"
	request := c.c.Put().Resource(path).Body(reqBody)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}

// SecretGetDefaultSecretKey returns cluster wide secret key's value
func (c *clusterClient) SecretGetDefaultSecretKey() (interface{}, error) {
	var defaultKeyResp interface{}
	path := clusterPath + secretPath + "/defaultsecretkey"
	request := c.c.Get().Resource(path)
	err := request.Do().Unmarshal(&defaultKeyResp)
	if err != nil {
		return defaultKeyResp, err
	}
	return defaultKeyResp, nil
}

// SecretSet the given value/data against the key
func (c *clusterClient) SecretSet(secretID string, secretValue interface{}) error {
	reqBody := &secrets.SetSecretRequest{
		SecretValue: secretValue,
	}
	path := clusterPath + secretPath
	request := c.c.Put().Resource(path).Body(reqBody)
	request.QueryOption(secrets.SecretKey, secretID)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}

// SecretGet retrieves the value/data for given key
func (c *clusterClient) SecretGet(secretID string) (interface{}, error) {
	var secResp interface{}
	path := clusterPath + secretPath
	request := c.c.Get().Resource(path)
	request.QueryOption(secrets.SecretKey, secretID)
	if err := request.Do().Unmarshal(&secResp); err != nil {
		return secResp, err
	}
	return secResp, nil
}

// SecretCheckLogin validates session with secret store
func (c *clusterClient) SecretCheckLogin() error {
	path := clusterPath + secretPath + "/verify"
	request := c.c.Get().Resource(path)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}

// SecretLogin create session with secret store
func (c *clusterClient) SecretLogin(secretType string, secretConfig map[string]string) error {
	reqBody := &secrets.SecretLoginRequest{
		SecretType:   secretType,
		SecretConfig: secretConfig,
	}
	path := clusterPath + secretPath + "/login"
	request := c.c.Post().Resource(path).Body(reqBody)
	resp := request.Do()
	if resp.Error() != nil {
		return resp.FormatError()
	}
	return nil
}
