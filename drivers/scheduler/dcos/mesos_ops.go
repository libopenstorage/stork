package dcos

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
)

const (
	defaultMesosHostname = "leader.mesos"
	defaultMesosPort     = "5050"
	getSlavesURL         = "master/slaves"
)

// Attributes are attributes on a mesos node
type Attributes struct {
	PublicIP string `json:"public_ip"`
}

// AgentNode is the agent node in a DC/OS cluster
type AgentNode struct {
	ID         string     `json:"id"`
	Hostname   string     `json:"hostname"`
	Attributes Attributes `json:"attributes"`
}

// GetAgentsResponse is the response received from get slaves call to mesos
type GetAgentsResponse struct {
	Agents []AgentNode `json:"slaves"`
}

// MesosOps is an interface to perform mesos related operations
type MesosOps interface {
	NodeOps
}

// NodeOps is an interface to perform node operations
type NodeOps interface {
	// GetPrivateAgentNodes gets all the private agents in a DC/OS cluster
	GetPrivateAgentNodes() ([]AgentNode, error)
}

var (
	mesosInstance MesosOps
	mesosOnce     sync.Once
)

type mesosOps struct {
	hostname string
	port     string
}

// MesosClient returns a singleton instance of MesosOps type
func MesosClient() MesosOps {
	mesosOnce.Do(func() {
		hostname := os.Getenv("MESOS_HOSTNAME")
		if len(hostname) == 0 {
			hostname = defaultMesosHostname
		}

		port := os.Getenv("MESOS_PORT")
		if len(port) == 0 {
			port = defaultMesosPort
		}

		mesosInstance = &mesosOps{
			hostname: hostname,
			port:     port,
		}
	})
	return mesosInstance
}

func (m *mesosOps) GetPrivateAgentNodes() ([]AgentNode, error) {
	url := &url.URL{
		Scheme: "http",
		Host:   m.hostname + ":" + m.port,
		Path:   getSlavesURL,
	}
	resp, err := http.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var agentsResp GetAgentsResponse
	err = json.Unmarshal(body, &agentsResp)
	if err != nil {
		return nil, err
	}

	var nodes []AgentNode
	for _, n := range agentsResp.Agents {
		// Filtering out public agent nodes. Public agent nodes will have the
		// public_ip attribute set to true to differentiate from other agents
		if n.Attributes.PublicIP == "" || n.Attributes.PublicIP != "true" {
			nodes = append(nodes, n)
		}
	}

	return nodes, nil
}
