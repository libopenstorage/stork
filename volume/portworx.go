package volume

import (
	"log"
	"os"
	"strings"

	dockerclient "github.com/fsouza/go-dockerclient"

	_ "github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	"github.com/libopenstorage/openstorage/cluster"
)

var (
	nodes []string
)

type driver struct {
	clusterManager cluster.Cluster
}

func (d *driver) String() string {
	return "pxd"
}

func (d *driver) Init() error {
	log.Printf("Using the Portworx volume driver.\n")

	n := "127.0.0.1"
	if len(nodes) > 0 {
		n = nodes[0]
	}

	if clnt, err := clusterclient.NewClusterClient("http://"+n+":9001", "v1"); err != nil {
		return err
	} else {
		d.clusterManager = clusterclient.ClusterManager(clnt)
	}

	if cluster, err := d.clusterManager.Enumerate(); err != nil {
		return err
	} else {
		log.Printf("The following Portworx nodes are in the cluster:\n")
		for _, n := range cluster.Nodes {
			log.Printf(
				"\tNode ID: %v\tNode IP: %v\tNode Status: %v\n",
				n.Id,
				n.DataIp,
				n.Status,
			)
		}
	}

	return nil
}

// Portworx runs as a container - so all we need to do is ask docker to
// stop the running portworx container.
func (d *driver) Stop(Ip string) error {
	endpoint := "tcp://" + Ip + ":2375"
	docker, err := dockerclient.NewClient(endpoint)
	if err != nil {
		return err
	} else {
		if err = docker.Ping(); err != nil {
			return err
		}
	}

	// Find the Portworx container

	// Stop the Portworx container

	return nil
}

func (d *driver) Start(Ip string) error {
	return nil
}

func init() {
	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")

	register("pxd", &driver{})
}
