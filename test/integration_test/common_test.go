// +build integrationtest

package integrationtest

import (
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/portworx/sched-ops/k8s"
	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/node"
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	nodeDriverName      = "ssh"
	volumeDriverName    = "pxd"
	schedulerDriverName = "k8s"

	nodeScore   = 100
	rackScore   = 50
	zoneScore   = 25
	regionScore = 10

	defaultWaitTimeout  time.Duration = 5 * time.Minute
	defaultWaitInterval time.Duration = 10 * time.Second
)

var nodeDriver node.Driver
var schedulerDriver scheduler.Driver
var volumeDriver volume.Driver
var storkVolumeDriver storkdriver.Driver

var snapshotScaleCount int

// TODO: Start stork scheduler and stork extender
// TODO: Take driver name from input
// TODO: Parse storageclass specs based on driver name
func setup(t *testing.T) {
	var err error

	storkVolumeDriver, err = storkdriver.Get(volumeDriverName)
	require.NoError(t, err, "Error getting stork driver %v", volumeDriverName)

	err = storkVolumeDriver.Init(nil)
	require.NoError(t, err, "Error initializing stork driver %v", volumeDriverName)

	nodeDriver, err = node.Get(nodeDriverName)
	require.NoError(t, err, "Error getting node driver %v", nodeDriverName)

	err = nodeDriver.Init()
	require.NoError(t, err, "Error initializing node driver %v", nodeDriverName)

	schedulerDriver, err = scheduler.Get(schedulerDriverName)
	require.NoError(t, err, "Error getting scheduler driver %v", schedulerDriverName)

	err = schedulerDriver.Init("specs", volumeDriverName, nodeDriverName)
	require.NoError(t, err, "Error initializing scheduler driver %v", schedulerDriverName)

	volumeDriver, err = volume.Get(volumeDriverName)
	require.NoError(t, err, "Error getting volume driver %v", volumeDriverName)

	err = volumeDriver.Init(schedulerDriverName, nodeDriverName)
	require.NoError(t, err, "Error initializing volume driver %v", volumeDriverName)
}

func TestMain(t *testing.T) {
	// If setup fails stop the test
	if passed := t.Run("setup", setup); !passed {
		t.FailNow()
	}
	/*t.Run("Extender", testExtender)
	t.Run("HealthMonitor", testHealthMonitor)
	t.Run("Snapshot", testSnapshot)
	t.Run("CmdExecutor", asyncPodCommandTest)*/
	t.Run("testBasicCloudMigartion", testBasicCloudMigration)
}

func generateInstanceID(t *testing.T, testName string) string {
	id, err := uuid.New()
	require.NoError(t, err, "Error generating uuid for task")
	return testName + "-" + id.String()
}

func destroyAndWait(t *testing.T, ctxs []*scheduler.Context) {
	for _, ctx := range ctxs {
		err := schedulerDriver.Destroy(ctx, nil)
		require.NoError(t, err, "Error destroying ctx: %+v", ctx)
		err = schedulerDriver.WaitForDestroy(ctx)
		require.NoError(t, err, "Error waiting for destroy of ctx: %+v", ctx)
		_, err = schedulerDriver.DeleteVolumes(ctx)
		require.NoError(t, err, "Error deleting volumes in ctx: %+v", ctx)
	}
}

func getVolumeNames(t *testing.T, ctx *scheduler.Context) []string {
	volumeParams, err := schedulerDriver.GetVolumeParameters(ctx)
	require.NoError(t, err, "Error getting volume Parameters")

	var volumes []string
	for vol := range volumeParams {
		volumes = append(volumes, vol)
	}
	return volumes
}

func verifyScheduledNode(t *testing.T, appNode node.Node, volumes []string) {
	driverNodes, err := storkVolumeDriver.GetNodes()
	require.NoError(t, err, "Error getting nodes from stork driver")

	found := false
	for _, dNode := range driverNodes {
		if dNode.Hostname == appNode.Name {
			found = true
			break
		}
		for _, address := range appNode.Addresses {
			for _, ip := range dNode.IPs {
				if ip == address {
					dNode.Hostname = appNode.Name
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}
	require.Equal(t, true, found, "Scheduled node not found in driver node list. DriverNodes: %v ScheduledNode: %v", driverNodes, appNode)

	scores := make(map[string]int)
	idMap := make(map[string]*storkdriver.NodeInfo)
	rackMap := make(map[string][]string)
	zoneMap := make(map[string][]string)
	regionMap := make(map[string][]string)
	for _, dNode := range driverNodes {
		scores[dNode.Hostname] = 0
		idMap[dNode.ID] = dNode
		if dNode.Status == storkdriver.NodeOnline {
			if dNode.Rack != "" {
				rackMap[dNode.Rack] = append(rackMap[dNode.Rack], dNode.Hostname)
			}
			if dNode.Zone != "" {
				zoneMap[dNode.Zone] = append(rackMap[dNode.Zone], dNode.Hostname)
			}
			if dNode.Region != "" {
				regionMap[dNode.Region] = append(rackMap[dNode.Region], dNode.Hostname)
			}
		}
	}

	// Calculate scores for each node
	for _, vol := range volumes {
		volInfo, err := storkVolumeDriver.InspectVolume(vol)
		require.NoError(t, err, "Error inspecting volume %v", vol)

		for _, dataNode := range volInfo.DataNodes {
			hostname := idMap[dataNode].Hostname
			scores[hostname] += nodeScore

			if idMap[dataNode].Rack != "" {
				for _, node := range rackMap[idMap[dataNode].Rack] {
					if dataNode != node {
						scores[node] += rackScore
					}
				}
			}
			if idMap[dataNode].Zone != "" {
				for _, node := range zoneMap[idMap[dataNode].Zone] {
					if dataNode != node {
						scores[node] += zoneScore
					}
				}
			}
			if idMap[dataNode].Rack != "" {
				for _, node := range regionMap[idMap[dataNode].Region] {
					if dataNode != node {
						scores[node] += regionScore
					}
				}
			}
		}
	}

	highScore := 0
	for _, score := range scores {
		if score > highScore {
			highScore = score
		}
	}

	logrus.Infof("Scores: %v", scores)
	require.Equal(t, highScore, scores[appNode.Name], "Scheduled node does not have the highest score")
}

// createClusterPairSpec from specification
func CreateClusterPairSpec(req ClusterPairRequest) error {
	// parseKubeConfig file from configMap
	kubeSpec, err := parseKubeConfig(req.ConfigMapName)
	if err != nil {
		return err
	}

	// CreateClusterPair spec req
	remotePort, err := strconv.Atoi(req.RemotePort)
	if err != nil {
		return err
	}
	clusterPair := &ClusterPair{
		PairName:             req.PairName,
		RemoteIP:             req.RemoteIP,
		RemotePort:           remotePort,
		RemoteToken:          req.RemoteClusterToken,
		RemoteKubeServer:     kubeSpec.ClusterInfo[0].Cluster["server"],
		RemoteConfigAuthData: kubeSpec.ClusterInfo[0].Cluster["certificate-authority-data"],
		RemoteConfigKeyData:  kubeSpec.UserInfo[0].User["client-key-data"],
		RemoteConfigCertData: kubeSpec.UserInfo[0].User["client-certificate-data"],
	}

	// Create pair file
	t := template.New("clusterPair")
	t.Parse(clusterPairSpec)
	//This should be path of clusterpair yaml
	f, err := os.Create(req.SpecDirPath + pairFileName)
	if err != nil {
		logrus.Errorf("Unable to create clusterPair.yaml: %v", err)
		return err
	}
	if err := t.Execute(f, clusterPair); err != nil {
		logrus.Errorf("Couldn't write to clsuterPair.yaml: %v", err)
		return err
	}

	logrus.Info("Created Clusterpair file")
	return nil
}

// write kubbeconfig file to  /tmp/kubeconfig
func dumpRemoteKubeConfig(configObject string) error {
	cm, err := k8s.Instance().GetConfigMap(configObject, "kube-system")
	if err != nil {
		logrus.Errorf("Error reading config map: %v", err)
		return err
	}
	status := cm.Data["kubeconfig"]
	if len(status) == 0 {
		logrus.Info("found empty failure status for key:remoteConifg in config map")
		return fmt.Errorf("Empty kubeconfig for remote cluster")
	}
	// dump to remoteFilePath
	return ioutil.WriteFile(remoteFilePath, []byte(status), 0644)
}

func parseKubeConfig(configObject string) (*KubeConfigSpec, error) {
	var spec *KubeConfigSpec
	cm, err := k8s.Instance().GetConfigMap(configObject, "kube-system")
	if err != nil {
		logrus.Errorf("Error reading config map %v", err)
		return nil, err
	}

	status := cm.Data["kubeconfig"]
	if len(status) == 0 {
		logrus.Info("found empty failure status for key:remoteConifg in config map")
		return nil, fmt.Errorf("Empty kubeconfig for remote cluster")
	}
	err = yaml.Unmarshal([]byte(status), &spec)
	if err != nil {
		fmt.Println("Error parsing kubeconfig file", err)
		return nil, err
	}

	return spec, nil
}

func getContextCRD(specName string) (*scheduler.Context, error) {
	specs, err := schedulerDriver.ParseSpecs("./migrs/" + specName + ".yaml")
	if err != nil {
		logrus.Errorf("Unable to parse specs %v", err)
		return nil, err
	}

	ctx := &scheduler.Context{
		App: &spec.AppSpec{
			Key:      specName,
			SpecList: specs,
		},
	}

	return ctx, err
}

func setRemoteConfig(kubeConfig string) error {
	k8sOps := k8s_ops.Instance()
	if k8sOps == nil {
		return fmt.Errorf("Unable to get k8s ops instance")
	}

	if kubeConfig == "" {
		k8sOps.SetConfig(nil)
		return nil
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		return err
	}

	k8sOps.SetConfig(config)
	return nil
}

func createClusterPair() error {
	f, err := os.Create("./migrs/cluster-pair.yaml")
	defer f.Close()
	if err != nil {
		logrus.Errorf("Unable to create clusterPair.yaml: %v", err)
		return err
	}
	cmd := storkctl.NewCommand(os.Stdin, f, os.Stdout)
	cmd.SetArgs([]string{"generate", "clusterpair", "--kubeconfig", remoteFilePath})
	if err := cmd.Execute(); err != nil {
		return err
	}
	b, err := ioutil.ReadFile("./migrs/cluster-pair.yaml")
	if err != nil {
		logrus.Errorf("read file %v", err)
		return err
	}
	logrus.Infof("file created %v", string(b))
	return nil
}

func init() {
	flag.IntVar(&snapshotScaleCount,
		"snapshot-scale-count",
		10,
		"Number of volumes to use for scale snapshot test")
	flag.Parse()
}
