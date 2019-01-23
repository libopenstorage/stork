// +build integrationtest

package integrationtest

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/storkctl"
	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/torpedo/drivers/node"
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	nodeDriverName      = "ssh"
	volumeDriverName    = "pxd"
	schedulerDriverName = "k8s"
	remotePairName      = "remoteclusterpair"
	remoteConfig        = "remoteconfigmap"
	specDir             = "./specs"
	pairFilePath        = "./specs/cluster-pair"
	pairFileName        = pairFilePath + "/cluster-pair.yaml"
	remoteFilePath      = "/tmp/kubeconfig"

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
	t.Run("Extender", testExtender)
	t.Run("HealthMonitor", testHealthMonitor)
	t.Run("Snapshot", testSnapshot)
	t.Run("CmdExecutor", asyncPodCommandTest)
	t.Run("Migration", testMigration)
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

// write kubbeconfig file to /tmp/kubeconfig
func dumpRemoteKubeConfig(configObject string) error {
	cm, err := k8s_ops.Instance().GetConfigMap(configObject, "kube-system")
	if err != nil {
		logrus.Errorf("Error reading config map: %v", err)
		return err
	}
	config := cm.Data["kubeconfig"]
	if len(config) == 0 {
		configErr := "Error reading kubeconfig: found empty remoteConfig in config map"
		return fmt.Errorf(configErr)
	}
	// dump to remoteFilePath
	return ioutil.WriteFile(remoteFilePath, []byte(config), 0644)
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

func createClusterPair(pairInfo map[string]string) error {
	err := os.MkdirAll(pairFilePath, 0777)
	if err != nil {
		logrus.Errorf("Unable to make directory (%v) for cluster pair spec: %v", pairFilePath, err)
		return err
	}
	pairFile, err := os.Create(pairFileName)
	if err != nil {
		logrus.Errorf("Unable to create clusterPair.yaml: %v", err)
		return err
	}
	defer pairFile.Close()

	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, pairFile, os.Stderr)
	cmd.SetArgs([]string{"generate", "clusterpair", remotePairName, "--kubeconfig", remoteFilePath})
	if err := cmd.Execute(); err != nil {
		logrus.Errorf("Execute storkctl failed: %v", err)
		return err
	}

	truncCmd := `sed -i "$((` + "`wc -l " + pairFileName + "|awk '{print $1}'`" + `-4)),$ d" ` + pairFileName
	logrus.Infof("trunc cmd: %v", truncCmd)
	err = exec.Command("sh", "-c", truncCmd).Run()
	if err != nil {
		logrus.Errorf("truncate failed %v", err)
		return err
	}

	// stokctl generate command sets k8s_ops to remoteclusterconfig
	err = setRemoteConfig("")
	if err != nil {
		logrus.Errorf("setting kubeconfig to default failed %v", err)
		return err
	}

	return addStorageOptions(pairInfo)
}

func addStorageOptions(pairInfo map[string]string) error {
	file, err := os.OpenFile(pairFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		logrus.Errorf("Unable to open %v: %v", pairFileName, err)
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	for k, v := range pairInfo {
		if k == "port" {
			// port is integer
			v = "\"" + v + "\""
		}
		_, err = fmt.Fprintf(w, "    %v: %v\n", k, v)
		if err != nil {
			logrus.Infof("error writing file %v", err)
			return err
		}
	}
	w.Flush()

	logrus.Info("cluster-pair.yml created")
	return nil
}

func scheduleClusterPair(ctx *scheduler.Context) error {
	var err error
	err = dumpRemoteKubeConfig(remoteConfig)
	if err != nil {
		logrus.Errorf("Unable to write clusterconfig: %v", err)
		return err
	}
	info, err := volumeDriver.GetClusterPairingInfo()
	if err != nil {
		logrus.Errorf("Error writing to clusterpair.yml: %v", err)
		return err
	}

	err = createClusterPair(info)
	if err != nil {
		logrus.Errorf("Error creating cluster Spec: %v", err)
		return err
	}

	err = schedulerDriver.RescanSpecs(specDir)
	if err != nil {
		logrus.Errorf("Unable to parse spec dir: %v", err)
		return err
	}

	err = schedulerDriver.AddTasks(ctx,
		scheduler.ScheduleOptions{AppKeys: []string{"cluster-pair"}})
	if err != nil {
		logrus.Errorf("Failed to schedule Cluster Pair Specs: %v", err)
		return err
	}

	err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
	if err != nil {
		logrus.Errorf("Error waiting to get cluster pair in ready state: %v", err)
		return err
	}

	return nil
}

func init() {
	flag.IntVar(&snapshotScaleCount,
		"snapshot-scale-count",
		10,
		"Number of volumes to use for scale snapshot test")
	flag.Parse()
}
