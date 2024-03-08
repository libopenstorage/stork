package anthos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/portworx/sched-ops/k8s/core"
	k8s "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
)

type Gcp struct {
	ComponentAccessServiceAccountKeyPath string `yaml:"componentAccessServiceAccountKeyPath"`
}

type HostConfig struct {
	Ip      string   `yaml:"ip"`
	Gateway string   `yaml:"gateway"`
	Netmask string   `yaml:"netmask"`
	Dns     []string `yaml:"dns"`
}

type Network struct {
	IpAllocationMode string     `yaml:"ipAllocationMode"`
	HostConfig       HostConfig `yaml:"hostconfig"`
}

type Workstation struct {
	Cpus         int     `yaml:"cpus"`
	DiskGB       int     `yaml:"diskGB"`
	DataDiskMB   int     `yaml:"dataDiskMB"`
	DataDiskName string  `yaml:"dataDiskName"`
	MemoryMB     int     `yaml:"memoryMB"`
	Name         string  `yaml:"name"`
	Network      Network `yaml:"network"`
	NtpServer    string  `yaml:"ntpServer"`
	ProxyUrl     string  `yaml:"proxyUrl"`
}

type FileRef struct {
	Entry string `yaml:"entry"`
	Path  string `yaml:"path"`
}

type Credentials struct {
	Address string  `yaml:"address"`
	FileRef FileRef `yaml:"fileRef"`
}

type vCenter struct {
	CaCertPath   string      `yaml:"caCertPath"`
	Cluster      string      `yaml:"cluster"`
	Credentials  Credentials `yaml:"credentials"`
	Datacenter   string      `yaml:"datacenter"`
	Datastore    string      `yaml:"datastore"`
	Folder       string      `yaml:"folder"`
	Network      string      `yaml:"network"`
	ResourcePool string      `yaml:"resourcePool"`
}

type AdminWorkstation struct {
	AdminWorkstation Workstation `yaml:"adminWorkstation"`
	Gcp              Gcp         `yaml:"gcp"`
	ProxyUrl         string      `yaml:"proxyUrl"`
	VCenter          vCenter     `yaml:"vCenter"`
}

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName                    = "anthos"
	adminUserName                = "ubuntu"
	homeDir                      = "/home/ubuntu"
	adminWsConfFile              = "admin-ws-config.yaml"
	adminKubeconfPath            = "/home/ubuntu/kubeconfig"
	gcpAccessFile                = "my-px-access.json"
	vcenterCrtFile               = "vcenter.crt"
	vcenterCredFile              = "credential.yaml"
	googleDownloadUrl            = "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads"
	googleCloudCliPkg            = "google-cloud-cli-428.0.0-linux-x86_64.tar.gz"
	upgradeAdminWsCmd            = "gkeadm upgrade admin-workstation"
	upgradePrepareCmd            = "gkectl prepare  --bundle-path /var/lib/gke/bundles/gke-onprem-vsphere-"
	upgradeUserClusterCmd        = "gkectl upgrade cluster"
	upgradeAdminClusterCmd       = "gkectl upgrade admin"
	listUserClustersCmd          = "gkectl list"
	labelKey                     = "cloud.google.com/gke-nodepool"
	userClusterDescribeCmd       = "gkectl describe clusters"
	adminWsIdRsa                 = "id_rsa"
	kubeConfig                   = "kubeconfig"
	kubeSystemNameSpace          = "kube-system"
	clusterApiKey                = "api"
	clusterApiValue              = "clusterapi"
	clusterApiContainer          = "clusterapi-controller-manager"
	vSphereCntrlManagerContainer = "vsphere-controller-manager"
	clusterGrpPath               = "k8s/clusterGroup0"
	gsUtilCmd                    = "./google-cloud-sdk/bin/gsutil"
	jsonInstances                = "/instances.json"
	userClusterConfPath          = "/home/ubuntu/user-cluster.yaml"
	adminClusterConfPath         = "/home/ubuntu/admin-cluster.yaml"
	errorTimeDuration            = 15 * time.Minute
	logCollectFrequencyDuration  = 15 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
	defaultWaitUpgradeRetry      = 10 * time.Second
)

var (
	versionReg = regexp.MustCompile(`\w.\w+.\w+-gke.\w+`)
	k8sCore    = k8s.Instance()
)

type AnthosInstance struct {
	Name             string
	HostName         string
	User             string
	PublicIpAddress  string
	PrivateIpAddress string
	IpV6IpAddress    string
	Passwd           string
	Key              string
	Port             string
	OS               string
	KernelVersion    string
	OSVersion        string
	Version          string
	ESXiHost         string
	PxClusterId      string
	Disks            []string
	Interfaces       []string
	IsJsonEmpty      bool
	IsWindows        bool
	VMSpec           any `json:"VMspec"`

	Datacenter string

	VcenterName                         string `json:",omitempty"`
	VcenterDatacenter                   string `json:",omitempty"`
	VcenterCluster                      string `json:",omitempty"`
	VcenterDatastore                    string `json:",omitempty"`
	VcenterResourcePool                 string `json:",omitempty"`
	VcenterHost                         string `json:",omitempty"`
	VcenterPCIPassthroughAllowedDevices string `json:",omitempty"`
	VcenterPCIPassthroughDeviceCount    int    `json:",omitempty"`

	DockerDisk   string
	JournalDisk  string
	MetadataDisk string
	CacheDisk    string
	Owner        string
	Lease        int
}

type anthos struct {
	version string
	kube.K8s
	adminWsSSHInstance  *ssh.SSH
	instances           []AnthosInstance
	adminWsNode         *node.Node
	adminWsKeyPath      string
	instPath            string
	confPath            string
	adminClusterUpgrade bool
}

// Init Initialize the driver
func (anth *anthos) Init(schedOpts scheduler.InitOptions) error {
	if schedOpts.AnthosAdminWorkStationNodeIP == "" {
		return fmt.Errorf("anthos admin workstation node is must for anthos scheduler")
	}
	if schedOpts.AnthosInstancePath == "" {
		return fmt.Errorf("anthos conf path is needed for anthos scheduler")
	}
	anth.adminWsSSHInstance = &ssh.SSH{}
	anth.adminWsNode = &node.Node{}
	anth.instPath = schedOpts.AnthosInstancePath
	anth.confPath = path.Join(anth.instPath, clusterGrpPath)
	anth.adminWsKeyPath = path.Join(anth.confPath, adminWsIdRsa)
	anth.adminWsNode.Name = schedOpts.AnthosAdminWorkStationNodeIP
	anth.adminWsNode.Addresses = append(anth.adminWsNode.Addresses, schedOpts.AnthosAdminWorkStationNodeIP)
	anth.adminWsNode.UsableAddr = schedOpts.AnthosAdminWorkStationNodeIP
	if err := anth.K8s.Init(schedOpts); err != nil {
		return err
	}
	if err := anth.setUserNameAndKey(); err != nil {
		return err
	}
	if err := anth.adminWsSSHInstance.Init(node.InitOptions{SpecDir: schedOpts.SpecDir}); err != nil {
		return err
	}
	if err := anth.unsetUserNameAndKey(); err != nil {
		return err
	}
	if err := anth.getVersion(); err != nil {
		return err
	}
	if len(schedOpts.UpgradeHops) > 0 && len(strings.Split(schedOpts.UpgradeHops, ",")) > 1 {
		anth.adminClusterUpgrade = true
	}
	log.Infof("Skip admin cluster upgrade is: [%t]", anth.adminClusterUpgrade)
	return nil
}

// execOnAdminWSNode execute command on admin workstation node
func (anth *anthos) execOnAdminWSNode(cmd string) (string, error) {
	if err := anth.setUserNameAndKey(); err != nil {
		return "", err
	}
	var connectOpts = node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	}
	out, err := anth.adminWsSSHInstance.RunCommand(*anth.adminWsNode, cmd, connectOpts)
	if err != nil {
		return out, err
	}
	if err := anth.unsetUserNameAndKey(); err != nil {
		return "", err
	}
	return out, err
}

// getVersion get anthos current version
func (anth *anthos) getVersion() error {
	cmd := "gkectl version"
	out, err := anth.execOnAdminWSNode(cmd)
	if err != nil {
		return err
	}
	matches := versionReg.FindAllString(out, -1)
	if len(matches) == 0 {
		return fmt.Errorf("unable to parse version from output: %s", out)
	}
	anth.version = matches[0]
	return nil
}

// UpgradeScheduler upgrade anthos scheduler and return time taken by user-cluster to upgrade
func (anth *anthos) UpgradeScheduler(version string) error {
	log.Info("Upgrading Anthos user cluster")
	if !versionReg.MatchString(version) {
		return fmt.Errorf("incorrect upgrade version: [%s] is provided", version)
	}
	if err := anth.VerifyUpgradeVersion(version); err != nil {
		return err
	}
	if err := anth.loadInstances(); err != nil {
		return err
	}
	if err := downloadAndInstallGsutils(); err != nil {
		return err
	}
	if err := anth.upgradeAdminWorkstation(version); err != nil {
		return err
	}
	startTime := time.Now()
	if err := anth.upgradeUserCluster(version); err != nil {
		return err
	}
	timeTaken := time.Since(startTime)
	log.Infof("Anthos user cluster took: %v time to complete the upgrade", timeTaken)
	if err := anth.RefreshNodeRegistry(); err != nil {
		return err
	}
	if err := anth.checkUserClusterNodesUpgradeTime(); err != nil {
		return err
	}
	if anth.adminClusterUpgrade {
		if err := anth.invokeUpgradeAdminCluster(version); err != nil {
			return err
		}
	}
	return nil
}

// invokeUpgradeAdminCluster start admin cluster upgrade
func (anth *anthos) invokeUpgradeAdminCluster(version string) error {
	log.Info("Upgrading admin cluster")
	initTime := time.Now()
	if err := anth.upgradeAdminCluster(version); err != nil {
		return err
	}
	timeTaken := time.Since(initTime)
	log.Infof("Anthos upgrade took: %v time to complete upgrade from %s to %s version",
		timeTaken, anth.version, version)
	if err := anth.updateNodeInstance(); err != nil {
		return err
	}
	if err := anth.saveInstance(); err != nil {
		return err
	}
	return nil
}

// updateNodeInstance will update the host info after upgrade
func (anth *anthos) updateNodeInstance() error {
	log.Info("Updating node Instance")
	var startAdminNodeIndex int = 2
	var lastAdminNodeIndex int = 4
	k8sOps, err := core.NewInstanceFromConfigFile(path.Join(anth.confPath, kubeConfig))
	if err != nil {
		return err
	}
	adminNodeList, err := k8sOps.GetNodes()
	if err != nil {
		return err
	}
	adminIndex := startAdminNodeIndex
	for _, adminNode := range adminNodeList.Items {
		if adminIndex > lastAdminNodeIndex {
			break
		}
		anth.instances[adminIndex].HostName = adminNode.Name
		anth.instances[adminIndex].PublicIpAddress = adminNode.Status.Addresses[0].Address
		anth.instances[adminIndex].PrivateIpAddress = adminNode.Status.Addresses[0].Address
		adminIndex += 1
	}
	return nil
}

// saveInstance save the instances.json after upgrade
func (anth *anthos) saveInstance() error {
	log.Debug("Saving instances.json")
	b, err := json.MarshalIndent(anth.instances, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshall Instances, %v", err)
	}
	if os.WriteFile(path.Join(anth.instPath, jsonInstances), b, 0644); err != nil {
		return fmt.Errorf("couldn't write to %s/%s: %v", anth.instPath, jsonInstances, err)
	}
	return nil
}

// verifyUpgradeVersion validates that correct version is provided for upgrade or not
func (anth *anthos) VerifyUpgradeVersion(upgradeVersion string) error {
	log.Infof("Checking the upgrade from version: [%s] to version: [%s]",
		anth.version, upgradeVersion)
	var vReg = regexp.MustCompile(`(^\w).(\w+).(\w+)`)
	parseV1 := strings.Split(anth.version, "-")
	parseV2 := strings.Split(upgradeVersion, "-")
	v1 := strings.TrimSpace(parseV1[0])
	v2 := strings.TrimSpace(parseV2[0])
	version1, err := version.NewVersion(v1)
	if err != nil {
		return err
	}
	version2, err := version.NewVersion(v2)
	if err != nil {
		return err
	}
	if version1.GreaterThanOrEqual(version2) {
		return fmt.Errorf("incorrect upgrade version:%s is provided."+
			"Upgrade version should be higher", upgradeVersion)
	}
	toVersion := vReg.FindAllStringSubmatch(v1, -1)
	fromVersion := vReg.FindAllStringSubmatch(v2, -1)
	val1, err := strconv.Atoi(toVersion[0][2])
	if err != nil {
		return err
	}
	val2, err := strconv.Atoi(fromVersion[0][2])
	if err != nil {
		return fmt.Errorf("failed to parse version: %s. Error: %v", fromVersion, err)
	}
	// Skip below check when current version is 1.16 and upgrading to version 1.28
	if !strings.Contains(anth.version, "1.16") && !strings.Contains(upgradeVersion, "1.28") {
		if (len(toVersion) > 0 && len(fromVersion) > 0) &&
			(toVersion[0][1] != fromVersion[0][1] || (val2-val1) > 1) {
			return fmt.Errorf("incorrect upgrade version:%s is provided."+
				"One major version upgrade support at a time", upgradeVersion)
		}
	}
	log.Debugf("Successfully verified the version:[%]", upgradeVersion)
	return nil
}

// updateGkeadmUtil update gkeadm version to given version
func (anth *anthos) updateGkeadmUtil(version string) error {
	log.Infof("Updating gkeadm to version: [%s]", version)
	src := fmt.Sprintf("gs://gke-on-prem-release/gkeadm/%s/linux/gkeadm", version)
	if out, err := exec.Command(gsUtilCmd, "cp", src, anth.confPath).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to download gkeadm : [%s], Err:(%v)", out, err)
	}
	gkeAdmCLI := fmt.Sprintf("%s/gkeadm", anth.confPath)
	if err := os.Chmod(gkeAdmCLI, 0755); err != nil {
		return err
	}
	return nil
}

// upgradeAdminWorkstation upgrade admin work-station node
func (anth *anthos) upgradeAdminWorkstation(version string) error {
	log.Infof("upgrading admin workstation node to version: %s", version)
	var re = regexp.MustCompile(`(?m)ubuntu\@([\d.]+)`)
	if err := anth.updateGkeadmUtil(version); err != nil {
		return err
	}
	if err := anth.updateAdminWorkstationNode(); err != nil {
		return err
	}
	gkeExecPath, err := getExecPath()
	if err != nil {
		return err
	}
	log.Debugf("Using path: [%s] for executing commands", gkeExecPath)
	gkeadmCmd := fmt.Sprintf("%s/gkeadm", anth.confPath)
	adminWsConfPath := path.Join(anth.confPath, adminWsConfFile)
	execCmd := exec.Command(gkeadmCmd,
		"upgrade", "admin-workstation", "--config", adminWsConfPath)
	execCmd.Dir = anth.confPath
	execCmd.Env = append(execCmd.Environ(), gkeExecPath)
	log.Debugf("Executing command: %v", execCmd)

	out, err := execCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to upgrade admin workstation: [%s]. Err: (%v)", out, err)
	}
	adminWsNewIp := re.FindAllStringSubmatch(string(out), -1)
	if len(adminWsNewIp) > 0 {
		anth.adminWsNode.Name = adminWsNewIp[0][1]
		anth.adminWsNode.Addresses = []string{adminWsNewIp[0][1]}
		anth.adminWsNode.UsableAddr = adminWsNewIp[0][1]
		anth.instances[0].PublicIpAddress = adminWsNewIp[0][1]
	}
	if err := anth.setUserNameAndKey(); err != nil {
		return err
	}
	err = anth.adminWsSSHInstance.TestConnection(*anth.adminWsNode, node.ConnectionOpts{
		Timeout:         defaultTestConnectionTimeout,
		TimeBeforeRetry: defaultWaitUpgradeRetry,
	})
	if err := anth.unsetUserNameAndKey(); err != nil {
		return err
	}
	if err != nil {
		return fmt.Errorf("admin work station node failed to come up after upgrade. Error: %v", err)
	}

	log.Debugf("Successfully upgraded the admin work-station node: %s", adminWsNewIp)
	return nil
}

// upgradeUserCluster upgrade user cluster to newer version
func (anth *anthos) upgradeUserCluster(version string) error {
	log.Infof("Upgrading user cluster to a newer version: %s", version)
	logChan := make(chan bool)
	enableControlplaneV2 := false
	controlPlaneEnableReg := regexp.MustCompile(`enableControlplaneV2:\s+true`)
	userClusterName, err := anth.getUserClusterName()
	if err != nil {
		return err
	}
	// Describe user cluster command help to identify dataplanev2 cluster
	cmd := fmt.Sprintf("%s --kubeconfig %s --cluster %s",
		userClusterDescribeCmd, adminKubeconfPath, userClusterName)
	log.Debugf("Executing command: %s", cmd)
	out, err := anth.execOnAdminWSNode(cmd)
	if err != nil {
		return fmt.Errorf("describing user cluster is failing: [%s]. Err: (%v)", out, err)
	}
	matches := controlPlaneEnableReg.FindAllString(out, -1)
	if len(matches) > 0 {
		log.Infof("controlplanev2 is enabled in cluster: [%s]", matches[0])
		enableControlplaneV2 = true
	}

	upgradeLogger := anth.startLogCollector(logChan, userClusterName, enableControlplaneV2)
	cmd = fmt.Sprintf("%s%s.tgz  --kubeconfig %s", upgradePrepareCmd, version, adminKubeconfPath)
	if out, err := anth.execOnAdminWSNode(cmd); err != nil {
		return fmt.Errorf("preparing user cluster for upgrade is failing: [%s]. Err: (%v)", out, err)
	}
	cmd = fmt.Sprintf("%s --kubeconfig %s --config %s",
		upgradeUserClusterCmd, adminKubeconfPath, userClusterConfPath)
	if out, err := anth.execOnAdminWSNode(cmd); err != nil {
		return fmt.Errorf("upgrading user cluster is failing: [%s]. Err: (%v)", out, err)
	}
	if err := anth.updateFileOwnership(homeDir); err != nil {
		return err
	}
	log.Debug("Successfully upgraded the user cluster")
	anth.stopLogCollector(upgradeLogger, logChan)
	return nil
}

// upgradeAdminCluster upgrades admin cluster
func (anth *anthos) upgradeAdminCluster(version string) error {
	log.Infof("Upgrading admin cluster to a newer version: %s", version)
	var cmd = fmt.Sprintf("cp /home/ubuntu/%s ~/", vcenterCrtFile)
	if _, err := anth.execOnAdminWSNode(cmd); err != nil {
		return fmt.Errorf("failed to copy vcenter certificate: %s. Err: %v", vcenterCrtFile, err)
	}
	cmd = fmt.Sprintf("%s --kubeconfig  %s --config %s",
		upgradeAdminClusterCmd, adminKubeconfPath, adminClusterConfPath)
	if out, err := anth.execOnAdminWSNode(cmd); err != nil {
		return fmt.Errorf("upgrading admin cluster is failing: [%s]. Err: (%v)", out, err)
	}
	if err := anth.updateFileOwnership(homeDir); err != nil {
		return err
	}
	log.Debug("Successfully upgraded the admin cluster")
	return nil
}

// loadInstance load the instances.json file
func (anth *anthos) loadInstances() error {
	log.Info("Loading the anthos admin instance")
	var instances []AnthosInstance
	b, err := ioutil.ReadFile(anth.instPath + "/instances.json")
	if err != nil {
		return fmt.Errorf("unable to read instances.json: %v", err)
	}
	if err := json.Unmarshal(b, &instances); err != nil {
		return fmt.Errorf("unable to unmarshal instances.json: %v", err)
	}
	anth.instances = instances
	return nil
}

// updateAdminWorkstationNode update container paths in admin-ws-config
func (anth *anthos) updateAdminWorkstationNode() error {
	log.Info("Updating admin workstation configs")
	adminWsConfigPath := path.Join(anth.confPath, adminWsConfFile)
	adminWsConfigYaml, err := os.Open(adminWsConfigPath)
	if err != nil {
		return err
	}
	defer adminWsConfigYaml.Close()
	adminWsConfigYamlContent, err := ioutil.ReadAll(adminWsConfigYaml)
	if err != nil {
		return err
	}
	var admWSObj AdminWorkstation
	err = yaml.Unmarshal(adminWsConfigYamlContent, &admWSObj)
	if err != nil {
		return err
	}
	admWSObj.Gcp.ComponentAccessServiceAccountKeyPath = path.Join(anth.instPath, gcpAccessFile)
	admWSObj.VCenter.Credentials.FileRef.Path = path.Join(anth.confPath, vcenterCredFile)
	admWSObj.VCenter.CaCertPath = path.Join(anth.confPath, vcenterCrtFile)
	out, err := yaml.Marshal(&admWSObj)
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(adminWsConfigPath, out, 0744); err != nil {
		return err
	}
	log.Debugf("[%s] file path successfully updated", adminWsConfigPath)
	return nil
}

// setUserNameAndKey set torpedo username and keypath
func (anth *anthos) setUserNameAndKey() error {
	if err := os.Setenv("TORPEDO_SSH_KEY", anth.adminWsKeyPath); err != nil {
		return err
	}
	if err := os.Setenv("TORPEDO_SSH_USER", adminUserName); err != nil {
		return err
	}
	return nil
}

// unsetUserNameAndKey unset torpedo username and keyPath
func (anth *anthos) unsetUserNameAndKey() error {
	if err := os.Unsetenv("TORPEDO_SSH_KEY"); err != nil {
		return err
	}
	if err := os.Unsetenv("TORPEDO_SSH_USER"); err != nil {
		return err
	}
	return nil
}

// checkUserClusterNodesUpgradeTime measure the time taken by each node and report error
func (anth *anthos) checkUserClusterNodesUpgradeTime() error {
	log.Info("Validating user cluster nodes upgrade time")
	userCluster, err := anth.getUserClusterName()
	if err != nil {
		return err
	}
	initNodeUpgradeTime, err := anth.getStartTimeForNodePoolUpgrade(userCluster)
	if err != nil {
		return err
	}
	log.Debugf("User cluster node pool upgrade started at: [%v]", initNodeUpgradeTime.Format(time.UnixDate))
	sortedNodes, err := getNodesSortByAge()
	if err != nil {
		return err
	}

	// As PX support one extra static IP across all node pool
	// this means Anthos node upgrade will be sequential
	startTime := initNodeUpgradeTime
	errorMessages := make([]string, 0)
	for _, node := range sortedNodes {
		diff := node.CreationTimestamp.Sub(startTime)
		log.Infof("[%s] node took: [%v] time to upgrade the node", node.Name, diff)
		if diff > errorTimeDuration {
			errorMessages = append(errorMessages, fmt.Sprintf("[%s] node upgrade took: [%v] minutes which is longer than the expected timeout value: [%v]",
				node.Name, diff, errorTimeDuration))
		}
		startTime = node.CreationTimestamp.Time
	}
	if len(errorMessages) > 0 {
		for _, errMsg := range errorMessages {
			log.Errorf(errMsg)
		}
		return fmt.Errorf("anthos node upgrade time exceeded the expected time")
	}
	return nil
}

// getUserClusterName return Anthos user cluster name
func (anth *anthos) getUserClusterName() (string, error) {
	log.Info("Retrieving user cluster name")
	var userCluster string
	// Listing user cluster to get user cluster name
	cmd := fmt.Sprintf("%s --kubeconfig %s clusters |grep -v NAME", listUserClustersCmd, adminKubeconfPath)
	out, err := anth.execOnAdminWSNode(cmd)
	if err != nil {
		return "", fmt.Errorf("listing user clusters is failing: [%s]. Err: (%v)", out, err)
	}
	userClusters := strings.Split(out, "\n")
	for _, cluster := range userClusters {
		clusterInfo := strings.Fields(cluster)
		userCluster = clusterInfo[1]
		break
	}
	if userCluster == "" {
		return "", fmt.Errorf("failed to find user cluster name")
	}
	log.Infof("Successfully retrieved user cluster name: [%s]", userCluster)
	return userCluster, nil
}

// getStartTimeForNodePoolUpgrade return start time when node pool upgrade started
func (anth *anthos) getStartTimeForNodePoolUpgrade(userClusterName string) (time.Time, error) {
	log.Info("Getting start time for node pool upgrade")
	var timeMatchReg = regexp.MustCompile(`\d+-\d+-\d+T\d+:\d+:\d+Z`)
	var nodeUpgradeStartedReg = regexp.MustCompile(`gke-on-prem-last-upgrade-start-time: .+`)
	layout := "2006-01-02T15:04:05Z"
	// Describe user cluster command provide last upgrade start time
	cmd := fmt.Sprintf("%s --kubeconfig %s --cluster %s",
		userClusterDescribeCmd, adminKubeconfPath, userClusterName)
	log.Debugf("Executing command: %s", cmd)
	out, err := anth.execOnAdminWSNode(cmd)
	if err != nil {
		return time.Time{}, fmt.Errorf("describing user cluster is failing: [%s]. Err: (%v)", out, err)
	}
	matches := nodeUpgradeStartedReg.FindAllString(out, -1)
	if matches == nil {
		return time.Time{}, fmt.Errorf("failed to match last-upgrade-start-time in describe user cluster command")
	}
	matchTimeInMinute := timeMatchReg.FindAllString(matches[0], -1)
	if matchTimeInMinute == nil {
		return time.Time{}, fmt.Errorf("failed to parse any line matching time")
	}
	startTime, err := time.Parse(layout, matchTimeInMinute[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse start time. Err: (%v)", err)
	}
	log.Debugf("Successfully retrieved startTime for user cluster [%s] upgrade: %s", userClusterName, startTime)
	return startTime, nil
}

// updateFileOwnership change file ownership to ubuntu user
func (anth *anthos) updateFileOwnership(dirPath string) error {
	cmd := fmt.Sprintf("chown -R %s:%s %s/*", adminUserName, adminUserName, dirPath)
	if out, err := anth.execOnAdminWSNode(cmd); err != nil {
		return fmt.Errorf("updating file permission after upgrade is failing: [%s]. Err: (%v)", out, err)
	}
	return nil
}

// dumpUpgradeLogs collects upgrade logs
func (anth *anthos) dumpUpgradeLogs(clusterName string, enableControlplaneV2 bool) error {
	adminKubeConfPath := path.Join(anth.confPath, kubeConfig)
	adminInstance, err := core.NewInstanceFromConfigFile(adminKubeConfPath)
	if err != nil {
		return fmt.Errorf("creating admin cluster instance failing with error. Err: (%v)", err)
	}
	if err := anth.collectUpgradeLogsInNameSpace(adminInstance, kubeSystemNameSpace, "admin"); err != nil {
		return err
	}
	userNamespaceInstance := adminInstance
	userLogNameSpace := clusterName
	if enableControlplaneV2 {
		log.Debugf("Collecting logs from usercluster kube-system namespace for dataplanev2: [%s]", enableControlplaneV2)
		userNamespaceInstance = k8sCore
		userLogNameSpace = kubeSystemNameSpace
	}
	// Collecting pods logs from usercluster namespace
	if err := anth.collectUpgradeLogsInNameSpace(userNamespaceInstance, userLogNameSpace, "user"); err != nil {
		return err
	}
	return nil
}

// collectUpgradeLogsInNameSpace collect upgrade logs for namespace provided
func (anth *anthos) collectUpgradeLogsInNameSpace(k8sInstance k8s.Ops, namespace string, clusterType string) error {
	clusterApiLabel := make(map[string]string, 0)
	clusterApiLabel[clusterApiKey] = clusterApiValue
	podList, err := k8sInstance.GetPods(namespace, clusterApiLabel)
	if err != nil {
		return fmt.Errorf("retrieving cluster api pod is failing with error. Err: (%v)", err)
	}
	if len(podList.Items) == 0 {
		return fmt.Errorf("no running pods found having label: [%v] in namespace: %s", clusterApiLabel, namespace)
	}

	// Collecting pods logs from kube-system namespace
	for _, pod := range podList.Items {
		if err = anth.writeContainerLog(k8sInstance, pod.Name, clusterApiContainer, namespace, clusterType); err != nil {
			return err
		}
		if err = anth.writeContainerLog(k8sInstance, pod.Name, vSphereCntrlManagerContainer, namespace, clusterType); err != nil {
			return err
		}
	}
	return nil
}

// writeContainerLog dump container logs into file
func (anth *anthos) writeContainerLog(k8sInstance k8s.Ops, podName string, containerName string, nameSpace string, clusterType string) error {
	layout := "2006-01-02T15:04:05Z"
	logOption := corev1.PodLogOptions{
		Container: containerName,
	}
	containerLog, err := k8sInstance.GetPodLog(podName, nameSpace, &logOption)
	if err != nil {
		return fmt.Errorf("unable to retrieve [%s] container logs. Err: (%v)", containerName, err)
	}
	logFileName := fmt.Sprintf("%s-%s-%v-%s.log", containerName, nameSpace, time.Now().Format(layout), clusterType)
	logPath := path.Join(anth.confPath, logFileName)
	if err = ioutil.WriteFile(logPath, []byte(containerLog), 0744); err != nil {
		return fmt.Errorf("unable to write log file for a container: [%s]. Err: (%v)", clusterApiContainer, err)
	}
	return nil
}

// startLogCollector start ticker for collecting upgrade logs
func (anth *anthos) startLogCollector(logChan chan bool, clusterName string, enableControlplaneV2 bool) *time.Ticker {
	logTicker := time.NewTicker(logCollectFrequencyDuration)
	go func() {
		for {
			select {
			case <-logChan:
				return
			case tm := <-logTicker.C:
				log.Debugf("Collecting upgrade logs at: %v", tm)
				if err := anth.dumpUpgradeLogs(clusterName, enableControlplaneV2); err != nil {
					log.Fatalf("Log collection fails with error. Err: (%v)", err)
				}
			}
		}
	}()
	return logTicker

}

// stopLogCollector stop ticker for collecting upgrade logs
func (anth *anthos) stopLogCollector(logTicker *time.Ticker, logChan chan bool) {
	log.Debugf("Stopping log collector at %t", time.Now())
	logTicker.Stop()
	logChan <- true
}

// getNodesSortByAge return sorted node list by their age
func getNodesSortByAge() ([]corev1.Node, error) {
	nodeList, err := k8sCore.GetNodes()
	if err != nil {
		return nil, err
	}

	sort.Slice(nodeList.Items, func(i, j int) bool {
		return nodeList.Items[i].CreationTimestamp.Before(&nodeList.Items[j].CreationTimestamp)
	})

	log.Infof("Successfully retrieved sorted nodes: [%v]", nodeList.Items)
	return nodeList.Items, nil
}

// downloadAndInstallGsutils download and install gsutil for google cloud
func downloadAndInstallGsutils() error {
	log.Info("Downloading gcloud sdk")
	var pkgName = "google-cloud-cli-linux.tar.gz"
	googleCliUrl := fmt.Sprintf("%s/%s", googleDownloadUrl, googleCloudCliPkg)
	out, err := exec.Command("curl", "-o", pkgName, googleCliUrl).CombinedOutput()
	if err != nil {
		return fmt.Errorf("[%s] downloading google cloud sdk failing: [%s]. Err: %v",
			googleCloudCliPkg, out, err)
	}
	out, err = exec.Command("tar", "-xvf", pkgName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("[%s] pkag extracting is failing: [%s]. Err: %v",
			pkgName, out, err)
	}
	log.Infof("Extracted %s successfully.", pkgName)
	out, err = exec.Command("apk", "add", "--update", "--no-cache", "python3").CombinedOutput()
	if err != nil {
		return fmt.Errorf("install python in container failing: [%s]. Err: %v", out, err)
	}
	out, err = exec.Command("ln", "-sf", "python3", "/usr/bin/python").CombinedOutput()
	if err != nil {
		return fmt.Errorf("linking python is failing: [%s]. Err: %v", out, err)
	}
	out, err = exec.Command("apk", "add", "--update", "--no-cache", "openssh").CombinedOutput()
	if err != nil {
		return fmt.Errorf("installing openssh is failing: [%s]. Err: %v", out, err)
	}

	return nil
}

// getExecPath return binaries exec path
func getExecPath() (string, error) {
	cmd := exec.Command("pwd")
	curWkDir, err := cmd.Output()
	if err != nil {
		return "", err
	}
	curDir := strings.TrimSpace(string(curWkDir))
	gcloudExecPath := path.Join(string(curDir), "google-cloud-sdk/bin")
	cmd = exec.Command("echo", os.Getenv("PATH"))
	execPath, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	envPath := strings.TrimSpace(string(execPath))
	return fmt.Sprintf("PATH=%s:%s:%s", curWkDir, envPath, gcloudExecPath), nil

}

// init registering anthos sheduler
func init() {
	anthos := &anthos{}
	scheduler.Register(SchedName, anthos)
}
