package openshift

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	"github.com/libopenstorage/openstorage/api"
	opv1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	optest "github.com/libopenstorage/operator/pkg/util/test"
	openshiftv1 "github.com/openshift/api/config/v1"
	ocpsecurityv1api "github.com/openshift/api/security/v1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	k8s "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalsnapshotter"
	opnshift "github.com/portworx/sched-ops/k8s/openshift"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/node/vsphere"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/netutil"
	"github.com/portworx/torpedo/pkg/osutils"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "openshift"
	// SystemdSchedServiceName is the name of the system service responsible for scheduling
	SystemdSchedServiceName = "atomic-openshift-node"
	// OpenshiftMirror is the mirror we use do download ocp client
	OpenshiftMirror             = "https://mirror.openshift.com/pub/openshift-v4/clients/ocp"
	releaseFileName             = "release.txt"
	defaultCmdTimeout           = 5 * time.Minute
	driverUpTimeout             = 10 * time.Minute
	generationNumberWaitTime    = 10 * time.Minute
	defaultCmdRetry             = 15 * time.Second
	defaultUpgradeTimeout       = 4 * time.Hour
	defaultUpgradeRetryInterval = 5 * time.Minute
	ocPath                      = " -c oc"
	OpenshiftMachineNamespace   = "openshift-machine-api"

	// Default name for the Torpedo SecurityContextContaints that will be created for the OpenShift scheduler
	defaultTorpedoSecurityContextContaintsName = "torpedo-privileged"
)

var (
	k8sOpenshift       = opnshift.Instance()
	pxOperator         = operator.Instance()
	k8sCore            = k8s.Instance()
	crdOps             = apiextensions.Instance()
	snapshoterOps      = externalsnapshotter.Instance()
	versionReg         = regexp.MustCompile(`^(stable|candidate|fast)-(\d\.\d+)?$`)
	volumeSnapshotCRDs = []string{
		"volumesnapshotclasses.snapshot.storage.k8s.io",
		"volumesnapshotcontents.snapshot.storage.k8s.io",
		"volumesnapshots.snapshot.storage.k8s.io",
	}

	minPxOperatorVersionOcp_4_14, _ = version.NewVersion("23.10.3-") // PX Operator version that only supports OCP 4.14+
	minPxOperatorVersionOcpAll, _   = version.NewVersion("23.10.4-") // PX Operator version that supports all OCP versions

	openshiftVersion_4_9, _  = version.NewVersion("4.9.0")
	openshiftVersion_4_12, _ = version.NewVersion("4.12.0")
	openshiftVersion_4_13, _ = version.NewVersion("4.13.0")
	openshiftVersion_4_14, _ = version.NewVersion("4.14.0")
)

type openshift struct {
	kube.K8s
	openshiftVersion string
}

// String returns the string name of this driver.
func (k *openshift) String() string {
	return SchedName
}

func init() {
	k := &openshift{}
	scheduler.Register(SchedName, k)
}

func (k *openshift) StopSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.K8s.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         kube.FindFilesOnWorkerTimeout,
			TimeBeforeRetry: kube.DefaultRetryInterval,
		},
		Action: "stop",
	}
	if err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts); err != nil {
		return &scheduler.ErrFailedToStopSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

func (k *openshift) getServiceName(driver node.Driver, n node.Node) (string, error) {
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         kube.DefaultTimeout,
			TimeBeforeRetry: kube.DefaultRetryInterval,
		},
	}
	// if the service doesn't exist fallback to kubelet.service
	if ok, err := driver.SystemctlUnitExist(n, SystemdSchedServiceName, systemOpts); ok {
		return SystemdSchedServiceName, nil
	} else if err != nil {
		return "", err
	}
	return kube.SystemdSchedServiceName, nil
}

func (k *openshift) StartSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.K8s.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         kube.DefaultTimeout,
			TimeBeforeRetry: kube.DefaultRetryInterval,
		},
		Action: "start",
	}
	if err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts); err != nil {
		return &scheduler.ErrFailedToStartSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

func (k *openshift) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			spec, err := k.SpecFactory.Get(key)
			if err != nil {
				return nil, err
			}
			apps = append(apps, spec)
		}
	} else {
		apps = k.SpecFactory.GetAll()
	}

	var contexts []*scheduler.Context
	oldOptionsNamespace := options.Namespace
	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		if options.Namespace != "" {
			appNamespace = options.Namespace
		} else {
			options.Namespace = appNamespace
		}

		// Update security context for namespace and user
		if err := k.updateSecurityContextConstraints(appNamespace); err != nil {
			return nil, err
		}

		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		helmSpecObjects, err := k.HelmSchedule(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		specObjects = append(specObjects, helmSpecObjects...)
		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
			ScheduleOptions: options,
		}

		contexts = append(contexts, ctx)
		options.Namespace = oldOptionsNamespace
	}

	return contexts, nil
}

// ScheduleWithCustomAppSpecs Schedules the application with custom app specs
func (k *openshift) ScheduleWithCustomAppSpecs(apps []*spec.AppSpec, instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var contexts []*scheduler.Context
	oldOptionsNamespace := options.Namespace

	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		if options.Namespace != "" {
			appNamespace = options.Namespace
		} else {
			options.Namespace = appNamespace
		}

		// Update security context for namespace and user
		if err := k.updateSecurityContextConstraints(appNamespace); err != nil {
			return nil, err
		}

		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		helmSpecObjects, err := k.HelmSchedule(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		specObjects = append(specObjects, helmSpecObjects...)
		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
			ScheduleOptions: options,
		}

		contexts = append(contexts, ctx)
		options.Namespace = oldOptionsNamespace
	}

	return contexts, nil
}

func (k *openshift) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(k.K8s.NodeDriverName)

	usableServiceName := SystemdSchedServiceName
	serviceName, err := k.getServiceName(driver, n)
	if err != nil {
		return err
	}
	usableServiceName = serviceName

	cmd := fmt.Sprintf("journalctl -lu %s* > %s/kubelet.log", usableServiceName, location)
	if _, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	}); err != nil {
		return err
	}
	return nil
}

// updateSecurityContextConstraints updates Torpedo SecurityContextConstrains users to allow app provisioning in the given namespace
func (k *openshift) updateSecurityContextConstraints(namespace string) error {
	log.Debugf("Update Torpedo SecurityContextConstraints [%s]", defaultTorpedoSecurityContextContaintsName)

	// Check if Torpedo SecurityContextConstrains exists, if not create it
	torpedoScc, err := k8sOpenshift.GetSecurityContextConstraints(defaultTorpedoSecurityContextContaintsName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Create Torpedo SecurityContextConstrains
			log.Warnf("SecurityContextConstraints [%s] doesn't exist, will create it..", defaultTorpedoSecurityContextContaintsName)
			torpedoScc, err = k.createTorpedoSecurityContextConstraints()
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// Add user and namespace to SecurityContextConstrains
	torpedoScc.Users = append(torpedoScc.Users, "system:serviceaccount:"+namespace+":default")

	// Update SecurityContextConstrains
	if _, err := k8sOpenshift.UpdateSecurityContextConstraints(torpedoScc); err != nil {
		return err
	}
	return nil
}

// createTorpedoSecurityContextConstraints creates a Torpedo SecurityContextConstrains based on the existing default privileged SecurityContextConstrains
func (k *openshift) createTorpedoSecurityContextConstraints() (*ocpsecurityv1api.SecurityContextConstraints, error) {
	log.Debugf("Create Torpedo SecurityContextConstraints [%s]", defaultTorpedoSecurityContextContaintsName)

	// Get default privileged SecurityContextConstrains
	context, err := k8sOpenshift.GetSecurityContextConstraints("privileged")
	if err != nil {
		return nil, err
	}

	// Change name of the SecurityContextConstrains and remove ResourceVersion
	context.Name = defaultTorpedoSecurityContextContaintsName
	context.ResourceVersion = ""

	// Create Torpedo SecurityContextConstrains
	torpedoScc, err := k8sOpenshift.CreateSecurityContextConstraints(context)
	if err != nil {
		return nil, err
	}

	return torpedoScc, err
}

func (k *openshift) UpgradeScheduler(version string) error {
	if err := downloadOCP4Client(version); err != nil {
		return err
	}

	// Get Openshift version
	ocpVersion, err := optest.GetOpenshiftVersion()
	if err != nil {
		return fmt.Errorf("failed to get Openshift version, Err: %v", err)
	}
	k.openshiftVersion = ocpVersion

	clientVersion, err := getClientVersion()
	if err != nil {
		return err
	}

	upgradeVersion := version
	if versionReg.MatchString(version) {
		upgradeVersion = clientVersion
	}

	if err := k.setOcpPrometheusPrereq(upgradeVersion); err != nil {
		return err
	}

	if err := selectChannel(version); err != nil {
		return err
	}

	if err := ackAPIRemoval(upgradeVersion); err != nil {
		return err
	}

	if err := startUpgrade(upgradeVersion); err != nil {
		return err
	}

	if err := waitUpgradeCompletion(clientVersion); err != nil {
		return err
	}

	log.Info("Waiting for all the nodes to become ready...")
	if err := waitNodesToBeReady(); err != nil {
		return err
	}
	log.Infof("Cluster is now %s", upgradeVersion)
	return nil
}

func getClientVersion() (string, error) {
	t := func() (interface{}, bool, error) {
		var output []byte
		cmd := "oc version --client -o json|jq -r .releaseClientVersion"
		output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
		if err != nil {
			return "", true, fmt.Errorf("failed to get client version, Err: %v", err)
		}
		clientVersion := strings.TrimSpace(string(output))
		clientVersion = strings.Trim(clientVersion, "\"")
		clientVersion = strings.Trim(clientVersion, "'")
		return clientVersion, false, nil
	}
	output, err := task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second)
	if err != nil {
		return "", err
	}
	return output.(string), nil
}

// getGenerationNumber gets and returns ObservedGeneration number as an int
func getGenerationNumber() (int, error) {
	log.Info("Get ObservedGeneration number from ClusterVersion object...")
	clusterVersion, err := k8sOpenshift.GetClusterVersion("version")
	if err != nil {
		return 0, fmt.Errorf("failed to get ClusterVersion object, Err: %v", err)
	}
	log.Infof("ObservedGeneration number is [%d]", int(clusterVersion.Status.ObservedGeneration))
	return int(clusterVersion.Status.ObservedGeneration), nil
}

func waitForNewGenertionNumber(currentGenNumber int) error {
	// Wait upto 10 minutes to update generation number
	t := func() (interface{}, bool, error) {
		newGenNumInt, err := getGenerationNumber()
		if err != nil {
			return nil, true, fmt.Errorf("Failed to convert generator number from string to int, Err: %v", err)
		}
		if newGenNumInt == currentGenNumber {
			return nil, false, fmt.Errorf("Generation number has not changed yet: New [%d], Current [%d]", newGenNumInt, currentGenNumber)
		}
		log.Debugf("Set channel spec has been updated: Generation number [%d]", newGenNumInt)
		return nil, true, nil
	}
	if _, err := task.DoRetryWithTimeout(t, generationNumberWaitTime, 5*time.Second); err != nil {
		return err
	}
	return nil
}

func selectChannel(ocpVer string) error {
	log.Infof("Selecting channel for OCP version [%s]...", ocpVer)
	var output []byte
	var err error

	// Select channel
	channel, err := getChannel(ocpVer)
	if err != nil {
		return fmt.Errorf("failed to select channel for OCP version [%s], Err: %v", ocpVer, err)
	}

	// Get curent channel
	currentChannel, err := getCurrentChannel()
	if err != nil {
		return fmt.Errorf("failed to get current channel, Err: %v", err)
	}

	// Compare current and desired upgrade channel
	if channel == currentChannel {
		log.Infof("Current cluster channel [%s] and desired upgrade channel [%s] are same, setting channel is not required", currentChannel, channel)
		return nil
	}

	// Get current ObservedGeneration number
	beforeGenNumInt, err := getGenerationNumber()
	if err != nil {
		return fmt.Errorf("failed to get current ObservedGeneration number from ClusterVersion object, Err: %v", err)
	}

	log.Infof("Generation number before setting channel [%d]", beforeGenNumInt)
	log.Infof("Set channel to [%s]..", channel)
	patch := `
spec:
  channel: %s
`
	t := func() (interface{}, bool, error) {
		args := []string{"patch", "clusterversion", "version", "--type=merge", "--patch", fmt.Sprintf(patch, channel)}
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return nil, true, fmt.Errorf("failed to select channel [%s], Err: %v %v", channel, string(output), err)
		}
		log.Info(output)
		if err := waitForNewGenertionNumber(beforeGenNumInt); err != nil {
			return nil, true, fmt.Errorf("failed to select channel [%s], Err: %v", channel, err)
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 5*time.Second); err != nil {
		return err
	}
	return nil
}

// getCurrentChannel gets and return current channel
func getCurrentChannel() (string, error) {
	log.Info("Get current cluster channel...")
	clusterVersion, err := k8sOpenshift.GetClusterVersion("version")
	if err != nil {
		return "", fmt.Errorf("failed to get ClusterVersion object, Err: %v", err)
	}

	log.Infof("Current cluster channel is [%s]", clusterVersion.Spec.Channel)
	return clusterVersion.Spec.Channel, nil
}

// getImageSha gets image sha which will be used to install/upgrade OCP
func getImageSha(ocpVersion string) (string, error) {
	downloadURL := fmt.Sprintf("%s/%s/%s", OpenshiftMirror,
		ocpVersion, releaseFileName)
	request := netutil.HttpRequest{
		Method:   "GET",
		Url:      downloadURL,
		Content:  "application/json",
		Body:     nil,
		Insecure: true,
	}

	log.Debugf("Getting image sha for OCP [%s] from URL [%s]", ocpVersion, downloadURL)
	content, err := netutil.DoRequest(request)
	if err != nil {
		return "", fmt.Errorf("Failed to get Get content from [%s], Err: %v", downloadURL, err)
	}

	// Convert the body to type string
	contentInString := string(content)
	parts := strings.Split(contentInString, "\n")
	for _, a := range parts {
		if strings.Contains(a, "Digest:") {
			return strings.TrimSpace(strings.Split(a, ": ")[1]), nil
		}
	}
	return "", fmt.Errorf("failed to find image sha from [%s]", downloadURL)
}

func startUpgrade(upgradeVersion string) error {
	var output []byte
	var shaName string
	args := []string{"adm", "upgrade", fmt.Sprintf("--to=%s", upgradeVersion)}

	t := func() (interface{}, bool, error) {
		output, stdErr, err := osutils.ExecTorpedoShell("oc", args...)
		if err != nil {
			forceUpgrade := "specify --to-image"
			notRecommended := "is not one of the recommended updates, but is available"
			if strings.Contains(string(stdErr), notRecommended) {
				args = []string{"adm", "upgrade", fmt.Sprintf("--to=%s", upgradeVersion), "--allow-not-recommended"}
				log.Infof("Retrying upgrade with --allow-not-recommended option")
				output, stdErr, err = osutils.ExecTorpedoShell("oc", args...)
				if err != nil {
					return output, true, fmt.Errorf("failed to start upgrade, Err: %s %v", stdErr, err)
				}
				log.Infof(output)
				log.Debugf(stdErr)
			} else if strings.Contains(string(stdErr), forceUpgrade) {
				log.Infof("Retrying upgrade with --force option")
				if shaName, err = getImageSha(upgradeVersion); err != nil {
					return "", false, err
				}
				imagePath := fmt.Sprintf("--to-image=quay.io/openshift-release-dev/ocp-release@%s", shaName)
				log.Infof("Image full path : %s", imagePath)
				args = []string{"adm", "upgrade", imagePath, "--force", "--allow-explicit-upgrade", "--allow-upgrade-with-warnings"}
				output, stdErr, err = osutils.ExecTorpedoShell("oc", args...)
				if err != nil {
					return output, true, fmt.Errorf("failed to start upgrade, Err: %s %v", stdErr, err)
				}
				log.Infof(output)
				log.Warnf(stdErr)
			} else {
				return output, true, fmt.Errorf("failed to start upgrade, Err: %v %v", stdErr, err)
			}
		}
		log.Debugf("Upgrade command output: %s", output)
		return output, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, defaultCmdRetry, defaultCmdRetry); err != nil {
		return err
	}

	t = func() (interface{}, bool, error) {
		clusterVersion, err := k8sOpenshift.GetClusterVersion("version")
		if err != nil {
			return nil, true, fmt.Errorf("failed to get cluster version. cause: %v", err)
		}

		desiredVersion := clusterVersion.Status.Desired.Version
		if desiredVersion != upgradeVersion {
			return nil, true, fmt.Errorf("version mismatch. expected: %s but got %s", upgradeVersion, desiredVersion)
		}
		log.Infof("Upgrade started: %s", output)
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, defaultCmdTimeout, defaultCmdRetry); err != nil {
		return err
	}
	return nil
}

func waitUpgradeCompletion(upgradeVersion string) error {
	t := func() (interface{}, bool, error) {
		clusterVersion, err := k8sOpenshift.GetClusterVersion("version")
		if err != nil {
			return nil, true, fmt.Errorf("failed to get cluster version. cause: %v", err)
		}

		for _, status := range clusterVersion.Status.Conditions {
			if status.Type == openshiftv1.OperatorProgressing && status.Status == openshiftv1.ConditionTrue {
				return nil, true, fmt.Errorf("cluster not upgraded yet. cause: %s", status.Message)
			} else if status.Type == openshiftv1.OperatorProgressing && status.Status == openshiftv1.ConditionFalse {
				break
			}
		}

		for _, history := range clusterVersion.Status.History {
			if history.Version == upgradeVersion && history.State != openshiftv1.CompletedUpdate {
				return nil, true, fmt.Errorf("cluster not upgraded yet. expected: %v got: %v", openshiftv1.CompletedUpdate, history.State)
			} else if history.Version == upgradeVersion && history.State == openshiftv1.CompletedUpdate {
				break
			}
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, defaultUpgradeTimeout, defaultUpgradeRetryInterval); err != nil {
		return err
	}
	return nil
}

// waitNodesToBeReady waits for all nodes to become Ready and using the same k8s version
func waitNodesToBeReady() error {
	t := func() (interface{}, bool, error) {
		var count int
		var k8sVersions = make(map[string]string)
		var versionSet = make(map[string]bool)

		nodeList, err := k8sCore.GetNodes()
		if err != nil {
			return nil, true, fmt.Errorf("failed to get nodes, Err:%v", err)
		}

		for _, k8sNode := range nodeList.Items {
			for _, status := range k8sNode.Status.Conditions {
				if status.Type == corev1.NodeReady && status.Status == corev1.ConditionTrue {
					count++
					kubeletVersion := k8sNode.Status.NodeInfo.KubeletVersion
					k8sVersions[k8sNode.Name] = kubeletVersion
					versionSet[kubeletVersion] = true
					break
				}
			}
		}

		totalNodes := len(nodeList.Items)
		if count < totalNodes {
			return nil, true, fmt.Errorf("nodes not ready, expected [%d] actual [%d]", totalNodes, count)
		}

		if len(versionSet) > 1 {
			return nil, true, fmt.Errorf("nodes are not the same version:\n%v", k8sVersions)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 30*time.Minute, 15*time.Second); err != nil {
		return err
	}
	return nil
}

// getChannel returns a channel string based on an OCP version format
func getChannel(ocpVer string) (string, error) {
	log.Infof("Get channel for OCP version [%s]...", ocpVer)
	if versionReg.MatchString(ocpVer) {
		return ocpVer, nil
	}

	versionSplit := strings.Split(ocpVer, "-")
	channel := "stable" // Default channel is stable
	var ocpVersion string
	if len(versionSplit) > 1 {
		channel = versionSplit[0]
		ocpVersion = versionSplit[1]
	}

	ver, err := version.NewVersion(ocpVersion)
	if err != nil {
		return "", fmt.Errorf("failed to parse version [%s], Err: %v", ocpVersion, err)
	}

	channels := map[string]string{
		"stable":    fmt.Sprintf("stable-%d.%d", ver.Segments()[0], ver.Segments()[1]),
		"candidate": fmt.Sprintf("candidate-%d.%d", ver.Segments()[0], ver.Segments()[1]),
		"fast":      fmt.Sprintf("fast-%d.%d", ver.Segments()[0], ver.Segments()[1]),
	}

	log.Infof("For OCP version [%s], selecting channel [%s]", ocpVer, channels[channel])
	return channels[channel], nil
}

// downloadOCP4Client Constructs URL, dowloads and prepares OC CLI to be used based on OCP version given
func downloadOCP4Client(ocpVersion string) error {
	var clientName string
	var downloadURL string
	ocBinaryDir := "/usr/local/bin"

	if ocpVersion == "" {
		ocpVersion = "latest"
	}

	log.Infof("Downloading OCP [%s] client. May take some time...", ocpVersion)
	if versionReg.MatchString(ocpVersion) {
		downloadURL = fmt.Sprintf("%s/%s/openshift-client-linux.tar.gz", OpenshiftMirror, ocpVersion)
		clientName = "openshift-client-linux.tar.gz"
	} else {
		downloadURL = fmt.Sprintf("%s/%s/openshift-client-linux-%s.tar.gz", OpenshiftMirror, ocpVersion, ocpVersion)
		clientName = fmt.Sprintf("openshift-client-linux-%s.tar.gz", ocpVersion)
	}
	if clientName == "" && downloadURL == "" {
		return fmt.Errorf("failed to construct URL [%s] and/or client package name [%s] for OCP [%s]", downloadURL, clientName, ocpVersion)
	}

	log.Infof("Downloading OCP [%s] client from URL [%s] to [%s]...", ocpVersion, downloadURL, clientName)
	stdout, err := exec.Command("curl", "-o", clientName, downloadURL).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to download OpenShift [%s] client from [%s], Err %v %v", clientName, downloadURL, stdout, err)
	}
	log.Infof("Openshift [%s] client successfully downloaded from [%s] and saved as [%s]", ocpVersion, downloadURL, clientName)

	log.Infof("Executing command [tar -xvf %s]...", clientName)
	stdout, err = exec.Command("tar", "-xvf", clientName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to extract [%s], Err: %v %v", clientName, err, string(stdout))
	}
	log.Infof("Successfully extracted [%s]", clientName)

	log.Infof("Executing command [cp ./oc %s]...", ocBinaryDir)
	stdout, err = exec.Command("cp", "./oc", ocBinaryDir).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to copy oc binary to [%s], Err %v %v", ocBinaryDir, err, string(stdout))
	}
	log.Infof("Successfully copied oc binary to [%s]", ocBinaryDir)

	log.Info("Executing command [oc version]...")
	stdout, err = exec.Command("oc", "version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get oc version, Err: %v %v", err, string(stdout))
	}
	log.Infof("Successfully got OCP version:\n%v\n", string(stdout))
	return nil
}

// ackAPIRemoval provides an acknowledgment before the cluster can be upgraded to specific versions
func ackAPIRemoval(ocpVer string) error {
	log.Info("Checking if we need to provide acknowledgment of API removal before proceeding with OCP cluster upgrade...")
	openshiftVersion, err := getParsedVersion(ocpVer)
	if err != nil {
		return err
	}

	// This is required for OCP version 4.9, 4.12, 4.13 and 4.14 upgrade hops
	var patchData string
	if openshiftVersion.GreaterThanOrEqual(openshiftVersion_4_9) && openshiftVersion.LessThan(openshiftVersion_4_12) {
		patchData = "{\"data\":{\"ack-4.8-kube-1.22-api-removals-in-4.9\":\"true\"}}"
	} else if openshiftVersion.GreaterThanOrEqual(openshiftVersion_4_12) && openshiftVersion.LessThan(openshiftVersion_4_13) {
		patchData = "{\"data\":{\"ack-4.11-kube-1.25-api-removals-in-4.12\":\"true\"}}"
	} else if openshiftVersion.GreaterThanOrEqual(openshiftVersion_4_13) && openshiftVersion.LessThan(openshiftVersion_4_14) {
		patchData = "{\"data\":{\"ack-4.12-kube-1.26-api-removals-in-4.13\":\"true\"}}"
	} else if openshiftVersion.GreaterThanOrEqual(openshiftVersion_4_14) {
		patchData = "{\"data\":{\"ack-4.13-kube-1.27-api-removals-in-4.14\":\"true\"}}"
	} else {
		log.Infof("Providing acknowledgment of API removal is not needed when upgrading to OCP version [%s]", openshiftVersion.String())
		return nil
	}

	t := func() (interface{}, bool, error) {
		var output []byte
		args := []string{"-n", "openshift-config", "patch", "cm", "admin-acks", "--type=merge", "--patch", patchData}
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return nil, true, fmt.Errorf("failed to provide acknowledgment of API removal, Err: %v %v", string(output), err)
		}
		log.Info(string(output))

		log.Infof("Successfully provided acknowledgment of API removal before proceeding to upgrade to OCP version [%s]", openshiftVersion.String())
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second); err != nil {
		return err
	}
	return nil
}

// Check for newly create OCP node and retun OCP node
func (k *openshift) checkAndGetNewNode() (string, error) {
	var newNodeName string

	// Waiting for new node to be ready
	newNodeName, err := k.getAndWaitMachineToBeReady()
	if err != nil {
		// This is to handle error case when newly provisioned node not ready in 10 minutes
		// Deleting the newly provisioned node and waiting for one more time before returning error
		if len(newNodeName) != 0 {
			k.deleteAMachine(newNodeName)
		}
		// Waiting for new node to be ready
		newNodeName, err = k.getAndWaitMachineToBeReady()
		if err != nil {
			return newNodeName, err
		}
	}

	// VM is up and ready. Waiting for other services to be up and joining it to cluster.
	if err := k.waitForJoinK8sNode(newNodeName); err != nil {
		return newNodeName, err
	}

	return newNodeName, nil
}

// Waits for newly provisioned OCP node to be ready and running
func (k *openshift) getAndWaitMachineToBeReady() (string, error) {
	var err error
	var isTriedOnce bool = false
	var provState string = "Provisioned"
	var driverName = k.K8s.NodeDriverName
	log.Info("Using Node Driver: ", driverName)

	t := func() (interface{}, bool, error) {

		var output []byte
		cmd := "kubectl get machines -n openshift-machine-api"
		cmd += " --sort-by='.metadata.creationTimestamp' | tail -1"

		output, err = exec.Command("sh", "-c", cmd).CombinedOutput()
		result := strings.Fields(string(output))
		if err != nil {
			return "", true, fmt.Errorf("failed to get new OCP VM [%s] status, Err: %v", result[0], err)
		} else if strings.ToLower(result[1]) != "running" {
			// Observed that OCP unable to power-on VM sometimes for vSphere driver
			// Trying to power on the new VM once
			if result[1] == provState && driverName == vsphere.DriverName && !isTriedOnce {
				isTriedOnce = true
				driver, _ := node.Get(driverName)
				if err = driver.AddMachine(result[0]); err != nil {
					return result[0], true, err
				}
				if err = driver.PowerOnVMByName(result[0]); err != nil {
					return result[0], true, err
				}
			}
			return result[0], true, &scheduler.ErrFailedToBringUpNode{
				Node:  result[0],
				Cause: fmt.Errorf("OCP was Unable to bring up the new node"),
			}
		}
		return result[0], false, nil
	}

	output, err := task.DoRetryWithTimeout(t, 20*time.Minute, 30*time.Second)
	if err != nil {
		if output != nil {
			return output.(string), err
		}
		return "", err
	}
	nodeName := output.(string)
	log.Infof("New OCP VM [%s] is up now", nodeName)
	return nodeName, nil
}

// Wait for node to join k8s cluster
func (k *openshift) waitForJoinK8sNode(node string) error {
	t := func() (interface{}, bool, error) {
		if err := k8sCore.IsNodeReady(node); err != nil {
			return "", true, fmt.Errorf("Waiting for new node [%s] to join k8s cluster, Err: %v", node, err)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	log.Infof("New OCP VM [%s] came up successfully and joined k8s cluster", node)
	return nil
}

// deleteAMachine deletes the OCP node using kubectl command
func (k *openshift) deleteAMachine(nodeName string) error {
	// Delete the node from machineset using kubectl command
	t := func() (interface{}, bool, error) {
		log.Infof("Deleting machine [%s]", nodeName)
		cmd := "kubectl delete machines -n openshift-machine-api " + nodeName
		if _, err := exec.Command("sh", "-c", cmd).CombinedOutput(); err != nil {
			return "", true, fmt.Errorf("failed to delete machine, Err: %v", err)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 60*time.Second); err != nil {
		return err
	}
	return nil
}

// getMachinesCount deletes the OCP node using kubectl command
func (k *openshift) getMachinesCount() (int, error) {
	var output []byte
	var err error
	// Get number of ocp machines using kubectl command
	log.Infof("Getting machine count")
	cmd := "kubectl get machines -n openshift-machine-api | awk 'NR > 1 {count++} END {print count}'"
	output, err = exec.Command("sh", "-c", cmd).CombinedOutput()

	if err != nil {
		return 0, fmt.Errorf("failed to run command [%s], Err: %v", cmd, err)
	}
	result := strings.TrimSpace(string(output))
	machineCount, err := strconv.Atoi(result)
	if err != nil {
		return 0, fmt.Errorf("error converting [%s] to int, Err: %v", result, err)
	}

	return machineCount, nil
}

// Method to recycling OCP node
func (k *openshift) DeleteNode(n node.Node) error {
	// Check if node is valid before proceeding for delete a node
	var worker []node.Node = node.GetWorkerNodes()
	var delNode *api.StorageNode
	var isStoragelessNode bool = false
	if node.Contains(worker, n) {

		// Check if node is meta node and set the meta flag
		isKVDBNode := n.IsMetadataNode

		// Get node info before deleting the node
		volDriver, err := volume.Get(k.VolDriverName)
		if err != nil {
			return err
		}

		delNode, err = volDriver.GetDriverNode(&n)
		if err != nil {
			return err
		}

		// Get storageless nodes
		storagelessNodes, err := volDriver.GetStoragelessNodes()
		if err != nil {
			return err
		}

		// Checking if given node is storageless node
		if volDriver.Contains(storagelessNodes, delNode) {
			log.Infof("PX node [%s] is storageless node and pool validation is not needed", delNode.Hostname)
			isStoragelessNode = true
		}

		// Printing the drives and pools info only for a storage node
		if !isStoragelessNode {
			log.Infof("Before recyling a node, Node [%s] is having following pools:", delNode.Hostname)
			for _, pool := range delNode.Pools {
				log.Infof("Node [%s] is having pool ID: [%s]", delNode.Hostname, pool.Uuid)
			}
			log.Infof("Before recyling a node, Node [%s] is having disks: [%v]", delNode.Hostname, delNode.Disks)

			if isKVDBNode {
				log.Infof("Node [%s] is one of the KVDB node", delNode.Hostname)
			}
		}

		// Delete the node from machines using kubectl command
		log.Infof("Recycling the node [%s] having NodeID: [%s]", n.Name, delNode.Id)

		// PowerOff machine before deleting the machine for vSphere driver
		var driverName = k.K8s.NodeDriverName
		if driverName == vsphere.DriverName {
			driver, _ := node.Get(driverName)
			if err := driver.PowerOffVM(n); err != nil {
				return err
			}
			//wait for power off complete before deleting machine
			time.Sleep(5 * time.Second)
		}

		eg := errgroup.Group{}

		eg.Go(func() error {
			if err := k.deleteAMachine(n.Name); err != nil {
				return fmt.Errorf("Failed to delete OCP node [%s], Err: %v", n.Name, err)
			}
			return nil
		})

		eg.Go(func() error {
			if !isStoragelessNode && driverName == vsphere.DriverName {
				driver, err := node.Get(driverName)
				if err != nil {
					return err
				}
				if err := driver.DestroyVM(n); err != nil {
					return err
				}
			}
			return nil
		})

		if err := eg.Wait(); err != nil {
			return err
		}

		// Removing the node from the nodeRegistry
		log.Infof("Deleting node [%s] from node registry..", n.Name)
		if err := node.DeleteNode(n); err != nil {
			return &scheduler.ErrFailedToUpdateNodeList{
				Node:  n.Name,
				Cause: fmt.Sprintf("Failed to remove OCP node [%s] from node list, Err: %v", n.Name, err),
			}
		}
		log.Infof("Successfully deleted the OCP node [%s]", n.Name)

		// OCP creates a new node once the desired number of worker node count goes down
		// Wait for OCP to provision new node and update new node to the k8s node list
		newOCPNode, err := k.checkAndGetNewNode()
		if err != nil {
			return &scheduler.ErrFailedToGetNode{
				Cause: fmt.Sprintf("failed to get newly created OCP node name. Err: %v", err),
			}
		}

		// Getting k8s node
		newNode, err := k8sCore.GetNodeByName(newOCPNode)
		if err != nil {
			return err
		}

		//Adding a new node to a nodeRegistry
		if err := k.AddNewNode(*newNode); err != nil {
			return &scheduler.ErrFailedToUpdateNodeList{
				Node:  newOCPNode,
				Cause: fmt.Sprintf("failed to update new OCP node [%s] in node list, Err: %v", newOCPNode, err),
			}
		}

		// Getting the node object for a new node
		newlyProvNode, err := node.GetNodeByName(newOCPNode)
		if err != nil {
			return err
		}

		// Waits for px pod to be up in new node
		if err := volDriver.WaitForPxPodsToBeUp(newlyProvNode); err != nil {
			return err
		}

		// Validation is needed only when deleted node was StorageNode
		if err := k.validateDrivesAfterNewNodePickUptheID(delNode, volDriver, storagelessNodes, isStoragelessNode); err != nil {
			return err
		}

		// Update the new node object with storage information
		if err := volDriver.UpdateNodeWithStorageInfo(newlyProvNode, n.Name); err != nil {
			return err
		}
		log.Infof("Successfully updated the storage info for new node: [%s] ", newlyProvNode.Name)

		// Getting the new node object after storage info updated
		newlyProvNode, err = node.GetNodeByName(newlyProvNode.Name)
		if err != nil {
			return err
		}

		// Waiting and make sure driver to come up successfuly on newly provisoned node
		log.Infof("Waiting for driver to be come up on node [%s]", newlyProvNode.Name)
		if err := volDriver.WaitDriverUpOnNode(newlyProvNode, driverUpTimeout); err != nil {
			return err
		}
		log.Infof("Driver came up successfully on node [%s]", newlyProvNode.Name)
		return nil

	}
	return fmt.Errorf("Node is not a worker node")
}

func (k *openshift) validateDrivesAfterNewNodePickUptheID(delNode *api.StorageNode,
	volDriver volume.Driver, storagelessNodes []*api.StorageNode, isStoragelessNode bool) error {

	log.Infof("Validating the pools and drives on new node")
	// Validation is needed only when deleted node was StorageNode
	if !isStoragelessNode {
		// Wait for new node to pick up the deleted node ID
		log.Infof("Waiting for NodeID [%s] to be picked by another node ", delNode.Id)
		newPXNode, err := volDriver.WaitForNodeIDToBePickedByAnotherNode(delNode)
		if err != nil {
			return err
		}
		log.Infof("NodeID [%s] pick up by another node: [%s]", delNode.Id, newPXNode.Hostname)
		log.Infof("Validating the node: [%s] after it picked the NodeID: [%s] ",
			newPXNode.Hostname, delNode.Id,
		)

		err = volDriver.ValidateNodeAfterPickingUpNodeID(delNode, newPXNode, storagelessNodes)
		if err != nil {
			return err
		}
		log.Infof("Successfully validated the pools and drives on new node")
		return nil
	}
	log.Infof("Skipping the pool and drives validation for storageless node: [%s]", delNode.Id)
	return nil
}

func getParsedVersion(ocpVer string) (*version.Version, error) {
	if versionReg.MatchString(ocpVer) {
		cli := &http.Client{}
		url := fmt.Sprintf("https://mirror.openshift.com/pub/openshift-v4/clients/ocp/%s/release.txt", ocpVer)
		log.Infof("OCP version [%s], will check [%s] for an actual full OCP version...", ocpVer, url)
		resp, err := cli.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		output, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		var re = regexp.MustCompile(`(?m)Name:\s+([\d.]+)`)
		match := re.FindStringSubmatch(string(output))
		if len(match) > 1 {
			ocpVer = match[1]
		}
	}

	parsedVersion, err := version.NewVersion(ocpVer)
	if err != nil {
		return nil, err
	}
	log.Infof("OCP actual full version is [%s]", parsedVersion.String())
	return parsedVersion, nil
}

func getMachineSetName() (string, error) {
	cmd := fmt.Sprintf("kubectl get machineset -n %s -o name", OpenshiftMachineNamespace)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return "", err
	}
	result := strings.TrimSpace(string(output))

	return result, nil

}

// needOcpPrereq checks if we need to do OCP Prometheus prereq and returns true or false
func (k *openshift) needOcpPrereq(ocpVer string) (bool, error) {
	log.Infof("Checking if OCP version  [%s] requires Prometheus configuration changes...", ocpVer)

	openshiftVersion, err := getParsedVersion(ocpVer)
	if err != nil {
		return false, fmt.Errorf("failed to parse OCP version [%s], Err: %v", ocpVer, err)
	}
	log.Infof("This Openshift cluster version is [%s]", openshiftVersion.String())

	// Get PX Operator version
	opVersion, err := optest.GetPxOperatorVersion()
	if err != nil {
		return false, fmt.Errorf("failed to get PX Operator version, Err: %v", err)
	}
	log.Infof("PX Operator version is [%s]", opVersion.String())

	// Check PX Operator version compatibility
	minPxOperatorVersionOcp_4_14, _ := version.NewVersion("23.10.3-") // PX Operator version that only supports OCP 4.14+
	minPxOperatorVersionOcpAll, _ := version.NewVersion("23.10.4-")   // PX Operator version that supports all OCP versions

	if !opVersion.GreaterThanOrEqual(minPxOperatorVersionOcpAll) {
		if !openshiftVersion.GreaterThanOrEqual(openshiftVersion_4_14) && !opVersion.GreaterThanOrEqual(minPxOperatorVersionOcp_4_14) {
			log.Warnf("Openshift version [%s] and PX Operator version [%s] are not compatible for integrating with Openshift Prometheus..", openshiftVersion.String(), opVersion.String())
			return false, nil
		}
	}
	log.Info("Openshift cluster version [%s] and PX Operator version [%s] are compatible, will prepare cluster to integrate with Openshift Prometheus..", openshiftVersion.String(), opVersion.String())

	return true, nil
}

// setOcpPrometheusPrereq perform required steps for PX to work with OCP Prometheus
func (k *openshift) setOcpPrometheusPrereq(version string) error {
	// Check if we need to perform this Prometheus prereq
	doPrereq, err := k.needOcpPrereq(version)
	if err != nil {
		return fmt.Errorf("failed to determine if we need to do OCP prereq or not, Err: %v", err)
	}

	if doPrereq {
		// Perform Prometheus prereq
		if err := updatePrometheusAndAutopilot(); err != nil {
			return err
		}
	}
	return nil
}

func configureClusterMonitoringConfig() error {
	log.Info("Configure Cluster Monitoring for OCP Prometheus...")
	// Openshift Cluster Moniting ConfigMap
	ocpConfigmap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-monitoring-config",
			Namespace: "openshift-monitoring",
		},
		Data: map[string]string{
			"config.yaml": "enableUserWorkload: true",
		},
	}

	// Try to get Openshift Cluster Monitoring ConfigMap and based on the results either create or modify it
	existingConfigMap, err := k8sCore.GetConfigMap(ocpConfigmap.Name, ocpConfigmap.Namespace)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Infof("Creating ConfigMap [%s] in namespace [%s]...", ocpConfigmap.Name, ocpConfigmap.Namespace)
			if _, err := k8sCore.CreateConfigMap(ocpConfigmap); err != nil {
				return fmt.Errorf("failed to create ConfigMap [%s] in namesapce [%s], Err: %v", ocpConfigmap.Name, ocpConfigmap.Namespace, err)
			}
			log.Infof("Successfully created ConfigMap [%s] in namespace [%s]", ocpConfigmap.Name, ocpConfigmap.Namespace)
		} else {
			return fmt.Errorf("failed to get ConfigMap [%s] in namespace [%s], Err: %v", ocpConfigmap.Name, ocpConfigmap.Namespace, err)
		}
	} else {
		log.Infof("ConfigMap [%s] already exists in [%s] namespace, will check if enableUserWorkload is true...", ocpConfigmap.Name, ocpConfigmap.Namespace)
		existingData := existingConfigMap.Data["config.yaml"]
		if strings.Contains(existingData, "enableUserWorkload: true") {
			log.Infof("ConfigMap [%s] already contains enableUserWorkload: true in [%s] namespace, skipping update", ocpConfigmap.Name, ocpConfigmap.Namespace)
		} else {
			log.Infof("Adding enableUserWorkload: true to ConfigMap [%s] in [%s] namespace...", ocpConfigmap.Name, ocpConfigmap.Namespace)
			newData := existingData + "enableUserWorkload: true\n"
			ocpConfigmap.Data["config.yaml"] = newData
			if _, err = k8sCore.UpdateConfigMap(ocpConfigmap); err != nil {
				return fmt.Errorf("failed to update ConfigMap [%s] in namespace [%s], Err: %v", ocpConfigmap.Name, ocpConfigmap.Namespace, err)
			}
			log.Infof("Successfully updated ConfigMap [%s] in namespace [%s]", ocpConfigmap.Name, ocpConfigmap.Namespace)
		}
	}
	log.Info("Successfully configured Cluster Monitoring for OCP Prometheus")
	return nil
}

func updatePrometheusAndAutopilot() error {
	// Get StorageCluster object
	log.Info("Looking for PX StorageCluster...")
	stcList, err := pxOperator.ListStorageClusters("")
	if err != nil {
		return fmt.Errorf("failed to get list of StorageCluster objects, Err: %v", err)
	}
	if len(stcList.Items) != 1 {
		return fmt.Errorf("invalid number of StorageCluster objects found, expected [1] got [%d]", len(stcList.Items))
	}
	stc := &stcList.Items[0]
	log.Infof("Successfully found PX StorageCluster [%s] in [%s] namespace", stc.Name, stc.Namespace)

	// Create or modify cluster monitoring ConfigMap
	if err = configureClusterMonitoringConfig(); err != nil {
		return err
	}

	log.Info("Looking for Thanos Querier Host...")
	thanosQuerierHostCmd := `kubectl get route thanos-querier -n openshift-monitoring -o json | jq -r '.spec.host'`
	var output []byte

	output, err = exec.Command("sh", "-c", thanosQuerierHostCmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get thanos querier host , Err: %v", err)
	}
	thanosQuerierHost := strings.TrimSpace(string(output))
	log.Infof("Thanos Querier Host is [%s]", thanosQuerierHost)
	thanosQuerierHostUrl := fmt.Sprintf("https://%s", thanosQuerierHost)

	if stc.Spec.Monitoring.Prometheus.Enabled {
		log.Debug("PX Prometheus is enabled, will disable it in the StorageCluster spec...")
		stc.Spec.Monitoring.Prometheus.Enabled = false
	}

	// Add Thanos Querir to Autopilot spec
	log.Info("Adding Thanos Querier Host to the Autopilot spec...")
	var dataProviders []opv1.DataProviderSpec
	if stc.Spec.Autopilot.Enabled {
		dataProviders = stc.Spec.Autopilot.Providers
		for _, dataProvider := range dataProviders {
			if dataProvider.Type == "prometheus" {
				isUrlUpdated := false
				if val, ok := dataProvider.Params["url"]; ok {
					if val == thanosQuerierHostUrl {
						isUrlUpdated = true
					}
				}
				if !isUrlUpdated {
					dataProvider.Params["url"] = thanosQuerierHostUrl
				}
			}
		}
	}
	log.Info("Successfully added Thanos Host to the Autopilot spec")

	// Update PX StorageCluster with the changes
	log.Infof("Updating PX StorageCluster [%s] in [%s] namespace with required changes to work with OCP Prometheus...")
	if _, err := pxOperator.UpdateStorageCluster(stc); err != nil {
		return fmt.Errorf("failed to update StorageCluster [%s] in [%s] namespace, Err: %v", stc.Name, stc.Namespace, err)
	}
	log.Infof("Successfully updated PX StorageCluster [%s] in [%s] namespace with required changes to work with OCP Prometheus...", stc.Name, stc.Namespace)
	return nil
}

func (k *openshift) SetASGClusterSize(replicas int64, timeout time.Duration) error {
	initialMachineCount, err := k.getMachinesCount()
	if err != nil {
		return err
	}

	machineSetName, err := getMachineSetName()
	if err != nil {
		return err
	}
	// kubectl scale machineset leela-ocp-vx6zf-worker-0 --replicas 6 -n openshift-machine-api
	cmd := fmt.Sprintf("kubectl -n %s scale %s --replicas %d", OpenshiftMachineNamespace, machineSetName, replicas)
	log.Infof("Executing command [%s]", cmd)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return err
	}
	log.Infof("output: %s", string(output))

	if !strings.Contains(string(output), fmt.Sprintf("%s scaled", machineSetName)) {
		return fmt.Errorf("failed to scale [%s], output: %s", machineSetName, string(output))
	}

	log.Infof("waiting for new machine to be created")

	t := func() (interface{}, bool, error) {
		newMachineCount, err := k.getMachinesCount()
		if err != nil {
			return "", true, fmt.Errorf("error getting machine count, Err: %v", err)
		}
		if newMachineCount <= initialMachineCount {
			return "", true, fmt.Errorf("waiting for new machine to provision initial count : [%d], current count: [%d]", initialMachineCount, newMachineCount)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}

	if _, err := k.checkAndGetNewNode(); err != nil {
		return err
	}

	return nil
}

func (k *openshift) GetASGClusterSize() (int64, error) {
	machineCount, err := k.getMachinesCount()
	if err != nil {
		return 0, err
	}

	return int64(machineCount), nil
}

func (k *openshift) GetZones() ([]string, error) {
	//OCP has one zone
	return []string{"zone-1"}, nil
}

func (k *openshift) Init(schedOpts scheduler.InitOptions) error {

	err := k.K8s.Init(schedOpts)
	if err != nil {
		return err
	}

	return nil
}
