package openshift

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/blang/semver"
	openshiftv1 "github.com/openshift/api/config/v1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	k8s "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalsnapshotter"
	opnshift "github.com/portworx/sched-ops/k8s/openshift"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "openshift"
	// SystemdSchedServiceName is the name of the system service responsible for scheduling
	SystemdSchedServiceName = "atomic-openshift-node"
	// OpenshiftMirror is the mirror we use do download ocp client
	OpenshiftMirror   = "https://mirror.openshift.com/pub/openshift-v4/clients/ocp"
	defaultCmdTimeout = 5 * time.Minute
	defaultCmdRetry   = 15 * time.Second
)

var (
	k8sOpenshift       = opnshift.Instance()
	k8sCore            = k8s.Instance()
	crdOps             = apiextensions.Instance()
	snapshoterOps      = externalsnapshotter.Instance()
	versionReg         = regexp.MustCompile(`^(stable|candidate|fast)(-\d\.\d)?$`)
	volumeSnapshotCRDs = []string{
		"volumesnapshotclasses.snapshot.storage.k8s.io",
		"volumesnapshotcontents.snapshot.storage.k8s.io",
		"volumesnapshots.snapshot.storage.k8s.io",
	}
)

type openshift struct {
	kube.K8s
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
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
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
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
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

func (k *openshift) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(k.K8s.NodeDriverName)

	usableServiceName := SystemdSchedServiceName
	if serviceName, err := k.getServiceName(driver, n); err == nil {
		usableServiceName = serviceName
	} else {
		return err
	}

	cmd := fmt.Sprintf("journalctl -lu %s* > %s/kubelet.log", usableServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

func (k *openshift) updateSecurityContextConstraints(namespace string) error {
	// Get privileged context
	context, err := k8sOpenshift.GetSecurityContextConstraints("privileged")
	if err != nil {
		return err
	}

	// Add user and namespace to context
	context.Users = append(context.Users, "system:serviceaccount:"+namespace+":default")

	// Update context
	_, err = k8sOpenshift.UpdateSecurityContextConstraints(context)
	if err != nil {
		return err
	}

	return nil
}

func (k *openshift) UpgradeScheduler(version string) error {
	var err error

	if err = downloadOCP4Client(version); err != nil {
		return err
	}

	clientVersion := ""
	if clientVersion, err = getClientVersion(); err != nil {
		return err
	}

	upgradeVersion := version
	if versionReg.MatchString(version) {
		upgradeVersion = clientVersion
	}

	if err := selectChannel(version); err != nil {
		return err
	}

	if err := fixOCPClusterStorageOperator(upgradeVersion); err != nil {
		return err
	}

	if err := startUpgrade(upgradeVersion); err != nil {
		return err
	}

	if err := waitUpgradeCompletion(clientVersion); err != nil {
		return err
	}

	logrus.Info("Waiting for all the nodes to become ready...")
	if err := waitNodesToBeReady(); err != nil {
		return err
	}
	logrus.Info(getCluterInfo())

	logrus.Infof("Cluster is now %s", upgradeVersion)
	return nil
}

func getCluterInfo() string {
	var output interface{}
	var err error

	t := func() (interface{}, bool, error) {
		nodeList, err := k8sCore.GetNodes()
		if err != nil {
			return "", true, fmt.Errorf("failed to get nodes. cause: %v", err)
		}
		if len(nodeList.Items) > 0 {
			firstNodeInfo := nodeList.Items[0].Status.NodeInfo
			info := fmt.Sprintf(
				"K8s version: %s\nOS: %s\nKernel: %s\nContainer Runtime: %s\n", firstNodeInfo.KubeletVersion,
				firstNodeInfo.OSImage, firstNodeInfo.KernelVersion, firstNodeInfo.ContainerRuntimeVersion)
			return info, false, nil
		}
		return "", false, nil
	}
	if output, err = task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second); err != nil {
		logrus.Errorf("Failed to get cluster info %v", err)
		return ""
	}
	return output.(string)
}

func getClientVersion() (string, error) {
	var err error
	var output interface{}

	t := func() (interface{}, bool, error) {
		var output []byte
		cmd := "oc version --client -o json|jq -r .releaseClientVersion"
		if output, err = exec.Command("sh", "-c", cmd).CombinedOutput(); err != nil {
			return "", true, fmt.Errorf("failed to get client version. cause: %v", err)
		}
		clientVersion := strings.TrimSpace(string(output))
		clientVersion = strings.Trim(clientVersion, "\"")
		clientVersion = strings.Trim(clientVersion, "'")
		return clientVersion, false, nil
	}
	if output, err = task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second); err != nil {
		return "", err
	}
	return output.(string), nil
}

func selectChannel(version string) error {
	var output []byte
	var err error

	channel := ""
	if channel, err = getChannel(version); err != nil {
		return err
	}
	logrus.Infof("Selected channel: %s", channel)

	patch := `
spec:
  channel: %s
`
	t := func() (interface{}, bool, error) {
		args := []string{"patch", "clusterversion", "version", "--type=merge", "--patch", fmt.Sprintf(patch, channel)}
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return nil, true, fmt.Errorf("failed to select channel due to %s. cause: %v", string(output), err)
		}
		logrus.Info(string(output))
		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second)
	return err
}

func startUpgrade(upgradeVersion string) error {
	var output []byte
	var err error

	args := []string{"adm", "upgrade", fmt.Sprintf("--to=%s", upgradeVersion)}
	t := func() (interface{}, bool, error) {
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return output, true, fmt.Errorf("failed to start upgrade due to %s. cause: %v", string(output), err)
		}
		return output, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, defaultCmdTimeout, defaultCmdRetry); err != nil {
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
		logrus.Infof("Upgrade started: %s", output)

		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(t, defaultCmdTimeout, defaultCmdRetry)
	return err
}

func waitUpgradeCompletion(upgradeVersion string) error {
	var err error

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

	_, err = task.DoRetryWithTimeout(t, 2*time.Hour, 15*time.Second)
	return err
}

// waitNodesToBeReady waits for all nodes to become Ready and using the same k8s version
func waitNodesToBeReady() error {
	var err error

	t := func() (interface{}, bool, error) {
		var count int
		var k8sVersions = make(map[string]string)
		var versionSet = make(map[string]bool)

		nodeList, err := k8sCore.GetNodes()
		if err != nil {
			return nil, true, fmt.Errorf("failed to get nodes. cause: %v", err)
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
			return nil, true, fmt.Errorf("nodes not ready. expected %d but got %d", totalNodes, count)
		}

		if len(versionSet) > 1 {
			return nil, true, fmt.Errorf("nodes are not in the same version.\n%v", k8sVersions)
		}
		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 30*time.Minute, 15*time.Second)
	return err
}

func getChannel(version string) (string, error) {
	if versionReg.MatchString(version) {
		return version, nil
	}

	versionSplit := strings.Split(version, "-")
	channel := "stable"
	if len(versionSplit) > 1 {
		channel = versionSplit[0]
		version = versionSplit[1]
	}

	ver, err := semver.Make(version)
	if err != nil {
		return "", fmt.Errorf("failed to parse version: %s. cause: %v", version, err)
	}

	channels := map[string]string{
		"stable":    fmt.Sprintf("stable-%d.%d", ver.Major, ver.Minor),
		"candidate": fmt.Sprintf("candidate-%d.%d", ver.Major, ver.Minor),
		"fast":      fmt.Sprintf("fast-%d.%d", ver.Major, ver.Minor),
	}

	return channels[channel], err
}

func downloadOCP4Client(ocpVersion string) error {
	var clientName = ""
	var downloadURL = ""
	var output []byte

	if ocpVersion == "" {
		ocpVersion = "latest"
	}

	logrus.Info("Downloading OCP 4.X client. May take some time...")
	if versionReg.MatchString(ocpVersion) {
		downloadURL = fmt.Sprintf("%s/%s/openshift-client-linux.tar.gz", OpenshiftMirror,
			ocpVersion)
		clientName = "openshift-client-linux.tar.gz"
	} else {
		downloadURL = fmt.Sprintf("%s/%s/openshift-client-linux-%s.tar.gz", OpenshiftMirror,
			ocpVersion, ocpVersion)
		clientName = fmt.Sprintf("openshift-client-linux-%s.tar.gz", ocpVersion)
	}

	stdout, err := exec.Command("curl", "-o", clientName, downloadURL).CombinedOutput()
	if err != nil {
		logrus.Errorf("Error while downloading OpenShift 4.X client from %s, error %v", downloadURL, err)
		logrus.Error(string(stdout))
		return err
	}

	logrus.Infof("Openshift client %s downloaded successfully.", clientName)

	stdout, err = exec.Command("tar", "-xvf", clientName).CombinedOutput()
	if err != nil {
		logrus.Errorf("Error extracting %s, error %v", clientName, err)
		logrus.Error(string(stdout))
		return err
	}

	logrus.Infof("Extracted %s successfully.", clientName)

	stdout, err = exec.Command("cp", "./oc", "/usr/local/bin").CombinedOutput()
	if err != nil {
		logrus.Errorf("Error copying %s, error %v", clientName, err)
		logrus.Error(string(stdout))
		return err
	}

	if output, err = exec.Command("oc", "version").CombinedOutput(); err != nil {
		logrus.Errorf("Error getting oc version, error %v", err)
		logrus.Error(string(stdout))
		return err
	}
	logrus.Info(string(output))
	return nil
}

// workaround for https://portworx.atlassian.net/browse/PWX-20465
func fixOCPClusterStorageOperator(version string) error {

	if versionReg.MatchString(version) {
		url := fmt.Sprintf("https://mirror.openshift.com/pub/openshift-v4/clients/ocp/%s/release.txt", version)
		command := []string{"-c", fmt.Sprintf("curl --silent -L %s | grep \"Name:\" || true", url)}
		stdout, err := exec.Command("bash", command...).CombinedOutput()
		if err != nil {
			return err
		}
		version = strings.TrimSpace(strings.ReplaceAll(string(stdout), "Name:", ""))
	}

	parsedVersion, err := semver.Parse(version)
	if err != nil {
		return err
	}

	// this issue happens on OCP 4.3.X, 4.4.15< and 4.5.3<
	parsedVersion43, _ := semver.Parse("4.3.0")
	parsedVersion4415, _ := semver.Parse("4.4.15")
	parsedVersion45, _ := semver.Parse("4.5.0")
	parsedVersion453, _ := semver.Parse("4.5.3")

	if (parsedVersion.GTE(parsedVersion43) && parsedVersion.LT(parsedVersion4415)) ||
		(parsedVersion.GTE(parsedVersion45) && parsedVersion.LT(parsedVersion453)) {

		logrus.Infof("Found version %s which uses alphav1 version of snapshot", version)
		logrus.Warn("This upgrade requires all snapshots to be deleted.")

		namespaces, err := k8sCore.ListNamespaces(nil)
		if err != nil {
			return err
		}

		logrus.Info("Deleting volume snapshots")
		for _, ns := range namespaces.Items {
			snaps, err := snapshoterOps.ListSnapshots(ns.Name)
			if k8serrors.IsNotFound(err) {
				logrus.Infof("No snapshots found for namespace %s", ns.Name)
				continue
			}
			if err != nil {
				return err
			}
			for _, snap := range snaps.Items {
				if err = snapshoterOps.DeleteSnapshot(snap.Name, snap.Namespace); err != nil {
					return err
				}
				logrus.Infof("Deleted snapshot [%s]%s", snap.Namespace, snap.Name)
			}
		}

		logrus.Info("Removing CRDs")
		for _, crd := range volumeSnapshotCRDs {
			err = crdOps.DeleteCRD(crd)
			if k8serrors.IsNotFound(err) {
				logrus.Infof("CRD %s not found", crd)
				continue
			}
			if err != nil {
				return err
			}
			logrus.Infof("Removed CRD %s", crd)
		}
	}
	return nil
}

func init() {
	k := &openshift{}
	scheduler.Register(SchedName, k)
}
