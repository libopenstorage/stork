package openshift

import (
	"fmt"
	"golang.org/x/sync/errgroup"
	"io/ioutil"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/libopenstorage/openstorage/api"
	openshiftv1 "github.com/openshift/api/config/v1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	k8s "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalsnapshotter"
	opnshift "github.com/portworx/sched-ops/k8s/openshift"
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
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
)

var (
	k8sOpenshift       = opnshift.Instance()
	k8sCore            = k8s.Instance()
	crdOps             = apiextensions.Instance()
	snapshoterOps      = externalsnapshotter.Instance()
	versionReg         = regexp.MustCompile(`^(stable|candidate|fast)(-\d\.\d+)?$`)
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
	log.Info(getCluterInfo())

	log.Infof("Cluster is now %s", upgradeVersion)
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
		log.Errorf("Failed to get cluster info %v", err)
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

func getGenerationNumber() (int, error) {
	var genNumInt int
	clusterVersionArgs := []string{"get", "clusterversion", " -o jsonpath='{.items[*].status.observedGeneration}'"}
	beforeGenNum, stdErr, err := osutils.ExecTorpedoShell("oc", clusterVersionArgs...)
	if err != nil {
		return 0, fmt.Errorf("Failed to get generation number %s. cause: %v", stdErr, err)
	}
	genNumInt, err = strconv.Atoi(beforeGenNum)
	if err != nil {
		return 0, fmt.Errorf("Failed to convert generator number from string to int : cause: %v", err)
	}
	return genNumInt, nil
}

func waitForNewGenertionNumber(currentGenNumber int) error {
	//Wait upto 10 minutes to update generation number
	var err error
	t := func() (interface{}, bool, error) {
		newGenNumInt, err := getGenerationNumber()
		if err != nil {
			return nil, true, fmt.Errorf("Failed to convert generator number from string to int : cause: %v", err)
		}
		if newGenNumInt == currentGenNumber {
			return nil, false, fmt.Errorf("Generation number has not changed yet: %d", currentGenNumber)
		}
		log.Debugf("Set channel spec has been updated: Generation number %d", newGenNumInt)
		return nil, true, nil
	}
	_, err = task.DoRetryWithTimeout(t, generationNumberWaitTime, 5*time.Second)
	return err
}

func selectChannel(version string) error {
	var output []byte
	var err error
	channel := ""
	if channel, err = getChannel(version); err != nil {
		return err
	}
	beforeGenNumInt, err := getGenerationNumber()
	if err != nil {
		return fmt.Errorf("Failed to convert generator number from string to int : cause: %v", err)
	}
	log.Infof("Generation number before select channel: %d ", beforeGenNumInt)
	log.Infof("Selected channel: %s", channel)
	patch := `
spec:
  channel: %s
`
	t := func() (interface{}, bool, error) {
		args := []string{"patch", "clusterversion", "version", "--type=merge", "--patch", fmt.Sprintf(patch, channel)}
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return nil, true, fmt.Errorf("failed to select channel due to %s. cause: %v", string(output), err)
		}
		log.Info(output)
		if err := waitForNewGenertionNumber(beforeGenNumInt); err != nil {
			return nil, true, fmt.Errorf("Failed to select channel: cause %v", err)
		}
		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 5*time.Second)
	return err
}

// getImageSha get Image sha
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
	log.Debugf("URL %s", downloadURL)
	content, err := netutil.DoRequest(request)
	if err != nil {
		return "", fmt.Errorf("Failed to get Get content from %s, error %v", downloadURL, err)
	}
	//Convert the body to type string
	contentInString := string(content)
	parts := strings.Split(contentInString, "\n")
	for _, a := range parts {
		if strings.Contains(a, "Digest:") {
			return strings.TrimSpace(strings.Split(a, ": ")[1]), nil
		}
	}
	return "", fmt.Errorf("Failed to find Image sha: in  %s", downloadURL)
}

func startUpgrade(upgradeVersion string) error {
	var output []byte
	var err error
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
					return output, true, fmt.Errorf("failed to start upgrade due to %s, cause: %v ", stdErr, err)
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
					return output, true, fmt.Errorf("failed to start upgrade due to %s. cause: %v", stdErr, err)
				}
				log.Infof(output)
				log.Warnf(stdErr)
			} else {
				return output, true, fmt.Errorf("failed to start upgrade due to %s. cause: %v", stdErr, err)
			}
		}
		log.Debugf("Upgrade command output %s", output)
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

	_, err = task.DoRetryWithTimeout(t, defaultUpgradeTimeout, defaultUpgradeRetryInterval)
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

	log.Info("Downloading OCP 4.X client. May take some time...")
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
		log.Errorf("Error while downloading OpenShift 4.X client from %s, error %v", downloadURL, err)
		log.Error(string(stdout))
		return err
	}

	log.Infof("Openshift client %s downloaded successfully.", clientName)

	stdout, err = exec.Command("tar", "-xvf", clientName).CombinedOutput()
	if err != nil {
		log.Errorf("Error extracting %s, error %v", clientName, err)
		log.Error(string(stdout))
		return err
	}

	log.Infof("Extracted %s successfully.", clientName)

	stdout, err = exec.Command("cp", "./oc", "/usr/local/bin").CombinedOutput()
	if err != nil {
		log.Errorf("Error copying %s, error %v", clientName, err)
		log.Error(string(stdout))
		return err
	}

	if output, err = exec.Command("oc", "version").CombinedOutput(); err != nil {
		log.Errorf("Error getting oc version, error %v", err)
		log.Error(string(stdout))
		return err
	}
	log.Info(string(output))
	return nil
}

// workaround for https://portworx.atlassian.net/browse/PWX-20465
func fixOCPClusterStorageOperator(version string) error {
	parsedVersion, err := getParsedVersion(version)
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

		log.Infof("Found version %s which uses alphav1 version of snapshot", version)
		log.Warn("This upgrade requires all snapshots to be deleted.")

		namespaces, err := k8sCore.ListNamespaces(nil)
		if err != nil {
			return err
		}

		log.Info("Deleting volume snapshots")
		for _, ns := range namespaces.Items {
			snaps, err := snapshoterOps.ListSnapshots(ns.Name)
			if k8serrors.IsNotFound(err) {
				log.Infof("No snapshots found for namespace %s", ns.Name)
				continue
			}
			if err != nil {
				return err
			}
			for _, snap := range snaps.Items {
				if err = snapshoterOps.DeleteSnapshot(snap.Name, snap.Namespace); err != nil {
					return err
				}
				log.Infof("Deleted snapshot [%s]%s", snap.Namespace, snap.Name)
			}
		}

		log.Info("Removing CRDs")
		for _, crd := range volumeSnapshotCRDs {
			err = crdOps.DeleteCRD(crd)
			if k8serrors.IsNotFound(err) {
				log.Infof("CRD %s not found", crd)
				continue
			}
			if err != nil {
				return err
			}
			log.Infof("Removed CRD %s", crd)
		}
	}
	return nil
}

func ackAPIRemoval(version string) error {
	parsedVersion, err := getParsedVersion(version)
	if err != nil {
		return err
	}
	// this issue happens on OCP 4.9, 4.12 and 4.13
	parsedVersion49, _ := semver.Parse("4.9.0")
	parsedVersion412, _ := semver.Parse("4.12.0")
	parsedVersion413, _ := semver.Parse("4.13.0")
	var patchData string
	if parsedVersion.GTE(parsedVersion49) && parsedVersion.LT(parsedVersion412) {
		patchData = "{\"data\":{\"ack-4.8-kube-1.22-api-removals-in-4.9\":\"true\"}}"
	} else if parsedVersion.GTE(parsedVersion412) && parsedVersion.LT(parsedVersion413) {
		patchData = "{\"data\":{\"ack-4.11-kube-1.25-api-removals-in-4.12\":\"true\"}}"
	} else if parsedVersion.GTE(parsedVersion413) {
		patchData = "{\"data\":{\"ack-4.12-kube-1.26-api-removals-in-4.13\":\"true\"}}"
	} else {
		return nil
	}
	t := func() (interface{}, bool, error) {
		var output []byte
		args := []string{"-n", "openshift-config", "patch", "cm", "admin-acks", "--type=merge", "--patch", patchData}
		if output, err = exec.Command("oc", args...).CombinedOutput(); err != nil {
			return nil, true, fmt.Errorf("failed to ack API removal due to %s. cause: %v", string(output), err)
		}
		log.Info(string(output))
		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second)
	return err
}

// Check for newly create OCP node and retun OCP node
func (k *openshift) checkAndGetNewNode() (string, error) {
	var err error
	var newNodeName string

	// Waiting for new node to be ready
	newNodeName, err = k.getAndWaitMachineToBeReady()
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
	err = k.waitForJoinK8sNode(newNodeName)
	if err != nil {
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
			return "", true, fmt.Errorf(
				"FAILED: Unable to get new OCP VM:[%s] status. cause: %v", result[0], err,
			)
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
				Cause: fmt.Errorf("FAILED: OCP Unable to bring up the new node"),
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
	log.Infof("New OCP VM: [%s] is up now", nodeName)
	return nodeName, nil
}

// Wait for node to join k8s cluster
func (k *openshift) waitForJoinK8sNode(node string) error {
	t := func() (interface{}, bool, error) {
		if err := k8sCore.IsNodeReady(node); err != nil {
			return "", true, fmt.Errorf(
				"FAILED: Waiting for new node:[%s] to join k8s cluster. cause: %v", node, err,
			)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	log.Infof("New OCP VM: [%s] came up successfully and joined k8s cluster", node)
	return nil
}

// Delete the OCP node using kubectl command
func (k *openshift) deleteAMachine(nodeName string) error {
	var err error

	// Delete the node from machineset using kubectl command
	t := func() (interface{}, bool, error) {
		log.Infof("Deleting machine %s", nodeName)
		cmd := "kubectl delete machines -n openshift-machine-api " + nodeName
		if _, err = exec.Command("sh", "-c", cmd).CombinedOutput(); err != nil {
			return "", true, fmt.Errorf("failed to delete machine. cause: %v", err)
		}
		return "", false, nil
	}
	if _, err = task.DoRetryWithTimeout(t, 2*time.Minute, 60*time.Second); err != nil {
		return err
	}

	return nil
}

// Method to recycling OCP node
func (k *openshift) RecycleNode(n node.Node) error {
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

		if delNode, err = volDriver.GetDriverNode(&n); err != nil {
			return err
		}

		// Get storageless nodes
		storagelessNodes, err := volDriver.GetStoragelessNodes()
		if err != nil {
			return err
		}

		// Checking if given node is storageless node
		if volDriver.Contains(storagelessNodes, delNode) {
			log.Infof(
				"PX node [%s] is storageless node and pool validation is not needed",
				delNode.Hostname,
			)
			isStoragelessNode = true
		}

		// Printing the drives and pools info only for a storage node
		if !isStoragelessNode {
			log.Infof("Before recyling a node, Node [%s] is having following pools:",
				delNode.Hostname)
			for _, pool := range delNode.Pools {
				log.Infof("Node [%s] is having pool ID: [%s]", delNode.Hostname, pool.Uuid)
			}
			log.Infof("Before recyling a node, Node [%s] is having disks: [%v]",
				delNode.Hostname, delNode.Disks)

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
			err = driver.PowerOffVM(n)
			if err != nil {
				return err
			}
			//wait for power off complete before deleting machine
			time.Sleep(5 * time.Second)
		}

		eg := errgroup.Group{}

		eg.Go(func() error {
			delErr := k.deleteAMachine(n.Name)
			if delErr != nil {
				log.Errorf("Failed to delete OCP node: [%s] due to err: [%v]", n.Name, delErr)
			}
			return delErr
		})

		eg.Go(func() error {
			var destroyErr error
			if !isStoragelessNode && driverName == vsphere.DriverName {
				driver, _ := node.Get(driverName)
				destroyErr = driver.DestroyVM(n)
				return destroyErr
			}
			return destroyErr
		})

		if err = eg.Wait(); err != nil {
			return err
		}

		// Removing the node from the nodeRegistry
		log.Infof("Deleting node %s from node registry", n.Name)
		err = node.DeleteNode(n)
		if err != nil {
			return &scheduler.ErrFailedToUpdateNodeList{
				Node: n.Name,
				Cause: fmt.Sprintf(
					"Failed to remove OCP node [%s] from node list. Error: [%v]", n.Name, err),
			}

		}
		log.Infof("Successfully deleted the OCP node: [%s] ", n.Name)

		// OCP creates a new node once the desired number of worker node count goes down
		// Wait for OCP to provision new node and update new node to the k8s node list
		newOCPNode, err := k.checkAndGetNewNode()
		if err != nil {
			return &scheduler.ErrFailedToGetNode{
				Cause: fmt.Sprintf("Failed to get newly created OCP node name. Error: [%v]", err),
			}
		}

		// Getting k8s node
		newNode, err := k8sCore.GetNodeByName(newOCPNode)
		if err != nil {
			return err
		}

		//Adding a new node to a nodeRegistry
		if err = k.AddNewNode(*newNode); err != nil {
			return &scheduler.ErrFailedToUpdateNodeList{
				Node: newOCPNode,
				Cause: fmt.Sprintf(
					"Failed to update new OCP node [%s] in node list. Error: [%v]", newOCPNode, err),
			}
		}

		// Getting the node object for a new node
		newlyProvNode, err := node.GetNodeByName(newOCPNode)

		if err != nil {
			return err
		}

		// Waits for px pod to be up in new node
		if err = volDriver.WaitForPxPodsToBeUp(newlyProvNode); err != nil {
			return err
		}

		// Validation is needed only when deleted node was StorageNode
		if err = k.validateDrivesAfterNewNodePickUptheID(delNode, volDriver,
			storagelessNodes, isStoragelessNode,
		); err != nil {
			return err
		}

		// Update the new node object with storage information
		if err = volDriver.UpdateNodeWithStorageInfo(newlyProvNode, n.Name); err != nil {
			return err
		}
		log.Infof("Successfully updated the storage info for new node: [%s] ", newlyProvNode.Name)

		// Getting the new node object after storage info updated
		newlyProvNode, err = node.GetNodeByName(newlyProvNode.Name)
		if err != nil {
			return err
		}

		log.Infof("Waiting for driver to be come up on node: [%s] ", newlyProvNode.Name)
		// Waiting and make sure driver to come up successfuly on newly provisoned node
		if err = volDriver.WaitDriverUpOnNode(newlyProvNode, driverUpTimeout); err != nil {
			return err
		}
		log.Infof("Driver came up successfully on node: [%s] ", newlyProvNode.Name)

		return nil

	}
	return fmt.Errorf("FAILED: Node is not a worker node")
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

// String returns the string name of this driver.
func (k *openshift) String() string {
	return SchedName
}

func getParsedVersion(version string) (semver.Version, error) {
	if versionReg.MatchString(version) {
		cli := &http.Client{}
		url := fmt.Sprintf("https://mirror.openshift.com/pub/openshift-v4/clients/ocp/%s/release.txt", version)
		resp, err := cli.Get(url)
		if err != nil {
			return semver.Version{}, err
		}
		defer resp.Body.Close()
		output, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return semver.Version{}, err
		}
		var re = regexp.MustCompile(`(?m)Name:\s+([\d.]+)`)
		match := re.FindStringSubmatch(string(output))
		if len(match) > 1 {
			version = match[1]
		}
	}

	parsedVersion, err := semver.Parse(version)
	if err != nil {
		return semver.Version{}, err
	}
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

// ScaleCluster scale the cluster to the given replicas
func (k *openshift) ScaleCluster(replicas int) error {

	machineSetName, err := getMachineSetName()
	if err != nil {
		return err
	}
	// kubectl scale machineset leela-ocp-vx6zf-worker-0 --replicas 6 -n openshift-machine-api
	cmd := fmt.Sprintf("kubectl -n %s scale %s --replicas %d", OpenshiftMachineNamespace, machineSetName, replicas)
	log.Infof("Running cmnd : %s", cmd)
	output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		return err
	}

	log.Infof("output : %s", string(output))

	if !strings.Contains(string(output), fmt.Sprintf("%s scaled", machineSetName)) {
		return fmt.Errorf("failed to scale %s, output %s", machineSetName, string(output))
	}

	_, err = k.checkAndGetNewNode()
	if err != nil {
		return err
	}
	err = k.RefreshNodeRegistry()
	if err != nil {
		return err
	}

	return nil
}

func init() {
	k := &openshift{}
	scheduler.Register(SchedName, k)
}
