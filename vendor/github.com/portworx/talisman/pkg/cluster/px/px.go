package px

import (
	"encoding/csv"
	"fmt"
	"regexp"
	"strings"
	"time"

	osd_api "github.com/libopenstorage/openstorage/api"
	osd_clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	osd_volclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/consul"
	e2 "github.com/portworx/kvdb/etcd/v2"
	e3 "github.com/portworx/kvdb/etcd/v3"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	apiv1beta1 "github.com/portworx/talisman/pkg/apis/portworx/v1beta1"
	"github.com/portworx/talisman/pkg/k8sutils"
	"github.com/sirupsen/logrus"
	apps_api "k8s.io/api/apps/v1beta2"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type pxInstallType string

var (
	errUsingInternalEtcd = fmt.Errorf("cluster is using internal etcd")
	configMapNameRegex   = regexp.MustCompile("[^a-zA-Z0-9]+")
)

const (
	pxInstallTypeOCI       pxInstallType = "oci"
	pxInstallTypeDocker    pxInstallType = "docker"
	pxInstallTypeNone      pxInstallType = "none"
	changeCauseAnnotation                = "kubernetes.io/change-cause"
	pxEnableLabelKey                     = "px/enabled"
	dsOptPwxVolumeName                   = "optpwx"
	dsEtcPwxVolumeName                   = "etcpwx"
	dsDbusVolumeName                     = "dbus"
	dsSysdVolumeName                     = "sysdmount"
	sysdmount                            = "/etc/systemd/system"
	dbusPath                             = "/var/run/dbus"
	pksPersistentStoreRoot               = "/var/vcap/store"
	pxOptPwx                             = "/opt/pwx"
	pxEtcdPwx                            = "/etc/pwx"
)

type platformType string

const (
	platformTypeDefault platformType = ""
	platformTypePKS     platformType = "pks"
)

// SharedAppsScaleDownMode is type for choosing behavior of scaling down shared apps
type SharedAppsScaleDownMode string

const (
	// SharedAppsScaleDownAuto is mode where shared px apps will be scaled down only when px major version is upgraded
	SharedAppsScaleDownAuto SharedAppsScaleDownMode = "auto"
	// SharedAppsScaleDownOn is mode where shared px apps will be scaled down without any version checks
	SharedAppsScaleDownOn SharedAppsScaleDownMode = "on"
	// SharedAppsScaleDownOff is mode where shared px apps will not be scaled down
	SharedAppsScaleDownOff SharedAppsScaleDownMode = "off"
)

var (
	pxVersionRegex         = regexp.MustCompile(`^(\d+\.\d+\.\d+).*`)
	ociMonImageRegex       = regexp.MustCompile(`.*registry.connect.redhat.com/portworx/(px-enterprise|px-monitor).+|.+oci-monitor.+`)
	pxEnterpriseImageRegex = regexp.MustCompile(".+px-enterprise.+")
)

const (
	pxDefaultNamespace              = "kube-system"
	pxSecretsNamespace              = "portworx"
	defaultPXImage                  = "portworx/px-enterprise"
	dockerPullerImage               = "portworx/docker-puller:latest"
	pxNodeWiperImage                = "portworx/px-node-wiper:1.5.1"
	pxdRestPort                     = 9001
	pxServiceName                   = "portworx-service"
	pxClusterRoleName               = "node-get-put-list-role"
	pxClusterRoleBindingName        = "node-role-binding"
	pxRoleName                      = "px-role"
	pxRoleBindingName               = "px-role-binding"
	pxServiceAccountName            = "px-account"
	lhRoleName                      = "px-lh-role"
	lhRoleBindingName               = "px-lh-role-binding"
	lhServiceAccountName            = "px-lh-account"
	lhConfigMap                     = "px-lighthouse-config"
	lhServiceName                   = "px-lighthouse"
	lhDeploymentName                = "px-lighthouse"
	internalEtcdConfigMapPrefix     = "px-bootstrap-"
	cloudDriveConfigMapPrefix       = "px-cloud-drive-"
	pvcControllerClusterRole        = "portworx-pvc-controller-role"
	pvcControllerClusterRoleBinding = "portworx-pvc-controller-role-binding"
	pvcControllerServiceAccount     = "portworx-pvc-controller-account"
	pvcControllerName               = "portworx-pvc-controller"
	pxVersionLabel                  = "PX Version"
	talismanServiceAccount          = "talisman-account"
	storkControllerName             = "stork"
	storkServiceName                = "stork-service"
	storkControllerConfigMap        = "stork-config"
	storkControllerClusterRole      = "stork-role"
	storkControllerClusterBinding   = "stork-role-binding"
	storkControllerServiceAccount   = "stork-account"
	storkSchedulerClusterRole       = "stork-scheduler-role"
	storkSchedulerCluserBinding     = "stork-scheduler-role-binding"
	storkSchedulerServiceAccount    = "stork-scheduler-account"
	storkSchedulerName              = "stork-scheduler"
	storkSnapshotStorageClass       = "stork-snapshot-sc"
	pxNodeWiperDaemonSetName        = "px-node-wiper"
	pxKvdbPrefix                    = "pwx/"
)

type pxClusterOps struct {
	k8sOps               k8s.Ops
	utils                *k8sutils.Instance
	dockerRegistrySecret string
	platform             platformType
}

// timeouts and intervals
const (
	defaultRetryInterval          = 10 * time.Second
	pxNodeWiperTimeout            = 10 * time.Minute
	sharedVolDetachTimeout        = 5 * time.Minute
	daemonsetUpdateTriggerTimeout = 5 * time.Minute
	daemonsetDeleteTimeout        = 5 * time.Minute
)

// UpgradeOptions are options to customize the upgrade process
type UpgradeOptions struct {
	SharedAppsScaleDown SharedAppsScaleDownMode
}

// DeleteOptions are options to customize the delete process
type DeleteOptions struct {
	// WipeCluster instructs if Portworx cluster metadata needs to be wiped off
	WipeCluster bool
}

// Cluster an interface to manage a storage cluster
type Cluster interface {
	// Create creates the given cluster
	Create(c *apiv1beta1.Cluster) error
	// Status returns the current status of the given cluster
	Status(c *apiv1beta1.Cluster) (*apiv1beta1.ClusterStatus, error)
	// Upgrade upgrades the given cluster
	Upgrade(c *apiv1beta1.Cluster, opts *UpgradeOptions) error
	// Delete removes all components of the given cluster
	Delete(c *apiv1beta1.Cluster, opts *DeleteOptions) error
}

// NewPXClusterProvider creates a new PX cluster
func NewPXClusterProvider(dockerRegistrySecret, kubeconfig string) (Cluster, error) {
	utils, err := k8sutils.New(kubeconfig)
	if err != nil {
		return nil, err
	}

	return &pxClusterOps{
		k8sOps:               k8s.Instance(),
		dockerRegistrySecret: dockerRegistrySecret,
		utils:                utils,
	}, nil
}

func (ops *pxClusterOps) Create(spec *apiv1beta1.Cluster) error {
	logrus.Warnf("creating px cluster is not yet implemented")
	return nil
}

func (ops *pxClusterOps) Status(c *apiv1beta1.Cluster) (*apiv1beta1.ClusterStatus, error) {
	logrus.Warnf("cluster status is not yet implemented")
	return nil, nil
}

func (ops *pxClusterOps) Upgrade(newSpec *apiv1beta1.Cluster, opts *UpgradeOptions) error {
	if newSpec == nil {
		return fmt.Errorf("new cluster spec is required for the upgrade call")
	}

	svc, err := ops.k8sOps.GetService(pxServiceName, pxDefaultNamespace)
	if err != nil {
		return err
	}

	ip := svc.Spec.ClusterIP
	if len(ip) == 0 {
		return fmt.Errorf("Portworx service doesn't have a ClusterIP assigned")
	}

	volDriver, clusterManager, err := getPXDriver(ip)
	if err != nil {
		return err
	}

	if err := ops.preFlightChecks(newSpec); err != nil {
		return err
	}

	if len(newSpec.Spec.PXImage) == 0 {
		newSpec.Spec.PXImage = defaultPXImage
	}

	if len(newSpec.Spec.PXTag) == 0 {
		newSpec.Spec.PXTag = newSpec.Spec.OCIMonTag
	}

	newOCIMonVer := fmt.Sprintf("%s:%s", newSpec.Spec.OCIMonImage, newSpec.Spec.OCIMonTag)

	isAppDrainNeeded, err := ops.isUpgradeAppDrainRequired(newSpec, clusterManager)
	if err != nil {
		return err
	}

	if isAppDrainNeeded {
		unManaged, err := ops.utils.IsAnyPXAppPodUnmanaged()
		if err != nil {
			return err
		}

		if unManaged {
			return fmt.Errorf("Aborting upgrade as some application pods using Portworx are not being managed by a controller." +
				" Retry after deleting these pods or running them using a controller (e.g Deployment, Statefulset etc).")
		}
	}

	logrus.Infof("Upgrading px cluster to %s. Upgrade opts: %v App drain requirement: %v", newOCIMonVer, opts, isAppDrainNeeded)

	logrus.Info("Attempting to parse kvdb info from Portworx daemonset")
	var configParseErr error
	_, _, _, ops.platform, configParseErr = ops.parseConfigFromDaemonset()
	if configParseErr != nil && configParseErr != errUsingInternalEtcd {
		err := fmt.Errorf("Failed to parse PX config from Daemonset for upgrading PX due to err: %v", configParseErr)
		return err
	}

	// NOTE: Skip step 1. with ops.runDockerPuller(newOCIMonVer, newPXVer) as it doesn't handle private registries, air gapped installs and containerd

	// 2. (Optional) Scale down px shared applications to 0 replicas if required based on opts
	if ops.isScaleDownOfSharedAppsRequired(isAppDrainNeeded, opts) {
		defer func() { // always restore the replicas
			err = ops.utils.RestoreScaledAppsReplicas()
			if err != nil {
				logrus.Errorf("Failed to restore PX shared applications replicas. Err: %v", err)
			}
		}()

		err = ops.utils.ScaleSharedAppsToZero()
		if err != nil {
			return err
		}

		// Wait till all px shared volumes are detached
		err = ops.waitTillPXSharedVolumesDetached(volDriver)
		if err != nil {
			return err
		}
	} else {
		logrus.Infof("Skipping scale down of shared volume applications")
	}

	// 3. Start rolling upgrade of PX DaemonSet
	err = ops.upgradePX(newOCIMonVer)
	if err != nil {
		return err
	}

	return nil
}

func (ops *pxClusterOps) Delete(c *apiv1beta1.Cluster, opts *DeleteOptions) error {
	// parse kvdb from daemonset before we delete it
	logrus.Info("Attempting to parse kvdb info from Portworx daemonset")
	var (
		endpoints      []string
		kvdbOpts       map[string]string
		clusterName    string
		configParseErr error
	)
	endpoints, kvdbOpts, clusterName, ops.platform, configParseErr = ops.parseConfigFromDaemonset()
	if configParseErr != nil && configParseErr != errUsingInternalEtcd {
		err := fmt.Errorf("Failed to parse PX config from Daemonset for deleting PX due to err: %v", configParseErr)
		return err
	}

	pwxHostPathRoot := "/"
	if ops.platform == platformTypePKS {
		pwxHostPathRoot = pksPersistentStoreRoot
	}

	if err := ops.deleteAllPXComponents(clusterName); err != nil {
		return err // this error is unexpected and should not be ignored
	}

	if opts != nil && opts.WipeCluster {
		// the wipe operation is best effort as it is used when cluster is already in a bad shape. for e.g
		// cluster might have been started with incorrect kvdb information. So we won't be able to wipe that off.

		// Wipe px from each node
		err := ops.runPXNodeWiper(pwxHostPathRoot)
		if err != nil {
			logrus.Warnf("Failed to wipe Portworx local node state. err: %v", err)
		}

		if configParseErr == errUsingInternalEtcd {
			logrus.Infof("Cluster is using internal etcd. No need to wipe kvdb.")
		} else {
			// Cleanup PX kvdb tree
			err = ops.wipePXKvdb(endpoints, kvdbOpts, clusterName)
			if err != nil {
				logrus.Warnf("Failed to wipe Portworx KVDB tree. err: %v", err)
			}
		}
	}

	return nil
}

// waitTillPXSharedVolumesDetached waits till all shared volumes are detached
func (ops *pxClusterOps) waitTillPXSharedVolumesDetached(volDriver volume.VolumeDriver) error {
	scs, err := ops.utils.GetPXSharedSCs()
	if err != nil {
		return err
	}

	var volsToInspect []string
	for _, sc := range scs {
		pvcs, err := ops.k8sOps.GetPVCsUsingStorageClass(sc)
		if err != nil {
			return err
		}

		for _, pvc := range pvcs {
			pv, err := ops.k8sOps.GetVolumeForPersistentVolumeClaim(&pvc)
			if err != nil {
				return err
			}

			volsToInspect = append(volsToInspect, pv)
		}
	}

	logrus.Infof("Waiting for detachment of PX shared volumes: %s", volsToInspect)

	t := func() (interface{}, bool, error) {

		vols, err := volDriver.Inspect(volsToInspect)
		if err != nil {
			return nil, true, err
		}

		for _, v := range vols {
			if v.AttachedState == osd_api.AttachState_ATTACH_STATE_EXTERNAL {
				return nil, true, fmt.Errorf("volume: %s is still attached", v.Locator.Name)
			}
		}

		logrus.Infof("All shared volumes: %v detached", volsToInspect)
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, sharedVolDetachTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// getPXDriver returns an instance of the PX volume and cluster driver client
func getPXDriver(ip string) (volume.VolumeDriver, cluster.Cluster, error) {
	pxEndpoint := fmt.Sprintf("http://%s:%d", ip, pxdRestPort)

	dClient, err := osd_volclient.NewDriverClient(pxEndpoint, "pxd", "", "pxd")
	if err != nil {
		return nil, nil, err
	}

	cClient, err := osd_clusterclient.NewClusterClient(pxEndpoint, "v1")
	if err != nil {
		return nil, nil, err
	}

	volDriver := osd_volclient.VolumeDriver(dClient)
	clusterManager := osd_clusterclient.ClusterManager(cClient)
	return volDriver, clusterManager, nil
}

// getPXDaemonsets return PX daemonsets in the cluster based on given installer type
func (ops *pxClusterOps) getPXDaemonsets(installType pxInstallType) ([]apps_api.DaemonSet, error) {
	listOpts := metav1.ListOptions{
		LabelSelector: "name=portworx",
	}

	dss, err := ops.k8sOps.ListDaemonSets(pxDefaultNamespace, listOpts)
	if err != nil {
		return nil, err
	}

	var ociList, dockerList []apps_api.DaemonSet
	for _, ds := range dss {
		for _, c := range ds.Spec.Template.Spec.Containers {
			if isPXOCIImage(c.Image) {
				ociList = append(ociList, ds)
				break
			} else if isPXEnterpriseImage(c.Image) {
				dockerList = append(dockerList, ds)
				break
			}
		}
	}

	if installType == pxInstallTypeDocker {
		return dockerList, nil
	}

	return ociList, nil
}

// upgradePX upgrades PX daemonsets and waits till all replicas are ready
func (ops *pxClusterOps) upgradePX(newVersion string) error {
	var err error

	// update RBAC cluster role
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: pxClusterRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"nodes"},
				Verbs:     []string{"get", "update", "list", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"persistentvolumeclaims", "persistentvolumes"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "update", "list", "create"},
			},
		},
	}

	logrus.Infof("Updating [%s] cluster role", pxClusterRoleName)
	_, err = ops.k8sOps.UpdateClusterRole(clusterRole)
	if err != nil {
		return err
	}

	// create or update Kubernetes secrets permissions
	if err := ops.updatePxSecretsPermissions(); err != nil {
		return err
	}

	var dss []apps_api.DaemonSet
	expectedGenerations := make(map[types.UID]int64)

	t := func() (interface{}, bool, error) {
		dss, err = ops.getPXDaemonsets(pxInstallTypeOCI)
		if err != nil {
			return nil, true, err
		}

		if len(dss) == 0 {
			return nil, true, fmt.Errorf("did not find any PX daemonsets for install type: %s", pxInstallTypeOCI)
		}

		for _, ds := range dss {
			skip := false
			logrus.Infof("Upgrading PX daemonset: [%s] %s to version: %s", ds.Namespace, ds.Name, newVersion)
			dsCopy := ds.DeepCopy()
			for i := 0; i < len(dsCopy.Spec.Template.Spec.Containers); i++ {
				c := &dsCopy.Spec.Template.Spec.Containers[i]
				if isPXOCIImage(c.Image) {
					if c.Image == newVersion {
						logrus.Infof("Skipping upgrade of PX daemonset: [%s] %s as it is already at %s version.",
							ds.Namespace, ds.Name, newVersion)
						expectedGenerations[ds.UID] = ds.Status.ObservedGeneration
						skip = true
					} else {
						expectedGenerations[ds.UID] = ds.Status.ObservedGeneration + 1
						c.Image = newVersion
						dsCopy.Annotations[changeCauseAnnotation] = fmt.Sprintf("update PX to %s", newVersion)
					}
					break
				}
			}

			if skip {
				continue
			}

			updatedDS, err := ops.k8sOps.UpdateDaemonSet(dsCopy)
			if err != nil {
				return nil, true, err
			}

			logrus.Infof("Initiated upgrade of PX daemonset: [%s] %s to version: %s",
				updatedDS.Namespace, updatedDS.Name, newVersion)
		}

		return nil, false, nil
	}

	if _, err = task.DoRetryWithTimeout(t, daemonsetUpdateTriggerTimeout, defaultRetryInterval); err != nil {
		return err
	}

	for _, ds := range dss {
		logrus.Infof("Checking upgrade status of PX daemonset: [%s] %s to version: %s", ds.Namespace, ds.Name, newVersion)

		t = func() (interface{}, bool, error) {
			updatedDS, err := ops.k8sOps.GetDaemonSet(ds.Name, ds.Namespace)
			if err != nil {
				return nil, true, err
			}

			expectedGeneration, _ := expectedGenerations[ds.UID]
			if updatedDS.Status.ObservedGeneration != expectedGeneration {
				return nil, true, fmt.Errorf("PX daemonset: [%s] %s still running previous generation: %d. Expected generation: %d",
					ds.Namespace, ds.Name, updatedDS.Status.ObservedGeneration, expectedGeneration)
			}

			return nil, false, nil
		}

		if _, err = task.DoRetryWithTimeout(t, daemonsetUpdateTriggerTimeout, defaultRetryInterval); err != nil {
			return err
		}

		daemonsetReadyTimeout, err := ops.getDaemonSetReadyTimeout()
		if err != nil {
			return err
		}

		logrus.Infof("Doing additional validations of PX daemonset: [%s] %s. timeout: %v", ds.Namespace, ds.Name, daemonsetReadyTimeout)
		err = ops.k8sOps.ValidateDaemonSet(ds.Name, ds.Namespace, daemonsetReadyTimeout)
		if err != nil {
			return err
		}

		logrus.Infof("Successfully upgraded PX daemonset: [%s] %s to version: %s", ds.Namespace, ds.Name, newVersion)
	}

	return nil
}

// updatePxSecretsPermissions will update the permissions needed by PX to access secrets
func (ops *pxClusterOps) updatePxSecretsPermissions() error {
	// create px namespace to store secrets
	logrus.Infof("Creating [%s] namespace", pxSecretsNamespace)
	_, err := ops.k8sOps.CreateNamespace(pxSecretsNamespace, nil)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	// update RBAC role
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxRoleName,
			Namespace: pxSecretsNamespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "list", "create", "update", "patch"},
			},
		},
	}

	logrus.Infof("Updating [%s] role in [%s] namespace", pxRoleName, pxSecretsNamespace)
	if err := ops.createOrUpdateRole(role); err != nil {
		return err
	}

	// update RBAC role binding
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxRoleBindingName,
			Namespace: pxSecretsNamespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      pxServiceAccountName,
				Namespace: pxDefaultNamespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			Name:     pxRoleName,
			APIGroup: "rbac.authorization.k8s.io",
		},
	}

	logrus.Infof("Updating [%s] rolebinding in [%s] namespace", pxRoleBindingName, pxSecretsNamespace)
	return ops.createOrUpdateRoleBinding(roleBinding)
}

// createOrUpdateRole creates a given role or updates if it already exists
func (ops *pxClusterOps) createOrUpdateRole(role *rbacv1.Role) error {
	_, err := ops.k8sOps.CreateRole(role)
	if errors.IsAlreadyExists(err) {
		if _, err = ops.k8sOps.UpdateRole(role); err != nil {
			return err
		}
	}
	return err
}

// createOrUpdateRoleBinding creates a given role binding or updates if it already exists
func (ops *pxClusterOps) createOrUpdateRoleBinding(binding *rbacv1.RoleBinding) error {
	_, err := ops.k8sOps.CreateRoleBinding(binding)
	if errors.IsAlreadyExists(err) {
		if _, err = ops.k8sOps.UpdateRoleBinding(binding); err != nil {
			return err
		}
	}
	return err
}

// isAnyNodeRunningVersionWithPrefix checks if any node in the cluster has a PX version with given prefix
func (ops *pxClusterOps) isAnyNodeRunningVersionWithPrefix(versionPrefix string, clusterManager cluster.Cluster) (bool, error) {
	cluster, err := clusterManager.Enumerate()
	if err != nil {
		return false, err
	}

	for _, n := range cluster.Nodes {
		ver := cluster.Nodes[0].NodeLabels[pxVersionLabel]
		if len(ver) == 0 {
			return false, fmt.Errorf("no version found in labels for node: %s", cluster.Nodes[0].Id)
		}

		if strings.HasPrefix(ver, versionPrefix) {
			logrus.Infof("Node: %s has version: %s with prefix: %s", n.Id, ver, versionPrefix)
			return true, nil
		}
	}

	return false, nil
}

// isScaleDownOfSharedAppsRequired decides if scaling down of apps using PX is required
func (ops *pxClusterOps) isScaleDownOfSharedAppsRequired(isMajorVerUpgrade bool, opts *UpgradeOptions) bool {
	if opts == nil || len(opts.SharedAppsScaleDown) == 0 || opts.SharedAppsScaleDown == SharedAppsScaleDownAuto {
		return isMajorVerUpgrade
	}

	return opts.SharedAppsScaleDown == SharedAppsScaleDownOn
}

// isUpgradeAppDrainRequired checks if target is 1.3.3/1.3.4 or upgrade is from 1.2 to 1.3/1.4
func (ops *pxClusterOps) isUpgradeAppDrainRequired(spec *apiv1beta1.Cluster, clusterManager cluster.Cluster) (bool, error) {
	currentVersionDublin, err := ops.isAnyNodeRunningVersionWithPrefix("1.2", clusterManager)
	if err != nil {
		return false, err
	}

	newVersion, err := parsePXVersion(spec.Spec.OCIMonTag)
	if err != nil {
		return false, err
	}

	logrus.Infof("Is any node running dublin version: %v. new version: %s", currentVersionDublin, newVersion)

	if strings.HasPrefix(newVersion, "1.3.3") || strings.HasPrefix(newVersion, "1.3.4") {
		// 1.3.3/1.3.4 has a change that requires a reboot even if starting version is not dublin
		return true, nil
	}

	return currentVersionDublin && (strings.HasPrefix(newVersion, "1.3") || strings.HasPrefix(newVersion, "1.4")), nil
}

func (ops *pxClusterOps) preFlightChecks(spec *apiv1beta1.Cluster) error {
	dss, err := ops.getPXDaemonsets(pxInstallTypeDocker)
	if err != nil {
		return err
	}

	if len(dss) > 0 {
		return fmt.Errorf("pre-flight check failed as found: %d PX DaemonSet(s) of type docker. Only upgrading from OCI is supported", len(dss))
	}

	if len(spec.Spec.OCIMonTag) == 0 {
		return fmt.Errorf("pre-flight check failed as new version of Portworx OCI monitor not given to upgrade API")
	}

	// check if PX is not ready in any of the nodes
	dss, err = ops.getPXDaemonsets(pxInstallTypeOCI)
	if err != nil {
		return err
	}

	if len(dss) == 0 {
		return fmt.Errorf("pre-flight check failed as it did not find any PX daemonsets for install type: %s", pxInstallTypeOCI)
	}

	for _, d := range dss {
		if err = ops.k8sOps.ValidateDaemonSet(d.Name, d.Namespace, 1*time.Minute); err != nil {
			return fmt.Errorf("pre-flight check failed as existing Portworx DaemonSet is not ready. err: %v", err)
		}
	}

	return nil
}

func (ops *pxClusterOps) getDaemonSetReadyTimeout() (time.Duration, error) {
	nodes, err := ops.k8sOps.GetNodes()
	if err != nil {
		return 0, err
	}

	daemonsetReadyTimeout := time.Duration(len(nodes.Items)-1) * 10 * time.Minute
	return daemonsetReadyTimeout, nil
}

func (ops *pxClusterOps) wipePXKvdb(endpoints []string, opts map[string]string, clusterName string) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("endpoints not supplied for wiping KVDB")
	}

	if len(clusterName) == 0 {
		return fmt.Errorf("PX cluster name not supplied for wiping KVDB")
	}

	logrus.Infof("Creating kvdb client for: %v", endpoints)
	kvdbInst, err := getKVDBClient(endpoints, opts)
	if err != nil {
		return err
	}

	logrus.Infof("Deleting Portworx kvdb tree: %s/%s for endpoint(s): %v", pxKvdbPrefix, clusterName, endpoints)
	return kvdbInst.DeleteTree(clusterName)
}

func (ops *pxClusterOps) parseConfigFromDaemonset() ([]string, map[string]string, string, platformType, error) {
	dss, err := ops.getPXDaemonsets(pxInstallTypeOCI)
	if err != nil {
		return nil, nil, "", platformTypeDefault, err
	}

	if len(dss) == 0 {
		return nil, nil, "", platformTypeDefault, fmt.Errorf("Portworx daemonset not found on the cluster. Ensure you have " +
			"Portworx specs applied in the cluster before issuing this operation.")
	}

	platform := platformTypeDefault
	ds := dss[0]
	var clusterName string
	var usingInternalEtcd bool
	endpoints := make([]string, 0)
	opts := make(map[string]string)

	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.HostPath != nil && volume.Name == dsOptPwxVolumeName {
			if strings.HasPrefix(volume.HostPath.Path, pksPersistentStoreRoot) {
				logrus.Infof("Detected %s install.", platformTypePKS)
				platform = platformTypePKS
				break
			}
		}
	}

	for _, c := range ds.Spec.Template.Spec.Containers {
		if isPXOCIImage(c.Image) || isPXEnterpriseImage(c.Image) {
			for i, arg := range c.Args {
				// Reference : https://docs.portworx.com/scheduler/kubernetes/px-k8s-spec-curl.html
				switch arg {
				case "-b":
					usingInternalEtcd = true
				case "-c":
					clusterName = c.Args[i+1]
				case "-k":
					endpoints, err = splitCSV(c.Args[i+1])
					if err != nil {
						return nil, nil, clusterName, platform, err
					}
				case "-pwd":
					parts := strings.Split(c.Args[i+1], ":")
					if len(parts) != 0 {
						return nil, nil, clusterName, platform, fmt.Errorf("failed to parse kvdb username and password: %s", c.Args[i+1])
					}
					opts[kvdb.UsernameKey] = parts[0]
					opts[kvdb.PasswordKey] = parts[1]
				case "-ca":
					opts[kvdb.CAFileKey] = c.Args[i+1]
				case "-cert":
					opts[kvdb.CertFileKey] = c.Args[i+1]
				case "-key":
					opts[kvdb.CertKeyFileKey] = c.Args[i+1]
				case "-acl":
					opts[kvdb.ACLTokenKey] = c.Args[i+1]
				default:
					// pass
				}
			}

			break
		}
	}

	if usingInternalEtcd {
		return endpoints, opts, clusterName, platform, errUsingInternalEtcd
	}

	if len(endpoints) == 0 {
		return nil, opts, clusterName, platform, fmt.Errorf("failed to get kvdb endpoint from daemonset containers: %v",
			ds.Spec.Template.Spec.Containers)
	}

	if len(clusterName) == 0 {
		return endpoints, opts, "", platform, fmt.Errorf("failed to get cluster name from daemonset containers: %v",
			ds.Spec.Template.Spec.Containers)
	}

	return endpoints, opts, clusterName, platform, nil
}

func (ops *pxClusterOps) deleteAllPXComponents(clusterName string) error {
	logrus.Infof("Deleting all PX Kubernetes components from the cluster")

	dss, err := ops.getPXDaemonsets(pxInstallTypeOCI)
	if err != nil {
		return err
	}

	for _, ds := range dss {
		err = ops.k8sOps.DeleteDaemonSet(ds.Name, ds.Namespace)
		if err != nil {
			return err
		}
	}

	depNames := [4]string{
		storkControllerName,
		storkSchedulerName,
		pvcControllerName,
		lhDeploymentName,
	}
	for _, depName := range depNames {
		err = ops.k8sOps.DeleteDeployment(depName, pxDefaultNamespace)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	roles := [2][2]string{
		{pxRoleName, pxSecretsNamespace},
		{lhRoleName, pxDefaultNamespace},
	}
	for _, role := range roles {
		err = ops.k8sOps.DeleteRole(role[0], role[1])
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	roleBindings := [2][2]string{
		{pxRoleBindingName, pxSecretsNamespace},
		{lhRoleBindingName, pxDefaultNamespace},
	}
	for _, binding := range roleBindings {
		err = ops.k8sOps.DeleteRoleBinding(binding[0], binding[1])
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	clusterWideSecret := strings.Replace(clusterName+"_px_secret", "_", "-", -1)
	err = ops.k8sOps.DeleteSecret(clusterWideSecret, pxSecretsNamespace)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	clusterRoles := [4]string{
		pxClusterRoleName,
		storkControllerClusterRole,
		storkSchedulerClusterRole,
		pvcControllerClusterRole,
	}
	for _, role := range clusterRoles {
		err = ops.k8sOps.DeleteClusterRole(role)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	clusterRoleBindings := [4]string{
		pxClusterRoleBindingName,
		storkControllerClusterBinding,
		storkSchedulerCluserBinding,
		pvcControllerClusterRoleBinding,
	}
	for _, binding := range clusterRoleBindings {
		err = ops.k8sOps.DeleteClusterRoleBinding(binding)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	accounts := [5]string{
		pxServiceAccountName,
		lhServiceAccountName,
		storkControllerServiceAccount,
		storkSchedulerServiceAccount,
		pvcControllerServiceAccount,
	}
	for _, acc := range accounts {
		err = ops.k8sOps.DeleteServiceAccount(acc, pxDefaultNamespace)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	services := [3]string{
		pxServiceName,
		storkServiceName,
		lhServiceName,
	}
	for _, svc := range services {
		err = ops.k8sOps.DeleteService(svc, pxDefaultNamespace)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	strippedClusterName := strings.ToLower(configMapNameRegex.ReplaceAllString(clusterName, ""))
	configMaps := [4]string{
		lhConfigMap,
		storkControllerConfigMap,
		fmt.Sprintf("%s%s", internalEtcdConfigMapPrefix, strippedClusterName),
		fmt.Sprintf("%s%s", cloudDriveConfigMapPrefix, strippedClusterName),
	}
	for _, cm := range configMaps {
		err = ops.k8sOps.DeleteConfigMap(cm, pxDefaultNamespace)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	err = ops.k8sOps.DeleteStorageClass(storkSnapshotStorageClass)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (ops *pxClusterOps) runPXNodeWiper(pwxHostPathRoot string) error {
	trueVar := true
	labels := map[string]string{
		"name": pxNodeWiperDaemonSetName,
	}
	args := []string{"-w"}
	ds := &apps_api.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pxNodeWiperDaemonSetName,
			Namespace: pxDefaultNamespace,
			Labels:    labels,
		},
		Spec: apps_api.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            pxNodeWiperDaemonSetName,
							Image:           pxNodeWiperImage,
							ImagePullPolicy: corev1.PullAlways,
							Args:            args,
							SecurityContext: &corev1.SecurityContext{
								Privileged: &trueVar,
							},
							ReadinessProbe: &corev1.Probe{
								InitialDelaySeconds: 30,
								Handler: corev1.Handler{
									Exec: &corev1.ExecAction{
										Command: []string{"cat", "/tmp/px-node-wipe-done"},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      dsEtcPwxVolumeName,
									MountPath: pxEtcdPwx,
								},
								{
									Name:      "hostproc",
									MountPath: "/hostproc",
								},
								{
									Name:      dsOptPwxVolumeName,
									MountPath: pxOptPwx,
								},
								{
									Name:      dsDbusVolumeName,
									MountPath: dbusPath,
								},
								{
									Name:      dsSysdVolumeName,
									MountPath: sysdmount,
								},
							},
						},
					},
					RestartPolicy:      "Always",
					ServiceAccountName: talismanServiceAccount,
					Volumes: []corev1.Volume{
						{
							Name: dsEtcPwxVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: pwxHostPathRoot + pxEtcdPwx,
								},
							},
						},
						{
							Name: "hostproc",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/proc",
								},
							},
						},
						{
							Name: dsOptPwxVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: pwxHostPathRoot + pxOptPwx,
								},
							},
						},
						{
							Name: dsDbusVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: dbusPath,
								},
							},
						},
						{
							Name: dsSysdVolumeName,
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: sysdmount,
								},
							},
						},
					},
				},
			},
		},
	}

	return ops.runDaemonSet(ds, pxNodeWiperTimeout)
}

// runDaemonSet runs the given daemonset, attempts to validate if it ran successfully and then
// deletes it
func (ops *pxClusterOps) runDaemonSet(ds *apps_api.DaemonSet, timeout time.Duration) error {
	err := ops.k8sOps.DeleteDaemonSet(ds.Name, ds.Namespace)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	t := func() (interface{}, bool, error) {
		_, err := ops.k8sOps.GetDaemonSet(ds.Name, ds.Namespace)
		if err == nil {
			return nil, true, fmt.Errorf("daemonset: [%s] %s is still present", ds.Namespace, ds.Name)
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, daemonsetDeleteTimeout, defaultRetryInterval); err != nil {
		return err
	}

	ds, err = ops.k8sOps.CreateDaemonSet(ds)
	if err != nil {
		return err
	}

	logrus.Infof("Started daemonSet: [%s] %s", ds.Namespace, ds.Name)

	// Delete the daemonset regardless of status
	defer func(ds *apps_api.DaemonSet) {
		err := ops.k8sOps.DeleteDaemonSet(ds.Name, ds.Namespace)
		if err != nil && !errors.IsNotFound(err) {
			logrus.Warnf("error while deleting daemonset: %v", err)
		}
	}(ds)

	err = ops.k8sOps.ValidateDaemonSet(ds.Name, ds.Namespace, timeout)
	if err != nil {
		return err
	}

	logrus.Infof("Validated successful run of daemonset: [%s] %s", ds.Namespace, ds.Name)

	return nil
}

func splitCSV(in string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(in))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil || len(records) < 1 {
		return []string{}, err
	} else if len(records) > 1 {
		return []string{}, fmt.Errorf("Multiline CSV not supported")
	}
	return records[0], err
}

func getKVDBClient(endpoints []string, opts map[string]string) (kvdb.Kvdb, error) {
	var urlPrefix, kvdbType, kvdbName string
	for i, url := range endpoints {
		urlTokens := strings.Split(url, ":")
		if i == 0 {
			if urlTokens[0] == "etcd" {
				kvdbType = "etcd"
			} else if urlTokens[0] == "consul" {
				kvdbType = "consul"
			} else {
				return nil, fmt.Errorf("unknown discovery endpoint : %v in %v", urlTokens[0], endpoints)
			}
		}

		if urlTokens[1] == "http" {
			urlPrefix = "http"
			urlTokens[1] = ""
		} else if urlTokens[1] == "https" {
			urlPrefix = "https"
			urlTokens[1] = ""
		} else {
			urlPrefix = "http"
		}

		kvdbURL := ""
		for j, v := range urlTokens {
			if j == 0 {
				kvdbURL = urlPrefix
			} else {
				if v != "" {
					kvdbURL = kvdbURL + ":" + v
				}
			}
		}
		endpoints[i] = kvdbURL
	}

	var kvdbVersion string
	var err error
	for i, url := range endpoints {
		kvdbVersion, err = kvdb.Version(kvdbType+"-kv", url, opts)
		if err == nil {
			break
		} else if i == len(endpoints)-1 {
			return nil, err
		}
	}

	switch kvdbVersion {
	case kvdb.ConsulVersion1:
		kvdbName = consul.Name
	case kvdb.EtcdBaseVersion:
		kvdbName = e2.Name
	case kvdb.EtcdVersion3:
		kvdbName = e3.Name
	default:
		return nil, fmt.Errorf("Unknown kvdb endpoint (%v) and version (%v) ", endpoints, kvdbVersion)
	}

	return kvdb.New(kvdbName, pxKvdbPrefix, endpoints, opts, nil)
}

func parsePXVersion(version string) (string, error) {
	matches := pxVersionRegex.FindStringSubmatch(version)
	if len(matches) != 2 {
		return "", fmt.Errorf("failed to get PX version from %s", version)
	}

	return matches[1], nil
}

// isPXOCIImage checks if given image name is an oci monitor image
func isPXOCIImage(pxImageName string) bool {
	return ociMonImageRegex.MatchString(pxImageName)
}

// isPXEnterpriseImage checks if given image name is a px enterprise image
func isPXEnterpriseImage(pxImageName string) bool {
	if isPXOCIImage(pxImageName) { // workaround as golang doesn't have a negative match
		return false
	}

	return pxEnterpriseImageRegex.MatchString(pxImageName)
}
