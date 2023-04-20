package linstor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	lstor "github.com/LINBIT/golinstor"
	lclient "github.com/LINBIT/golinstor/client"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

const (
	// pvcProvisionerAnnotation is the annotation on PVC which has the provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"

	provisionerName = "linstor.csi.linbit.com"

	rackLabelKey = "linstor/rack"
)

type linstor struct {
	cli         *lclient.Client
	store       cache.Store
	stopChannel chan struct{}
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.BackupRestoreNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
}

func (l *linstor) linstorClient() (*lclient.Client, error) {
	if l.cli == nil {
		err := l.Init(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize driver: %w", err)
		}
	}

	return l.cli, nil
}

func (l *linstor) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	*storkapi.ApplicationRestore,
	[]runtime.Unstructured,
	[]byte,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

func (l *linstor) Init(_ interface{}) error {
	// Configuration of linstor client happens via environment variables:
	// * LS_CONTROLLERS
	// * LS_USERNAME
	// * LS_PASSWORD
	// * LS_USER_CERTIFICATE
	// * LS_USER_KEY
	// * LS_ROOT_CA
	client, err := lclient.NewClient(
		lclient.Log(logrus.StandardLogger()),
	)
	if err != nil {
		return fmt.Errorf("error creating linstor client: %w", err)
	}

	l.cli = client

	l.stopChannel = make(chan struct{})
	return l.startNodeCache()
}

func (l *linstor) startNodeCache() error {
	resyncPeriod := 30 * time.Second

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting client, %v", err)
	}

	restClient := k8sClient.CoreV1().RESTClient()

	watchlist := cache.NewListWatchFromClient(restClient, "nodes", v1.NamespaceAll, fields.Everything())
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watchlist.Watch(options)
		},
	}
	store, controller := cache.NewInformer(lw, &v1.Node{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{},
	)
	l.store = store

	go controller.Run(l.stopChannel)
	return nil
}

func (l *linstor) String() string {
	return storkvolume.LinstorDriverName
}

func (l *linstor) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

func contains(a []string, e string) bool {
	for _, x := range a {
		if x == e {
			return true
		}
	}
	return false
}

func (l *linstor) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	cli, err := l.linstorClient()
	if err != nil {
		return nil, err
	}
	rd, err := cli.ResourceDefinitions.Get(context.TODO(), volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource definition: %w", err)
	}

	resources, err := cli.Resources.GetResourceView(context.TODO(), &lclient.ListOpts{
		Resource: []string{volumeID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get resources: %w", err)
	}

	var nodes []string
	for _, r := range resources {
		state := r.Volumes[0].State.DiskState
		if state != "UpToDate" {
			logrus.Debugf("Resource %s is not UpToDate on node %s, skipping (is %s)",
				r.Name, r.NodeName, state)
			continue
		}

		if contains(r.Flags, lstor.FlagDiskless) {
			// skip diskless deployments
			logrus.Debugf("Resource %s is diskless on node %s, skipping",
				r.Name, r.NodeName)
			continue
		}
		nodes = append(nodes, r.NodeName)
	}

	vd, err := cli.ResourceDefinitions.GetVolumeDefinition(context.TODO(), volumeID, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume defintion: %w", err)
	}

	return &storkvolume.Info{
		VolumeID:   rd.Name,
		VolumeName: rd.Name,
		DataNodes:  nodes,
		Size:       vd.SizeKib / 1048576, // KiB to GiB
	}, nil
}

func (l *linstor) getNodeLabels(nodeInfo *storkvolume.NodeInfo) (map[string]string, error) {
	obj, exists, err := l.store.GetByKey(nodeInfo.SchedulerID)
	if err != nil {
		return nil, err
	} else if !exists {
		obj, exists, err = l.store.GetByKey(nodeInfo.StorageID)
		if err != nil {
			return nil, err
		} else if !exists {
			obj, exists, err = l.store.GetByKey(nodeInfo.Hostname)
			if err != nil {
				return nil, err
			} else if !exists {
				return nil, fmt.Errorf("node %v/%v/%v not found in cache", nodeInfo.StorageID, nodeInfo.SchedulerID, nodeInfo.Hostname)
			}
		}
	}
	node := obj.(*v1.Node)
	return node.Labels, nil
}

func (l *linstor) mapLinstorStatus(n lclient.Node) storkvolume.NodeStatus {
	switch n.ConnectionStatus {
	case "ONLINE":
		return storkvolume.NodeOnline
	default:
		return storkvolume.NodeOffline
	}
}

func (l *linstor) GetNodes() ([]*storkvolume.NodeInfo, error) {
	cli, err := l.linstorClient()
	if err != nil {
		return nil, err
	}
	nodes, err := cli.Nodes.GetAll(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to get linstor nodes: %w", err)
	}

	var infos []*storkvolume.NodeInfo
	for _, n := range nodes {
		var ips []string
		for _, iface := range n.NetInterfaces {
			ips = append(ips, iface.Address)
		}

		newInfo := &storkvolume.NodeInfo{
			StorageID: n.Name,
			Hostname:  n.Name,
			IPs:       ips,
			Status:    l.mapLinstorStatus(n),
		}
		labels, err := l.getNodeLabels(newInfo)
		if err == nil {
			if rack, ok := labels[rackLabelKey]; ok {
				newInfo.Rack = rack
			}
			if zone, ok := labels[v1.LabelZoneFailureDomain]; ok {
				newInfo.Zone = zone
			}
			if region, ok := labels[v1.LabelZoneRegion]; ok {
				newInfo.Region = region
			}
		}
		infos = append(infos, newInfo)
	}
	return infos, nil
}

func (l *linstor) GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*storkvolume.Info, []*storkvolume.Info, error) {
	// includePendingWFFC - Includes pending volumes in the second return value if they are using WaitForFirstConsumer binding mode
	var volumes []*storkvolume.Info
	var pendingWFFCVolumes []*storkvolume.Info
	for _, volume := range podSpec.Volumes {
		volumeName := ""
		isPendingWFFC := false
		if volume.PersistentVolumeClaim != nil {
			pvc, err := core.Instance().GetPersistentVolumeClaim(
				volume.PersistentVolumeClaim.ClaimName,
				namespace)
			if err != nil {
				return nil, nil, err
			}

			if !l.OwnsPVC(core.Instance(), pvc) {
				continue
			}

			if pvc.Status.Phase == v1.ClaimPending {
				// Only include pending volume if requested and storage class has WFFC
				if includePendingWFFC && isWaitingForFirstConsumer(pvc) {
					isPendingWFFC = true
				} else {
					return nil, nil, &storkvolume.ErrPVCPending{
						Name: volume.PersistentVolumeClaim.ClaimName,
					}
				}
			}
			volumeName = pvc.Spec.VolumeName
		}

		if volumeName != "" {
			volumeInfo, err := l.InspectVolume(volumeName)
			if err != nil {
				// If the ispect volume fails return with atleast some info
				volumeInfo = &storkvolume.Info{
					VolumeName: volumeName,
				}
			}
			if isPendingWFFC {
				pendingWFFCVolumes = append(pendingWFFCVolumes, volumeInfo)
			} else {
				volumes = append(volumes, volumeInfo)
			}
		}
	}
	return volumes, pendingWFFCVolumes, nil
}

func isWaitingForFirstConsumer(pvc *v1.PersistentVolumeClaim) bool {
	var sc *storagev1.StorageClass
	var err error
	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName != "" {
		sc, err = storage.Instance().GetStorageClass(storageClassName)
		if err != nil {
			logrus.Warnf("Did not get the storageclass %s for pvc %s/%s, err: %v", storageClassName, pvc.Namespace, pvc.Name, err)
			return false
		}
		return *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
	}
	return false
}

func (l *linstor) GetVolumeClaimTemplates(templates []v1.PersistentVolumeClaim) ([]v1.PersistentVolumeClaim, error) {
	var linstorTemplates []v1.PersistentVolumeClaim
	for _, t := range templates {
		if l.OwnsPVC(core.Instance(), &t) {
			linstorTemplates = append(linstorTemplates, t)
		}
	}
	return linstorTemplates, nil
}

func (l *linstor) OwnsPVCForBackup(coreOps core.Ops, pvc *v1.PersistentVolumeClaim, cmBackupType string, crBackupType string, blType storkapi.BackupLocationType) bool {
	return l.OwnsPVC(coreOps, pvc)
}

func (l *linstor) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {
	provisioner := ""
	// Check for the provisioner in the PVC annotation. If not populated
	// try getting the provisioner from the Storage class.
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	} else {
		storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
		if storageClassName != "" {
			storageClass, err := storage.Instance().GetStorageClass(storageClassName)
			if err == nil {
				provisioner = storageClass.Provisioner
			} else {
				logrus.Warnf("Error getting storageclass %v for pvc %v: %v", storageClassName, pvc.Name, err)
			}
		}
	}

	if provisioner == "" {
		// Try to get info from the PV since storage class could be deleted
		pv, err := coreOps.GetPersistentVolume(pvc.Spec.VolumeName)
		if err != nil {
			logrus.Warnf("Error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
			return false
		}
		return l.OwnsPV(pv)
	}

	if provisioner == provisionerName {
		return true
	}

	return false
}

func (l *linstor) OwnsPV(pv *v1.PersistentVolume) bool {
	provisioner := ""
	// Check the annotation in the PV for the provisioner
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	}
	if provisioner == provisionerName {
		return true
	}
	return false
}

func (l *linstor) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func (l *linstor) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotImplemented{}
}

func (l *linstor) Stop() error {
	close(l.stopChannel)
	return nil
}

func (l *linstor) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
	vInfo *storkapi.ApplicationRestoreVolumeInfo,
	namespaceMapping map[string]string,
) (*v1.PersistentVolume, error) {

	return pv, nil

}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (l *linstor) GetClusterID() (string, error) {
	cli, err := l.linstorClient()
	if err != nil {
		return "", err
	}
	props, err := cli.Controller.GetProps(context.TODO())
	if err != nil {
		return "", fmt.Errorf("failed to query linstor controller properties: %w", err)
	}

	var id string
	for k, v := range props {
		if k == "Aux/StorkClusterId" {
			id = v
			break
		}
	}

	if id == "" {
		id = randString(16)
		logrus.Debugf("linstor: no cluster ID found, generating new (%s)", id)
		err := cli.Controller.Modify(context.TODO(), lclient.GenericPropsModify{
			OverrideProps: lclient.OverrideProps{
				"Aux/StorkClusterId": id,
			},
		})
		if err != nil {
			return "", fmt.Errorf("failed to set cluster ID property: %w", err)
		}
	}
	return id, nil
}

// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
func (l *linstor) GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error) {
	return nil, nil
}

// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
func (a *linstor) GetCSIPodPrefix() (string, error) {
	return "", &errors.ErrNotSupported{}
}

func init() {
	l := &linstor{}
	if err := storkvolume.Register(storkvolume.LinstorDriverName, l); err != nil {
		logrus.Panicf("Error registering linstor volume driver: %v", err)
	}
}
