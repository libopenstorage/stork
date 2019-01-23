// Package k8sutils provides Kubernetes utility functions that are specific to Portworx
package k8sutils

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	apps_api "k8s.io/api/apps/v1beta2"
	core_api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	replicaMemoryKey         = "px/replicas-before-scale-down"
	deploymentUpdateTimeout  = 5 * time.Minute
	statefulSetUpdateTimeout = 10 * time.Minute
	defaultRetryInterval     = 10 * time.Second
	pxStorageProvisionerName = "kubernetes.io/portworx-volume"
)

// Instance reperesents an instance of k8sutils
type Instance struct {
	kubeClient kubernetes.Interface
	k8sOps     k8s.Ops
}

// New returns a new instance of k8sutils
func New(kubeconfig string) (*Instance, error) {
	var cfg *rest.Config
	var err error

	if len(kubeconfig) == 0 {
		kubeconfig = os.Getenv("KUBECONFIG")
	}

	if len(kubeconfig) > 0 {
		logrus.Debugf("using kubeconfig: %s to create k8s client", kubeconfig)
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		logrus.Debugf("will use in-cluster config to create k8s client")
		cfg, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, fmt.Errorf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Instance{
		kubeClient: kubeClient,
		k8sOps:     k8s.Instance(),
	}, nil
}

// GetPXSharedApps returns all deployments and statefulsets using Portworx storage class for shared volumes
func (k *Instance) GetPXSharedApps() ([]apps_api.Deployment, []apps_api.StatefulSet, error) {
	scs, err := k.GetPXSharedSCs()
	if err != nil {
		return nil, nil, err
	}

	var sharedDeps []apps_api.Deployment
	var sharedSS []apps_api.StatefulSet
	for _, sc := range scs {
		deps, err := k.k8sOps.GetDeploymentsUsingStorageClass(sc)
		if err != nil {
			return nil, nil, err
		}

		for _, d := range deps {
			sharedDeps = append(sharedDeps, d)
		}

		ss, err := k.k8sOps.GetStatefulSetsUsingStorageClass(sc)
		if err != nil {
			return nil, nil, err
		}

		for _, s := range ss {
			sharedSS = append(sharedSS, s)
		}
	}

	return sharedDeps, sharedSS, nil
}

// IsAnyPXAppPodUnmanaged checks if any application pod using PX volumes is not managed by a kubernetes controller
func (k *Instance) IsAnyPXAppPodUnmanaged() (bool, error) {
	pods, err := k.getPodsUsingPX()
	if err != nil {
		return false, fmt.Errorf("failed to get pods using %s. Err: %v", pxStorageProvisionerName, err)
	}

	var unManagedPods []core_api.Pod
	for _, p := range pods {
		if !k.k8sOps.IsPodBeingManaged(p) {
			unManagedPods = append(unManagedPods, p)
		}
	}

	if len(unManagedPods) > 0 {
		logrus.Infof("Following pods are using Portworx and are not being managed by any controller:")
		for _, p := range unManagedPods {
			logrus.Infof("  pod name: %s namespace: %s", p.Name, p.Namespace)
		}

		return true, nil
	}

	return false, nil
}

// GetPXSharedSCs returns all storage classes that have the shared parameter set to true
func (k *Instance) GetPXSharedSCs() ([]string, error) {
	scs, err := k.kubeClient.Storage().StorageClasses().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []string
	for _, sc := range scs.Items {
		params, err := k.k8sOps.GetStorageClassParams(&sc)
		if err != nil {
			return nil, err
		}

		if isShared, present := params["shared"]; present && isShared == "true" {
			retList = append(retList, sc.Name)
		}

	}

	return retList, nil
}

// ScaleSharedAppsToZero scales down PX shared apps to 0 replicas
func (k *Instance) ScaleSharedAppsToZero() error {
	sharedDeps, sharedSS, err := k.GetPXSharedApps()
	if err != nil {
		return err
	}

	logrus.Infof("found %d deployments and %d statefulsets to scale down", len(sharedDeps), len(sharedSS))

	var valZero int32
	for _, d := range sharedDeps {
		deploymentName := d.Name
		deploymentNamespace := d.Namespace
		logrus.Infof("scaling down deployment: [%s] %s", deploymentNamespace, deploymentName)

		t := func() (interface{}, bool, error) {
			dCopy, err := k.kubeClient.Apps().Deployments(deploymentNamespace).Get(deploymentName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return nil, false, nil // done as deployment is deleted
				}

				return nil, true, err
			}

			if *dCopy.Spec.Replicas == 0 {
				logrus.Infof("app [%s] %s is already scaled down to 0", dCopy.Namespace, dCopy.Name)
				return nil, false, nil
			}

			dCopy = dCopy.DeepCopy()

			// save current replica count in annotations so it can be used later on to restore the replicas
			dCopy.Annotations[replicaMemoryKey] = fmt.Sprintf("%d", *dCopy.Spec.Replicas)
			dCopy.Spec.Replicas = &valZero

			_, err = k.kubeClient.Apps().Deployments(dCopy.Namespace).Update(dCopy)
			if err != nil {
				return nil, true, err
			}

			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, deploymentUpdateTimeout, defaultRetryInterval); err != nil {
			return err
		}
	}

	for _, s := range sharedSS {
		logrus.Infof("scaling down statefulset: [%s] %s", s.Namespace, s.Name)

		t := func() (interface{}, bool, error) {
			sCopy, err := k.kubeClient.Apps().StatefulSets(s.Namespace).Get(s.Name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return nil, false, nil // done as statefulset is deleted
				}

				return nil, true, err
			}

			sCopy = sCopy.DeepCopy()
			// save current replica count in annotations so it can be used later on to restore the replicas
			sCopy.Annotations[replicaMemoryKey] = fmt.Sprintf("%d", *sCopy.Spec.Replicas)
			sCopy.Spec.Replicas = &valZero

			_, err = k.kubeClient.Apps().StatefulSets(sCopy.Namespace).Update(sCopy)
			if err != nil {
				return nil, true, err
			}

			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, statefulSetUpdateTimeout, defaultRetryInterval); err != nil {
			return err
		}
	}

	return nil
}

// RestoreScaledAppsReplicas restores PX shared apps that were scaled down to 0 replicas
func (k *Instance) RestoreScaledAppsReplicas() error {
	sharedDeps, sharedSS, err := k.GetPXSharedApps()
	if err != nil {
		return err
	}

	logrus.Infof("found %d deployments and %d statefulsets to restore", len(sharedDeps), len(sharedSS))

	for _, d := range sharedDeps {
		deploymentName := d.Name
		deploymentNamespace := d.Namespace
		logrus.Infof("restoring app: [%s] %s", d.Namespace, d.Name)
		t := func() (interface{}, bool, error) {
			dCopy, err := k.kubeClient.Apps().Deployments(deploymentNamespace).Get(deploymentName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return nil, false, nil // done as deployment is deleted
				}

				return nil, true, err
			}

			dCopy = dCopy.DeepCopy()
			if dCopy.Annotations == nil {
				return nil, false, nil // done as this is not an app we touched
			}

			val, present := dCopy.Annotations[replicaMemoryKey]
			if !present || len(val) == 0 {
				logrus.Infof("not restoring app: [%s] %s as no annotation found to track replica count", deploymentNamespace, deploymentName)
				return nil, false, nil // done as this is not an app we touched
			}

			parsedVal := intstr.Parse(val)
			if parsedVal.Type != intstr.Int {
				return nil, false /*retry won't help */, fmt.Errorf("failed to parse saved replica count: %v", val)
			}

			delete(dCopy.Annotations, replicaMemoryKey)
			dCopy.Spec.Replicas = &parsedVal.IntVal

			_, err = k.kubeClient.Apps().Deployments(dCopy.Namespace).Update(dCopy)
			if err != nil {
				return nil, true, err
			}

			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, deploymentUpdateTimeout, defaultRetryInterval); err != nil {
			return err
		}
	}

	for _, s := range sharedSS {
		logrus.Infof("restoring app: [%s] %s", s.Namespace, s.Name)

		t := func() (interface{}, bool, error) {
			sCopy, err := k.kubeClient.Apps().StatefulSets(s.Namespace).Get(s.Name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return nil, false, nil // done as statefulset is deleted
				}

				return nil, true, err
			}

			sCopy = sCopy.DeepCopy()
			if sCopy.Annotations == nil {
				return nil, false, nil // done as this is not an app we touched
			}

			val, present := sCopy.Annotations[replicaMemoryKey]
			if !present || len(val) == 0 {
				return nil, false, nil // done as this is not an app we touched
			}

			parsedVal := intstr.Parse(val)
			if parsedVal.Type != intstr.Int {
				return nil, false, fmt.Errorf("failed to parse saved replica count: %v", val)
			}

			delete(sCopy.Annotations, replicaMemoryKey)
			sCopy.Spec.Replicas = &parsedVal.IntVal

			_, err = k.kubeClient.Apps().StatefulSets(sCopy.Namespace).Update(sCopy)
			if err != nil {
				return nil, true, err
			}

			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, statefulSetUpdateTimeout, defaultRetryInterval); err != nil {
			return err
		}
	}

	return nil
}

// getPodsUsingPX returns all pods using Portworx on given k8s node name
func (k *Instance) getPodsUsingPX() ([]core_api.Pod, error) {
	pods, err := k.k8sOps.GetPods("")
	if err != nil {
		return nil, err
	}

	pxPods := make([]core_api.Pod, 0)
	for _, pod := range pods.Items {
		usingPX, err := k.isPodUsingPxVolume(pod)
		if err != nil {
			return nil, err
		}

		if usingPX {
			pxPods = append(pxPods, pod)
		}
	}

	return pxPods, nil
}

// isPodUsingPxVolume inspects pod's volumes and checks it was a PX provisioned volume
func (k *Instance) isPodUsingPxVolume(pod core_api.Pod) (bool, error) {
	for _, vol := range pod.Spec.Volumes {
		// check if pod uses a PX PV directly
		if vol.PortworxVolume != nil {
			return true, nil
		}

		if vol.PersistentVolumeClaim != nil {
			pvc, err := k.k8sOps.GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
			if err != nil {
				return false, err
			}

			// check if pvc is using a PX pre-provisioned volume
			pv, err := k.k8sOps.GetPersistentVolume(pvc.Spec.VolumeName)
			if err != nil {
				return false, err
			}

			if pv.Spec.PortworxVolume != nil {
				return true, nil
			}

			// check if pvc is using a PX storage class
			scName, err := k.k8sOps.GetStorageProvisionerForPVC(pvc)
			if err != nil {
				if strings.Contains(err.Error(), "does not have a storage class") {
					continue
				}

				return false, err
			}

			if scName == pxStorageProvisionerName {
				return true, nil
			}
		}
	}
	return false, nil
}
