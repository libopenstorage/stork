package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/heptio/ark/pkg/discovery"
	"github.com/heptio/ark/pkg/util/collections"
	"github.com/libopenstorage/stork/drivers/volume"
	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

const (
	resyncPeriod                     = 30
	storkMigrationReplicasAnnotation = "stork.openstorage.org/migrationReplicas"
)

// MigrationController migrationcontroller
type MigrationController struct {
	Driver            volume.Driver
	Recorder          record.EventRecorder
	discoveryHelper   discovery.Helper
	dynamicClientPool dynamic.ClientPool
}

// Init Initialize the migration controller
func (m *MigrationController) Init() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("Error getting cluster config: %v", err)
	}

	aeclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Error getting apiextention client, %v", err)
	}

	err = m.createCRD()
	if err != nil {
		return err
	}

	discoveryClient := aeclient.Discovery()
	m.discoveryHelper, err = discovery.NewHelper(discoveryClient, logrus.New())
	if err != nil {
		return err
	}
	err = m.discoveryHelper.Refresh()
	if err != nil {
		return err
	}
	m.dynamicClientPool = dynamic.NewDynamicClientPool(config)

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork.Version,
			Kind:    reflect.TypeOf(storkv1.Migration{}).Name(),
		},
		"",
		resyncPeriod,
		m)
}

// Handle updates for Migration objects
func (m *MigrationController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *storkv1.Migration:
		migration := o
		if event.Deleted {
			return m.Driver.CancelMigration(migration)
		}

		if migration.Spec.ClusterPair == "" {
			err := fmt.Errorf("clusterPair to migrate to cannot be empty")
			logrus.Errorf(err.Error())
			m.Recorder.Event(migration,
				v1.EventTypeWarning,
				string(storkv1.MigrationStatusFailed),
				err.Error())
			return err
		}

		switch migration.Status.Stage {

		case storkv1.MigrationStageInitial,
			storkv1.MigrationStageVolumes:
			err := m.migrateVolumes(migration)
			if err != nil {
				message := fmt.Sprintf("Error migrating volumes: %v", err)
				logrus.Errorf(message)
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(storkv1.MigrationStatusFailed),
					message)
				return err
			}
		case storkv1.MigrationStageApplications:
			err := m.migrateResources(migration)
			if err != nil {
				message := fmt.Sprintf("Error migrating resources: %v", err)
				logrus.Errorf(message)
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(storkv1.MigrationStatusFailed),
					message)
				return err
			}

		case storkv1.MigrationStageFinal:
			// Do Nothing
			return nil
		default:
			logrus.Errorf("Invalid stage for migration: %v", migration.Status.Stage)
		}
	}
	return nil
}

func (m *MigrationController) migrateVolumes(migration *storkv1.Migration) error {
	storageStatus, err := getClusterPairStorageStatus(migration.Spec.ClusterPair)
	if err != nil {
		return err
	}

	if storageStatus != storkv1.ClusterPairStatusReady {
		return fmt.Errorf("Storage Cluster pair is not ready. Status: %v", storageStatus)
	}

	migration.Status.Stage = storkv1.MigrationStageVolumes
	// Trigger the migration if we don't have any status
	if migration.Status.Volumes == nil {
		volumeInfos, err := m.Driver.StartMigration(migration)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*storkv1.VolumeInfo, 0)
		}
		migration.Status.Volumes = volumeInfos
		migration.Status.Status = storkv1.MigrationStatusInProgress
		err = sdk.Update(migration)
		if err != nil {
			return err
		}
	}

	// Now check the status
	volumeInfos, err := m.Driver.GetMigrationStatus(migration)
	if err != nil {
		return err
	}
	if volumeInfos == nil {
		volumeInfos = make([]*storkv1.VolumeInfo, 0)
	}
	migration.Status.Volumes = volumeInfos
	// Store the new status
	err = sdk.Update(migration)
	if err != nil {
		return err
	}

	// Now check if there is any failure or success
	// TODO: On failure of one volume cancel other migrations?
	for _, vInfo := range volumeInfos {
		// Return if we have any volume migrations still in progress
		if vInfo.Status == storkv1.MigrationStatusInProgress {
			logrus.Infof("Volume Migration still in progress: %v", migration.Name)
			return nil
		} else if vInfo.Status == storkv1.MigrationStatusFailed {
			m.Recorder.Event(migration,
				v1.EventTypeWarning,
				string(vInfo.Status),
				fmt.Sprintf("Error migrating volume %v: %v", vInfo.Volume, vInfo.Reason))
			migration.Status.Stage = storkv1.MigrationStageFinal
			migration.Status.Status = storkv1.MigrationStatusFailed
		} else if vInfo.Status == storkv1.MigrationStatusSuccessful {
			m.Recorder.Event(migration,
				v1.EventTypeNormal,
				string(vInfo.Status),
				fmt.Sprintf("Volume %v migrated successfuly", vInfo.Volume))
		}
	}

	// If the migration hasn't failed move on to the next stage.
	if migration.Status.Status != storkv1.MigrationStatusFailed {
		if migration.Spec.IncludeResources {
			migration.Status.Stage = storkv1.MigrationStageApplications
			migration.Status.Status = storkv1.MigrationStatusInProgress
			// Update the current state and then move on to migrating
			// resources
			err = sdk.Update(migration)
			if err != nil {
				return err
			}
			err = m.migrateResources(migration)
			if err != nil {
				logrus.Errorf("Error migrating resources: %v", err)
				return err
			}
		}
		migration.Status.Stage = storkv1.MigrationStageFinal
		migration.Status.Status = storkv1.MigrationStatusSuccessful
	}

	err = sdk.Update(migration)
	if err != nil {
		return err
	}
	return nil
}

func resourceToBeMigrated(migration *storkv1.Migration, resource metav1.APIResource) bool {
	// Deployment is present in "apps" and "extensions" group, so ignore
	// "extensions"
	if resource.Group == "extensions" && resource.Kind == "Deployment" {
		return false
	}

	switch resource.Kind {
	case "PersistentVolumeClaim",
		"PersistentVolume",
		"Deployment",
		"StatefulSet",
		"ConfigMap",
		"Service",
		"Secret":
		return true
	default:
		return false
	}
}

func objectToBeMigrated(
	resourceMap map[types.UID]bool,
	object runtime.Unstructured,
	namespace string,
) (bool, error) {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return false, err
	}
	if _, ok := resourceMap[metadata.GetUID()]; ok {
		return false, nil
	}
	objectType, err := meta.TypeAccessor(object)
	if err != nil {
		return false, err
	}

	switch objectType.GetKind() {
	case "Service":
		// Don't migrate the kubernetes service
		metadata, err := meta.Accessor(object)
		if err != nil {
			return false, err
		}
		if metadata.GetName() == "kubernetes" {
			return false, nil
		}
	case "PersistentVolume":
		spec, err := collections.GetMap(object.UnstructuredContent(), "spec.claimRef")
		if err != nil {
			return false, err
		}
		if spec["namespace"] == namespace {
			return true, nil
		}
		return false, nil
	case "Secret":
		secretType, err := collections.GetString(object.UnstructuredContent(), "type")
		if err != nil {
			return false, err
		}
		if secretType == string(v1.SecretTypeServiceAccountToken) {
			return false, nil
		}
	}

	return true, nil
}

func (m *MigrationController) migrateResources(migration *storkv1.Migration) error {
	schedulerStatus, err := getClusterPairSchedulerStatus(migration.Spec.ClusterPair)
	if err != nil {
		return err
	}

	if schedulerStatus != storkv1.ClusterPairStatusReady {
		return fmt.Errorf("Scheduler Cluster pair is not ready. Status: %v", schedulerStatus)
	}

	allObjects, err := m.getResources(migration)
	if err != nil {
		logrus.Errorf("Error getting resources: %v", err)
		return err
	}

	err = m.prepareResources(migration, allObjects)
	if err != nil {
		m.Recorder.Event(migration,
			v1.EventTypeWarning,
			string(storkv1.MigrationStatusFailed),
			fmt.Sprintf("Error preparing resource: %v", err))
		logrus.Errorf("Error preparing resources: %v", err)
		return err
	}
	err = m.applyResources(migration, allObjects)
	if err != nil {
		m.Recorder.Event(migration,
			v1.EventTypeWarning,
			string(storkv1.MigrationStatusFailed),
			fmt.Sprintf("Error applying resource: %v", err))
		logrus.Errorf("Error applying resources: %v", err)
		return err
	}

	migration.Status.Stage = storkv1.MigrationStageFinal
	migration.Status.Status = storkv1.MigrationStatusSuccessful
	for _, resource := range migration.Status.Resources {
		if resource.Status != storkv1.MigrationStatusSuccessful {
			migration.Status.Status = storkv1.MigrationStatusPartialSuccess
			break
		}
	}
	err = sdk.Update(migration)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationController) getResources(
	migration *storkv1.Migration,
) ([]runtime.Unstructured, error) {
	err := m.discoveryHelper.Refresh()
	if err != nil {
		return nil, err
	}
	allObjects := make([]runtime.Unstructured, 0)
	resourceInfos := make([]*storkv1.ResourceInfo, 0)

	for _, group := range m.discoveryHelper.Resources() {
		groupVersion, err := schema.ParseGroupVersion(group.GroupVersion)
		if err != nil {
			return nil, err
		}
		if groupVersion.Group == "extensions" {
			continue
		}

		resourceMap := make(map[types.UID]bool)
		for _, resource := range group.APIResources {
			if !resourceToBeMigrated(migration, resource) {
				continue
			}

			for _, ns := range migration.Spec.Namespaces {
				dynamicClient, err := m.dynamicClientPool.ClientForGroupVersionKind(groupVersion.WithKind(""))
				if err != nil {
					return nil, err
				}
				client := dynamicClient.Resource(&resource, ns)

				objectsList, err := client.List(metav1.ListOptions{})
				if err != nil {
					return nil, err
				}
				objects, err := meta.ExtractList(objectsList)
				if err != nil {
					return nil, err
				}
				for _, o := range objects {
					runtimeObject, ok := o.(runtime.Unstructured)
					if !ok {
						return nil, fmt.Errorf("Error casting object: %v", o)
					}

					migrate, err := objectToBeMigrated(resourceMap, runtimeObject, ns)
					if err != nil {
						return nil, fmt.Errorf("Error processing object %v: %v", runtimeObject, err)
					}
					if !migrate {
						continue
					}
					metadata, err := meta.Accessor(runtimeObject)
					if err != nil {
						return nil, err
					}
					resourceInfo := &storkv1.ResourceInfo{
						Name:      metadata.GetName(),
						Namespace: metadata.GetNamespace(),
						Status:    storkv1.MigrationStatusInProgress,
					}
					resourceInfo.Kind = resource.Kind
					resourceInfo.Group = groupVersion.Group
					// core Group doesn't have a name, so override it
					if resourceInfo.Group == "" {
						resourceInfo.Group = "core"
					}
					resourceInfo.Version = groupVersion.Version
					resourceInfos = append(resourceInfos, resourceInfo)
					allObjects = append(allObjects, runtimeObject)
					resourceMap[metadata.GetUID()] = true
				}
			}
		}
		migration.Status.Resources = resourceInfos
		err = sdk.Update(migration)
		if err != nil {
			return nil, err
		}
	}

	return allObjects, nil
}

func (m *MigrationController) prepareResources(
	migration *storkv1.Migration,
	objects []runtime.Unstructured,
) error {
	for _, o := range objects {
		content := o.UnstructuredContent()
		// Status shouldn't be migrated between clusters
		delete(content, "status")

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			updatedObject, err := m.preparePVResource(migration, o)
			if err != nil {
				m.updateResourceStatus(
					migration,
					o,
					storkv1.MigrationStatusFailed,
					fmt.Sprintf("Error preparing PV resource: %v", err))
				continue
			}
			o = updatedObject
		case "Deployment", "StatefulSet":
			updatedObject, err := m.prepareApplicationResource(migration, o)
			if err != nil {
				m.updateResourceStatus(
					migration,
					o,
					storkv1.MigrationStatusFailed,
					fmt.Sprintf("Error preparing Application resource: %v", err))
				continue
			}
			o = updatedObject
		}
		metadata, err := collections.GetMap(content, "metadata")
		if err != nil {
			m.updateResourceStatus(
				migration,
				o,
				storkv1.MigrationStatusFailed,
				fmt.Sprintf("Error getting metadata for resource: %v", err))
			continue
		}
		for key := range metadata {
			switch key {
			case "name", "namespace", "labels", "annotations":
			default:
				delete(metadata, key)
			}
		}
	}
	return nil
}

func (m *MigrationController) updateResourceStatus(
	migration *storkv1.Migration,
	object runtime.Unstructured,
	status storkv1.MigrationStatusType,
	reason string,
) {
	for _, resource := range migration.Status.Resources {
		metadata, err := meta.Accessor(object)
		if err != nil {
			continue
		}
		gkv := object.GetObjectKind().GroupVersionKind()
		if resource.Name == metadata.GetName() &&
			resource.Namespace == metadata.GetNamespace() &&
			(resource.Group == gkv.Group || (resource.Group == "core" && gkv.Group == "")) &&
			resource.Version == gkv.Version &&
			resource.Kind == gkv.Kind {
			resource.Status = status
			resource.Reason = reason
			eventType := v1.EventTypeNormal
			if status == storkv1.MigrationStatusFailed {
				eventType = v1.EventTypeWarning
			}
			eventMessage := fmt.Sprintf("%v %v/%v: %v",
				gkv,
				resource.Namespace,
				resource.Name,
				reason)
			m.Recorder.Event(migration, eventType, string(status), eventMessage)
			return
		}
	}
}

func (m *MigrationController) preparePVResource(
	migration *storkv1.Migration,
	object runtime.Unstructured,
) (runtime.Unstructured, error) {
	spec, err := collections.GetMap(object.UnstructuredContent(), "spec")
	if err != nil {
		return nil, err
	}
	delete(spec, "claimRef")
	delete(spec, "storageClassName")

	return m.Driver.UpdateMigratedPersistentVolumeSpec(object)
}

func (m *MigrationController) prepareApplicationResource(
	migration *storkv1.Migration,
	object runtime.Unstructured,
) (runtime.Unstructured, error) {
	if migration.Spec.StartApplications {
		return object, nil
	}

	// Reset the replicas to 0 and store the current replicas in an annotation
	content := object.UnstructuredContent()
	spec, err := collections.GetMap(content, "spec")
	if err != nil {
		return nil, err
	}
	replicas := spec["replicas"].(int64)
	annotations, err := collections.GetMap(content, "metadata.annotations")
	annotations[storkMigrationReplicasAnnotation] = strconv.FormatInt(replicas, 10)
	spec["replicas"] = 0
	return object, nil
}

func (m *MigrationController) applyResources(
	migration *storkv1.Migration,
	objects []runtime.Unstructured,
) error {
	remoteConfig, err := getClusterPairSchedulerConfig(migration.Spec.ClusterPair)
	if err != nil {
		return err
	}

	client, err := kubernetes.NewForConfig(remoteConfig)
	if err != nil {
		return err
	}

	// First make sure all the namespaces are created on the
	// remote cluster
	for _, ns := range migration.Spec.Namespaces {
		namespace, err := k8s.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}
		_, err = client.CoreV1().Namespaces().Create(&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   namespace.Name,
				Labels: namespace.Labels,
			},
		})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	remoteDynamicClientPool := dynamic.NewDynamicClientPool(remoteConfig)
	for _, o := range objects {
		dynamicClient, err := remoteDynamicClientPool.ClientForGroupVersionKind(o.GetObjectKind().GroupVersionKind())
		if err != nil {
			return err
		}
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}
		logrus.Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
		resource := &metav1.APIResource{
			Name:       strings.ToLower(objectType.GetKind()) + "s",
			Namespaced: len(metadata.GetNamespace()) > 0,
		}
		client := dynamicClient.Resource(resource, metadata.GetNamespace())
		unstructured, ok := o.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("Unable to cast object to unstructured: %v", o)
		}
		_, err = client.Create(unstructured)
		if err != nil && apierrors.IsAlreadyExists(err) {
			switch objectType.GetKind() {
			// Don't want to delete the Volume resources
			case "PersistentVolumeClaim", "PersistentVolume":
				err = nil
			default:
				// Delete the resource if it already exists on the destination
				// cluster and try creating again
				err = client.Delete(metadata.GetName(), &metav1.DeleteOptions{})
				if err == nil {
					_, err = client.Create(unstructured)
				} else {
					logrus.Errorf("Error deleting %v %v during migrate: %v", objectType.GetKind(), metadata.GetName(), err)
				}
			}

		}
		if err != nil {
			m.updateResourceStatus(
				migration,
				o,
				storkv1.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err))
		} else {
			m.updateResourceStatus(
				migration,
				o,
				storkv1.MigrationStatusSuccessful,
				"Resource migrated successfully")
		}
	}
	return nil
}

func (m *MigrationController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    storkv1.MigrationResourceName,
		Plural:  storkv1.MigrationResourcePlural,
		Group:   stork.GroupName,
		Version: stork.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkv1.Migration{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource)
}
