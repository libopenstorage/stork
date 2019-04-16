package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/heptio/ark/pkg/util/collections"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
)

const (
	resyncPeriod = 30 * time.Second
	// StorkMigrationReplicasAnnotation is the annotation used to keep track of
	// the number of replicas for an application when it was migrated
	StorkMigrationReplicasAnnotation = "stork.libopenstorage.org/migrationReplicas"
)

// MigrationController reconciles migration objects
type MigrationController struct {
	Driver                  volume.Driver
	Recorder                record.EventRecorder
	ResourceCollector       resourcecollector.ResourceCollector
	migrationAdminNamespace string
}

// Init Initialize the migration controller
func (m *MigrationController) Init(migrationAdminNamespace string) error {
	err := m.createCRD()
	if err != nil {
		return err
	}

	m.migrationAdminNamespace = migrationAdminNamespace
	if err := m.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for migration rules: %v", err)
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.Migration{}).Name(),
		},
		"",
		resyncPeriod,
		m)
}

func setKind(snap *stork_api.Migration) {
	snap.Kind = "Migration"
	snap.APIVersion = stork_api.SchemeGroupVersion.String()
}

// performRuleRecovery terminates potential background commands running pods for
// all migration objects
func (m *MigrationController) performRuleRecovery() error {
	migrations, err := k8s.Instance().ListMigrations(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all migrations during rule recovery: %v", err)
		return err
	}

	if migrations == nil {
		return nil
	}

	var lastError error
	for _, migration := range migrations.Items {
		setKind(&migration)
		err := rule.PerformRuleRecovery(&migration)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}

func setDefaults(migration *stork_api.Migration) *stork_api.Migration {
	if migration.Spec.IncludeVolumes == nil {
		defaultBool := true
		migration.Spec.IncludeVolumes = &defaultBool
	}
	if migration.Spec.IncludeResources == nil {
		defaultBool := true
		migration.Spec.IncludeResources = &defaultBool
	}
	if migration.Spec.StartApplications == nil {
		defaultBool := false
		migration.Spec.StartApplications = &defaultBool
	}
	return migration
}

// Handle updates for Migration objects
func (m *MigrationController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.Migration:
		migration := o
		if event.Deleted {
			return m.Driver.CancelMigration(migration)
		}
		migration = setDefaults(migration)

		if migration.Spec.ClusterPair == "" {
			err := fmt.Errorf("clusterPair to migrate to cannot be empty")
			log.MigrationLog(migration).Errorf(err.Error())
			m.Recorder.Event(migration,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				err.Error())
			return nil
		}

		// Check whether namespace is allowed to be migrated before each stage
		// Restrict migration to only the namespace that the object belongs
		// except for the namespace designated by the admin
		if !m.namespaceMigrationAllowed(migration) {
			err := fmt.Errorf("Spec.Namespaces should only contain the current namespace")
			log.MigrationLog(migration).Errorf(err.Error())
			m.Recorder.Event(migration,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				err.Error())
			return nil
		}

		var terminationChannels []chan bool
		var err error

		switch migration.Status.Stage {
		case stork_api.MigrationStageInitial:
			// Make sure the namespaces exist
			for _, ns := range migration.Spec.Namespaces {
				_, err := k8s.Instance().GetNamespace(ns)
				if err != nil {
					migration.Status.Status = stork_api.MigrationStatusFailed
					migration.Status.Stage = stork_api.MigrationStageFinal
					migration.Status.FinishTimestamp = metav1.Now()
					err = fmt.Errorf("error getting namespace %v: %v", ns, err)
					log.MigrationLog(migration).Errorf(err.Error())
					m.Recorder.Event(migration,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						err.Error())
					err = sdk.Update(migration)
					if err != nil {
						log.MigrationLog(migration).Errorf("Error updating")
					}
					return nil
				}
			}
			// Make sure the rules exist if configured
			if migration.Spec.PreExecRule != "" {
				_, err := k8s.Instance().GetRule(migration.Spec.PreExecRule, migration.Namespace)
				if err != nil {
					message := fmt.Sprintf("Error getting PreExecRule %v: %v", migration.Spec.PreExecRule, err)
					log.MigrationLog(migration).Errorf(message)
					m.Recorder.Event(migration,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						message)
					return nil
				}
			}
			if migration.Spec.PostExecRule != "" {
				_, err := k8s.Instance().GetRule(migration.Spec.PostExecRule, migration.Namespace)
				if err != nil {
					message := fmt.Sprintf("Error getting PostExecRule %v: %v", migration.Spec.PreExecRule, err)
					log.MigrationLog(migration).Errorf(message)
					m.Recorder.Event(migration,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						message)
					return nil
				}
			}
			fallthrough
		case stork_api.MigrationStagePreExecRule:
			terminationChannels, err = m.runPreExecRule(migration)
			if err != nil {
				message := fmt.Sprintf("Error running PreExecRule: %v", err)
				log.MigrationLog(migration).Errorf(message)
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)
				migration.Status.Stage = stork_api.MigrationStageInitial
				migration.Status.Status = stork_api.MigrationStatusInitial
				err := sdk.Update(migration)
				if err != nil {
					return err
				}
				return nil
			}
			fallthrough
		case stork_api.MigrationStageVolumes:
			if *migration.Spec.IncludeVolumes {
				err := m.migrateVolumes(migration, terminationChannels)
				if err != nil {
					message := fmt.Sprintf("Error migrating volumes: %v", err)
					log.MigrationLog(migration).Errorf(message)
					m.Recorder.Event(migration,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						message)
					return nil
				}
			} else {
				migration.Status.Stage = stork_api.MigrationStageApplications
				migration.Status.Status = stork_api.MigrationStatusInitial
				err := sdk.Update(migration)
				if err != nil {
					return err
				}
			}
		case stork_api.MigrationStageApplications:
			err := m.migrateResources(migration)
			if err != nil {
				message := fmt.Sprintf("Error migrating resources: %v", err)
				log.MigrationLog(migration).Errorf(message)
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)
				return nil
			}

		case stork_api.MigrationStageFinal:
			// Do Nothing
			return nil
		default:
			log.MigrationLog(migration).Errorf("Invalid stage for migration: %v", migration.Status.Stage)
		}
	}
	return nil
}

func (m *MigrationController) namespaceMigrationAllowed(migration *stork_api.Migration) bool {
	// Restrict migration to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if migration.Namespace != m.migrationAdminNamespace {
		for _, ns := range migration.Spec.Namespaces {
			if ns != migration.Namespace {
				return false
			}
		}
	}
	return true
}

func (m *MigrationController) migrateVolumes(migration *stork_api.Migration, terminationChannels []chan bool) error {
	defer func() {
		for _, channel := range terminationChannels {
			channel <- true
		}
	}()

	migration.Status.Stage = stork_api.MigrationStageVolumes
	// Trigger the migration if we don't have any status
	if migration.Status.Volumes == nil {
		// Make sure storage is ready in the cluster pair
		storageStatus, err := getClusterPairStorageStatus(
			migration.Spec.ClusterPair,
			migration.Namespace)
		if err != nil || storageStatus != stork_api.ClusterPairStatusReady {
			// If there was a preExecRule configured, reset the stage so that it
			// gets retriggered in the next cycle
			if migration.Spec.PreExecRule != "" {
				migration.Status.Stage = stork_api.MigrationStageInitial
				err := sdk.Update(migration)
				if err != nil {
					return err
				}
			}
			return fmt.Errorf("cluster pair storage status is not ready. Status: %v Err: %v",
				storageStatus, err)
		}

		volumeInfos, err := m.Driver.StartMigration(migration)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.VolumeInfo, 0)
		}
		migration.Status.Volumes = volumeInfos
		migration.Status.Status = stork_api.MigrationStatusInProgress
		err = sdk.Update(migration)
		if err != nil {
			return err
		}

		// Terminate any background rules that were started
		for _, channel := range terminationChannels {
			channel <- true
		}
		terminationChannels = nil

		// Run any post exec rules once migration is triggered
		if migration.Spec.PostExecRule != "" {
			err = m.runPostExecRule(migration)
			if err != nil {
				message := fmt.Sprintf("Error running PostExecRule: %v", err)
				log.MigrationLog(migration).Errorf(message)
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)

				// Cancel the migration and mark it as failed if the postExecRule failed
				err = m.Driver.CancelMigration(migration)
				if err != nil {
					log.MigrationLog(migration).Errorf("Error cancelling migration: %v", err)
				}
				migration.Status.Stage = stork_api.MigrationStageFinal
				migration.Status.FinishTimestamp = metav1.Now()
				migration.Status.Status = stork_api.MigrationStatusFailed
				err = sdk.Update(migration)
				if err != nil {
					return err
				}
				return fmt.Errorf("%v", message)
			}
		}
	}

	inProgress := false
	// Skip checking status if no volumes are being migrated
	if len(migration.Status.Volumes) != 0 {
		// Now check the status
		volumeInfos, err := m.Driver.GetMigrationStatus(migration)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.VolumeInfo, 0)
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
			if vInfo.Status == stork_api.MigrationStatusInProgress {
				log.MigrationLog(migration).Infof("Volume migration still in progress: %v", vInfo.Volume)
				inProgress = true
			} else if vInfo.Status == stork_api.MigrationStatusFailed {
				m.Recorder.Event(migration,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error migrating volume %v: %v", vInfo.Volume, vInfo.Reason))
				migration.Status.Stage = stork_api.MigrationStageFinal
				migration.Status.FinishTimestamp = metav1.Now()
				migration.Status.Status = stork_api.MigrationStatusFailed
			} else if vInfo.Status == stork_api.MigrationStatusSuccessful {
				m.Recorder.Event(migration,
					v1.EventTypeNormal,
					string(vInfo.Status),
					fmt.Sprintf("Volume %v migrated successfully", vInfo.Volume))
			}
		}
	}

	// Return if we have any volume migrations still in progress
	if inProgress {
		return nil
	}

	// If the migration hasn't failed move on to the next stage.
	if migration.Status.Status != stork_api.MigrationStatusFailed {
		if *migration.Spec.IncludeResources {
			migration.Status.Stage = stork_api.MigrationStageApplications
			migration.Status.Status = stork_api.MigrationStatusInProgress
			// Update the current state and then move on to migrating
			// resources
			err := sdk.Update(migration)
			if err != nil {
				return err
			}
			err = m.migrateResources(migration)
			if err != nil {
				log.MigrationLog(migration).Errorf("Error migrating resources: %v", err)
				return err
			}
		} else {
			migration.Status.Stage = stork_api.MigrationStageFinal
			migration.Status.FinishTimestamp = metav1.Now()
			migration.Status.Status = stork_api.MigrationStatusSuccessful
		}
	}

	err := sdk.Update(migration)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationController) runPreExecRule(migration *stork_api.Migration) ([]chan bool, error) {
	if migration.Spec.PreExecRule == "" {
		migration.Status.Stage = stork_api.MigrationStageVolumes
		migration.Status.Status = stork_api.MigrationStatusPending
		err := sdk.Update(migration)
		if err != nil {
			return nil, err
		}
		return nil, nil
	} else if migration.Status.Stage == stork_api.MigrationStageInitial {
		migration.Status.Stage = stork_api.MigrationStagePreExecRule
		migration.Status.Status = stork_api.MigrationStatusPending
	}

	if migration.Status.Stage == stork_api.MigrationStagePreExecRule {
		if migration.Status.Status == stork_api.MigrationStatusPending {
			migration.Status.Status = stork_api.MigrationStatusInProgress
			err := sdk.Update(migration)
			if err != nil {
				return nil, err
			}
		} else if migration.Status.Status == stork_api.MigrationStatusInProgress {
			m.Recorder.Event(migration,
				v1.EventTypeNormal,
				string(stork_api.MigrationStatusInProgress),
				fmt.Sprintf("Waiting for PreExecRule %v", migration.Spec.PreExecRule))
			return nil, nil
		}
	}
	terminationChannels := make([]chan bool, 0)
	for _, ns := range migration.Spec.Namespaces {
		r, err := k8s.Instance().GetRule(migration.Spec.PreExecRule, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, err
		}

		ch, err := rule.ExecuteRule(r, rule.PreExecRule, migration, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
		if ch != nil {
			terminationChannels = append(terminationChannels, ch)
		}
	}
	return terminationChannels, nil
}

func (m *MigrationController) runPostExecRule(migration *stork_api.Migration) error {
	for _, ns := range migration.Spec.Namespaces {
		r, err := k8s.Instance().GetRule(migration.Spec.PostExecRule, ns)
		if err != nil {
			return err
		}

		_, err = rule.ExecuteRule(r, rule.PostExecRule, migration, ns)
		if err != nil {
			return fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
	}
	return nil
}

func (m *MigrationController) migrateResources(migration *stork_api.Migration) error {
	schedulerStatus, err := getClusterPairSchedulerStatus(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return err
	}

	if schedulerStatus != stork_api.ClusterPairStatusReady {
		return fmt.Errorf("scheduler Cluster pair is not ready. Status: %v", schedulerStatus)
	}

	allObjects, err := m.ResourceCollector.GetResources(migration.Spec.Namespaces, migration.Spec.Selectors)
	if err != nil {
		m.Recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error getting resource: %v", err))
		log.MigrationLog(migration).Errorf("Error getting resources: %v", err)
		return err
	}

	// Save the collected resources infos in the status
	resourceInfos := make([]*stork_api.ResourceInfo, 0)
	for _, obj := range allObjects {
		metadata, err := meta.Accessor(obj)
		if err != nil {
			return err
		}

		resourceInfo := &stork_api.ResourceInfo{
			Name:      metadata.GetName(),
			Namespace: metadata.GetNamespace(),
			Status:    stork_api.MigrationStatusInProgress,
		}
		gvk := obj.GetObjectKind().GroupVersionKind()
		resourceInfo.Kind = gvk.Kind
		resourceInfo.Group = gvk.Group
		// core Group doesn't have a name, so override it
		if resourceInfo.Group == "" {
			resourceInfo.Group = "core"
		}
		resourceInfo.Version = gvk.Version

		resourceInfos = append(resourceInfos, resourceInfo)
	}
	migration.Status.Resources = resourceInfos
	err = sdk.Update(migration)
	if err != nil {
		return err
	}

	err = m.prepareResources(migration, allObjects)
	if err != nil {
		m.Recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error preparing resource: %v", err))
		log.MigrationLog(migration).Errorf("Error preparing resources: %v", err)
		return err
	}
	err = m.applyResources(migration, allObjects)
	if err != nil {
		m.Recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error applying resource: %v", err))
		log.MigrationLog(migration).Errorf("Error applying resources: %v", err)
		return err
	}

	migration.Status.Stage = stork_api.MigrationStageFinal
	migration.Status.FinishTimestamp = metav1.Now()
	migration.Status.Status = stork_api.MigrationStatusSuccessful
	for _, resource := range migration.Status.Resources {
		if resource.Status != stork_api.MigrationStatusSuccessful {
			migration.Status.Status = stork_api.MigrationStatusPartialSuccess
			break
		}
	}
	err = sdk.Update(migration)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationController) prepareResources(
	migration *stork_api.Migration,
	objects []runtime.Unstructured,
) error {
	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			err := m.preparePVResource(o)
			if err != nil {
				return fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		case "Deployment", "StatefulSet":
			err := m.prepareApplicationResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing %v resource %v: %v", o.GetObjectKind().GroupVersionKind().Kind, metadata.GetName(), err)
			}
		}
	}
	return nil
}

func (m *MigrationController) updateResourceStatus(
	migration *stork_api.Migration,
	object runtime.Unstructured,
	status stork_api.MigrationStatusType,
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
			if status == stork_api.MigrationStatusFailed {
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
	object runtime.Unstructured,
) error {
	_, err := m.Driver.UpdateMigratedPersistentVolumeSpec(object)
	return err
}

func (m *MigrationController) prepareApplicationResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	if *migration.Spec.StartApplications {
		return nil
	}

	// Reset the replicas to 0 and store the current replicas in an annotation
	content := object.UnstructuredContent()
	spec, err := collections.GetMap(content, "spec")
	if err != nil {
		return err
	}
	replicas := spec["replicas"].(int64)
	annotations, err := collections.GetMap(content, "metadata.annotations")
	if err != nil {
		return err
	}

	annotations[StorkMigrationReplicasAnnotation] = strconv.FormatInt(replicas, 10)
	spec["replicas"] = 0
	return nil
}

func (m *MigrationController) applyResources(
	migration *stork_api.Migration,
	objects []runtime.Unstructured,
) error {
	remoteConfig, err := getClusterPairSchedulerConfig(migration.Spec.ClusterPair, migration.Namespace)
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

		// Don't create if the namespace already exists on the remote cluster
		_, err = client.CoreV1().Namespaces().Get(namespace.Name, metav1.GetOptions{})
		if err == nil {
			continue
		}

		_, err = client.CoreV1().Namespaces().Create(&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        namespace.Name,
				Labels:      namespace.Labels,
				Annotations: namespace.Annotations,
			},
		})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	remoteDynamicInterface, err := dynamic.NewForConfig(remoteConfig)
	if err != nil {
		return nil
	}
	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}
		resource := &metav1.APIResource{
			Name:       strings.ToLower(objectType.GetKind()) + "s",
			Namespaced: len(metadata.GetNamespace()) > 0,
		}
		dynamicClient := remoteDynamicInterface.Resource(
			o.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(metadata.GetNamespace())

		log.MigrationLog(migration).Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
		unstructured, ok := o.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("unable to cast object to unstructured: %v", o)
		}
		_, err = dynamicClient.Create(unstructured)
		if err != nil && (apierrors.IsAlreadyExists(err) || strings.Contains(err.Error(), portallocator.ErrAllocated.Error())) {
			switch objectType.GetKind() {
			// Don't want to delete the Volume resources
			case "PersistentVolumeClaim", "PersistentVolume":
				err = nil
			default:
				// Delete the resource if it already exists on the destination
				// cluster and try creating again
				err = dynamicClient.Delete(metadata.GetName(), &metav1.DeleteOptions{})
				if err == nil {
					_, err = dynamicClient.Create(unstructured)
				} else {
					log.MigrationLog(migration).Errorf("Error deleting %v %v during migrate: %v", objectType.GetKind(), metadata.GetName(), err)
				}
			}

		}
		if err != nil {
			m.updateResourceStatus(
				migration,
				o,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err))
		} else {
			m.updateResourceStatus(
				migration,
				o,
				stork_api.MigrationStatusSuccessful,
				"Resource migrated successfully")
		}
	}
	return nil
}

func (m *MigrationController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.MigrationResourceName,
		Plural:  stork_api.MigrationResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.Migration{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
