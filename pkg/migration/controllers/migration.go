package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/inflect"
	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/mitchellh/hashstructure"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// StorkMigrationReplicasAnnotation is the annotation used to keep track of
	// the number of replicas for an application when it was migrated
	StorkMigrationReplicasAnnotation = "stork.libopenstorage.org/migrationReplicas"
	// StorkMigrationAnnotation is the annotation used to keep track of resources
	// migrated by stork
	StorkMigrationAnnotation = "stork.libopenstorage.org/migrated"
	// StorkMigrationName is the annotation used to identify resource migrated by
	// migration CRD name
	StorkMigrationName = "stork.libopenstorage.org/migrationName"
	// StorkMigrationTime is the annotation used to specify time of migration
	StorkMigrationTime = "stork.libopenstorage.org/migrationTime"
	// StorkMigrationCRDActivateAnnotation is the annotation used to keep track of
	// the value to be set for activating crds
	StorkMigrationCRDActivateAnnotation = "stork.libopenstorage.org/migrationCRDActivate"
	// StorkMigrationCRDDeactivateAnnotation is the annotation used to keep track of
	// the value to be set for deactivating crds
	StorkMigrationCRDDeactivateAnnotation = "stork.libopenstorage.org/migrationCRDDeactivate"
	// storageClassAnnotation for pvc sc
	storageClassAnnotation = "volume.beta.kubernetes.io/storage-class"
	// PVReclaimAnnotation for pvc's reclaim policy
	PVReclaimAnnotation = "stork.libopenstorage.org/reclaimPolicy"

	// Max number of times to retry applying resources on the desination
	maxApplyRetries      = 10
	cattleAnnotations    = "cattle.io"
	deletedMaxRetries    = 12
	deletedRetryInterval = 10 * time.Second
	boundRetryInterval   = 5 * time.Second
)

// NewMigration creates a new instance of MigrationController.
func NewMigration(mgr manager.Manager, d volume.Driver, r record.EventRecorder, rc resourcecollector.ResourceCollector) *MigrationController {
	return &MigrationController{
		client:            mgr.GetClient(),
		volDriver:         d,
		recorder:          r,
		resourceCollector: rc,
	}
}

// MigrationController reconciles migration objects
type MigrationController struct {
	client runtimeclient.Client

	volDriver               volume.Driver
	recorder                record.EventRecorder
	resourceCollector       resourcecollector.ResourceCollector
	migrationAdminNamespace string
}

// Init Initialize the migration controller
func (m *MigrationController) Init(mgr manager.Manager, migrationAdminNamespace string) error {
	err := m.createCRD()
	if err != nil {
		return err
	}

	m.migrationAdminNamespace = migrationAdminNamespace
	if err := m.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for migration rules: %v", err)
		return err
	}

	return controllers.RegisterTo(mgr, "migration-controller", m, &stork_api.Migration{})
}

// Reconcile manages Migration resources.
func (m *MigrationController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling Migration %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	migration := &stork_api.Migration{}
	err := m.client.Get(context.TODO(), request.NamespacedName, migration)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if !controllers.ContainsFinalizer(migration, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(migration, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, m.client.Update(context.TODO(), migration)
	}

	if err = m.handle(context.TODO(), migration); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(m), migration.Namespace, migration.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func setKind(snap *stork_api.Migration) {
	snap.Kind = "Migration"
	snap.APIVersion = stork_api.SchemeGroupVersion.String()
}

// performRuleRecovery terminates potential background commands running pods for
// all migration objects
func (m *MigrationController) performRuleRecovery() error {
	migrations, err := storkops.Instance().ListMigrations(v1.NamespaceAll)
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

func setDefaults(spec stork_api.MigrationSpec) stork_api.MigrationSpec {
	if spec.IncludeVolumes == nil {
		defaultBool := true
		spec.IncludeVolumes = &defaultBool
	}
	if spec.IncludeResources == nil {
		defaultBool := true
		spec.IncludeResources = &defaultBool
	}
	if spec.StartApplications == nil {
		defaultBool := false
		spec.StartApplications = &defaultBool
	}
	if spec.PurgeDeletedResources == nil {
		defaultBool := false
		spec.PurgeDeletedResources = &defaultBool
	}
	if spec.SkipServiceUpdate == nil {
		defaultBool := false
		spec.SkipServiceUpdate = &defaultBool
	}
	return spec
}

func (m *MigrationController) handle(ctx context.Context, migration *stork_api.Migration) error {
	if migration.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(migration, controllers.FinalizerCleanup) {
			if err := m.cleanup(migration); err != nil {
				logrus.Errorf("%s: cleanup: %s", reflect.TypeOf(m), err)
			}
		}

		if migration.GetFinalizers() != nil {
			controllers.RemoveFinalizer(migration, controllers.FinalizerCleanup)
			return m.client.Update(ctx, migration)
		}

		return nil
	}

	migration.Spec = setDefaults(migration.Spec)

	if migration.Spec.ClusterPair == "" {
		err := fmt.Errorf("clusterPair to migrate to cannot be empty")
		log.MigrationLog(migration).Errorf(err.Error())
		m.recorder.Event(migration,
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
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			err.Error())
		return nil
	}
	var terminationChannels []chan bool
	var err error
	var clusterDomains *stork_api.ClusterDomains
	if !*migration.Spec.IncludeVolumes {
		for i := 0; i < domainsMaxRetries; i++ {
			clusterDomains, err = m.volDriver.GetClusterDomains()
			if err == nil {

				break
			}
			time.Sleep(domainsRetryInterval)
		}
		// Fail the migration if the current domain is inactive
		// Ignore errors
		if err == nil {
			for _, domainInfo := range clusterDomains.ClusterDomainInfos {
				if domainInfo.Name == clusterDomains.LocalDomain &&
					domainInfo.State == stork_api.ClusterDomainInactive {
					migration.Status.Status = stork_api.MigrationStatusFailed
					migration.Status.Stage = stork_api.MigrationStageFinal
					migration.Status.FinishTimestamp = metav1.Now()
					msg := "Failing migration since local clusterdomain is inactive"
					m.recorder.Event(migration,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						msg)
					log.MigrationLog(migration).Warn(msg)
					return m.client.Update(context.TODO(), migration)
				}
			}
		}
	}

	switch migration.Status.Stage {
	case stork_api.MigrationStageInitial:
		// Make sure the namespaces exist
		for _, ns := range migration.Spec.Namespaces {
			_, err := core.Instance().GetNamespace(ns)
			if err != nil {
				migration.Status.Status = stork_api.MigrationStatusFailed
				migration.Status.Stage = stork_api.MigrationStageFinal
				migration.Status.FinishTimestamp = metav1.Now()
				err = fmt.Errorf("error getting namespace %v: %v", ns, err)
				log.MigrationLog(migration).Errorf(err.Error())
				m.recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					err.Error())
				err = m.client.Update(context.Background(), migration)
				if err != nil {
					log.MigrationLog(migration).Errorf("Error updating")
				}
				return nil
			}
		}
		// Make sure the rules exist if configured
		if migration.Spec.PreExecRule != "" {
			_, err := storkops.Instance().GetRule(migration.Spec.PreExecRule, migration.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PreExecRule %v: %v", migration.Spec.PreExecRule, err)
				log.MigrationLog(migration).Errorf(message)
				m.recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)
				return nil
			}
		}
		if migration.Spec.PostExecRule != "" {
			_, err := storkops.Instance().GetRule(migration.Spec.PostExecRule, migration.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PostExecRule %v: %v", migration.Spec.PreExecRule, err)
				log.MigrationLog(migration).Errorf(message)
				m.recorder.Event(migration,
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
			m.recorder.Event(migration,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				message)
			migration.Status.Stage = stork_api.MigrationStageInitial
			migration.Status.Status = stork_api.MigrationStatusInitial
			err := m.client.Update(context.Background(), migration)
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
				m.recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)
				return nil
			}
		} else {
			migration.Status.Stage = stork_api.MigrationStageApplications
			migration.Status.Status = stork_api.MigrationStatusInitial
			err := m.client.Update(context.Background(), migration)
			if err != nil {
				return err
			}
		}
	case stork_api.MigrationStageApplications:
		err := m.migrateResources(migration, false)
		if err != nil {
			message := fmt.Sprintf("Error migrating resources: %v", err)
			log.MigrationLog(migration).Errorf(message)
			m.recorder.Event(migration,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				message)
			return nil
		}
	case stork_api.MigrationStageFinal:
		return nil
	default:
		log.MigrationLog(migration).Errorf("Invalid stage for migration: %v", migration.Status.Stage)
	}

	return nil
}

func (m *MigrationController) purgeMigratedResources(migration *stork_api.Migration) error {
	remoteConfig, err := getClusterPairSchedulerConfig(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return err
	}

	log.MigrationLog(migration).Infof("Purging old unused resources ...")
	// use seperate resource collector for collecting resources
	// from destination cluster
	rc := resourcecollector.ResourceCollector{
		Driver: m.volDriver,
	}
	err = rc.Init(remoteConfig)
	if err != nil {
		log.MigrationLog(migration).Errorf("Error initializing resource collector: %v", err)
		return err
	}
	destObjects, err := rc.GetResources(
		migration.Spec.Namespaces,
		migration.Spec.Selectors,
		nil,
		migration.Spec.IncludeOptionalResourceTypes,
		false)
	if err != nil {
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error getting resources from destination: %v", err))
		log.MigrationLog(migration).Errorf("Error getting resources: %v", err)
		return err
	}
	srcObjects, err := m.resourceCollector.GetResources(
		migration.Spec.Namespaces,
		migration.Spec.Selectors,
		nil,
		migration.Spec.IncludeOptionalResourceTypes,
		false)
	if err != nil {
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error getting resources from source: %v", err))
		log.MigrationLog(migration).Errorf("Error getting resources: %v", err)
		return err
	}
	obj, err := objectToCollect(destObjects)
	if err != nil {
		return err
	}
	toBeDeleted := objectTobeDeleted(srcObjects, obj)
	dynamicInterface, err := dynamic.NewForConfig(remoteConfig)
	if err != nil {
		return err
	}
	err = m.resourceCollector.DeleteResources(dynamicInterface, toBeDeleted)
	if err != nil {
		return err
	}

	// update status of cleaned up objects migration info
	for _, r := range toBeDeleted {
		nm, ns, kind, err := getObjectDetails(r)
		if err != nil {
			// log error and skip adding object to status
			log.MigrationLog(migration).Errorf("Unable to get object details: %v", err)
			continue
		}
		resourceInfo := &stork_api.MigrationResourceInfo{
			Name:      nm,
			Namespace: ns,
			Status:    stork_api.MigrationStatusPurged,
		}
		resourceInfo.Kind = kind
		migration.Status.Resources = append(migration.Status.Resources, resourceInfo)
	}

	return nil
}

func getObjectDetails(o interface{}) (name, namespace, kind string, err error) {
	metadata, err := meta.Accessor(o)
	if err != nil {
		return "", "", "", err
	}
	objType, err := meta.TypeAccessor(o)
	if err != nil {
		return "", "", "", err
	}
	return metadata.GetName(), metadata.GetNamespace(), objType.GetKind(), nil
}

func objectToCollect(destObject []runtime.Unstructured) ([]runtime.Unstructured, error) {
	var objects []runtime.Unstructured
	for _, obj := range destObject {
		metadata, err := meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		if metadata.GetNamespace() != "" {
			if val, ok := metadata.GetAnnotations()[StorkMigrationAnnotation]; ok {
				if skip, err := strconv.ParseBool(val); err == nil && skip {
					objects = append(objects, obj)
				}
			}
		}
	}
	return objects, nil
}

func objectTobeDeleted(srcObjects, destObjects []runtime.Unstructured) []runtime.Unstructured {
	var deleteObjects []runtime.Unstructured
	for _, o := range destObjects {
		name, namespace, kind, err := getObjectDetails(o)
		if err != nil {
			// skip purging if we are not able to get object details
			logrus.Errorf("Unable to get object details %v", err)
			continue
		}
		isPresent := false
		for _, s := range srcObjects {
			sname, snamespace, skind, err := getObjectDetails(s)
			if err != nil {
				// skip purging if we are not able to get object details
				continue
			}
			if skind == kind && snamespace == namespace && sname == name {
				isPresent = true
				break
			}
		}
		if !isPresent {
			logrus.Infof("Deleting object from destination(%v:%v:%v)", name, namespace, kind)
			deleteObjects = append(deleteObjects, o)
		}
	}
	return deleteObjects
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
				err := m.client.Update(context.TODO(), migration)
				if err != nil {
					return err
				}
			}
			return fmt.Errorf("cluster pair storage status is not ready. Status: %v Err: %v",
				storageStatus, err)
		}

		volumeInfos, err := m.volDriver.StartMigration(migration)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.MigrationVolumeInfo, 0)
		}
		migration.Status.Volumes = volumeInfos
		migration.Status.Status = stork_api.MigrationStatusInProgress
		err = m.client.Update(context.TODO(), migration)
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
				m.recorder.Event(migration,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					message)

				// Cancel the migration and mark it as failed if the postExecRule failed
				err = m.volDriver.CancelMigration(migration)
				if err != nil {
					log.MigrationLog(migration).Errorf("Error cancelling migration: %v", err)
				}
				migration.Status.Stage = stork_api.MigrationStageFinal
				migration.Status.FinishTimestamp = metav1.Now()
				migration.Status.Status = stork_api.MigrationStatusFailed
				err = m.client.Update(context.TODO(), migration)
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
		volumeInfos, err := m.volDriver.GetMigrationStatus(migration)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.MigrationVolumeInfo, 0)
		}
		migration.Status.Volumes = volumeInfos
		// Store the new status
		err = m.client.Update(context.TODO(), migration)
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
				m.recorder.Event(migration,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error migrating volume %v: %v", vInfo.Volume, vInfo.Reason))
				migration.Status.Stage = stork_api.MigrationStageFinal
				migration.Status.FinishTimestamp = metav1.Now()
				migration.Status.Status = stork_api.MigrationStatusFailed
			} else if vInfo.Status == stork_api.MigrationStatusSuccessful {
				m.recorder.Event(migration,
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
			err := m.client.Update(context.TODO(), migration)
			if err != nil {
				return err
			}
			err = m.migrateResources(migration, false)
			if err != nil {
				log.MigrationLog(migration).Errorf("Error migrating resources: %v", err)
				return err
			}
		} else {
			err := m.migrateResources(migration, true)
			if err != nil {
				log.MigrationLog(migration).Errorf("Error migrating resources: %v", err)
				return err
			}
			migration.Status.Stage = stork_api.MigrationStageFinal
			migration.Status.FinishTimestamp = metav1.Now()
			migration.Status.Status = stork_api.MigrationStatusSuccessful
		}
	}

	return m.client.Update(context.TODO(), migration)
}

func (m *MigrationController) runPreExecRule(migration *stork_api.Migration) ([]chan bool, error) {
	if migration.Spec.PreExecRule == "" {
		migration.Status.Stage = stork_api.MigrationStageVolumes
		migration.Status.Status = stork_api.MigrationStatusPending
		err := m.client.Update(context.TODO(), migration)
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
			err := m.client.Update(context.TODO(), migration)
			if err != nil {
				return nil, err
			}
		} else if migration.Status.Status == stork_api.MigrationStatusInProgress {
			m.recorder.Event(migration,
				v1.EventTypeNormal,
				string(stork_api.MigrationStatusInProgress),
				fmt.Sprintf("Waiting for PreExecRule %v", migration.Spec.PreExecRule))
			return nil, nil
		}
	}
	terminationChannels := make([]chan bool, 0)
	for _, ns := range migration.Spec.Namespaces {
		r, err := storkops.Instance().GetRule(migration.Spec.PreExecRule, ns)
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
		r, err := storkops.Instance().GetRule(migration.Spec.PostExecRule, ns)
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

func (m *MigrationController) migrateResources(migration *stork_api.Migration, volumesOnly bool) error {
	schedulerStatus, err := getClusterPairSchedulerStatus(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return err
	}

	if schedulerStatus != stork_api.ClusterPairStatusReady {
		return fmt.Errorf("scheduler Cluster pair is not ready. Status: %v", schedulerStatus)
	}

	if migration.Spec.AdminClusterPair != "" {
		schedulerStatus, err = getClusterPairSchedulerStatus(migration.Spec.AdminClusterPair, m.migrationAdminNamespace)
		if err != nil {
			return err
		}
		if schedulerStatus != stork_api.ClusterPairStatusReady {
			return fmt.Errorf("scheduler in Admin Cluster pair is not ready. Status: %v", schedulerStatus)
		}
	}
	resKinds := make(map[string]string)
	var updateObjects []runtime.Unstructured

	// Don't modify resources if mentioned explicitly in specs
	if *migration.Spec.SkipServiceUpdate {
		if m.resourceCollector.Opts == nil {
			m.resourceCollector.Opts = make(map[string]string)
		}
		m.resourceCollector.Opts[resourcecollector.ServiceKind] = "true"
	}
	allObjects, err := m.resourceCollector.GetResources(
		migration.Spec.Namespaces,
		migration.Spec.Selectors,
		nil,
		migration.Spec.IncludeOptionalResourceTypes,
		false)
	if err != nil {
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error getting resource: %v", err))
		log.MigrationLog(migration).Errorf("Error getting resources: %v", err)
		return err
	}

	// Save the collected resources infos in the status
	resourceInfos := make([]*stork_api.MigrationResourceInfo, 0)
	for _, obj := range allObjects {
		metadata, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		gvk := obj.GetObjectKind().GroupVersionKind()
		if volumesOnly {
			switch gvk.Kind {
			case "PersistentVolume":
			case "PersistentVolumeClaim":
			default:
				continue
			}
		}
		resourceInfo := &stork_api.MigrationResourceInfo{
			Name:      metadata.GetName(),
			Namespace: metadata.GetNamespace(),
			Status:    stork_api.MigrationStatusInProgress,
		}

		resourceInfo.Kind = gvk.Kind
		resourceInfo.Group = gvk.Group
		// core Group doesn't have a name, so override it
		if resourceInfo.Group == "" {
			resourceInfo.Group = "core"
		}
		resourceInfo.Version = gvk.Version
		resKinds[gvk.Kind] = gvk.Version
		resourceInfos = append(resourceInfos, resourceInfo)
		updateObjects = append(updateObjects, obj)
	}

	migration.Status.Resources = resourceInfos
	err = m.client.Update(context.TODO(), migration)
	if err != nil {
		return err
	}

	err = m.prepareResources(migration, updateObjects)
	if err != nil {
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error preparing resource: %v", err))
		log.MigrationLog(migration).Errorf("Error preparing resources: %v", err)
		return err
	}
	err = m.applyResources(migration, updateObjects, resKinds)
	if err != nil {
		m.recorder.Event(migration,
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
	if *migration.Spec.PurgeDeletedResources {
		if err := m.purgeMigratedResources(migration); err != nil {
			message := fmt.Sprintf("Error cleaning up resources: %v", err)
			log.MigrationLog(migration).Errorf(message)
			m.recorder.Event(migration,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusPartialSuccess),
				message)
			return nil
		}
	}

	err = m.client.Update(context.TODO(), migration)
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationController) prepareResources(
	migration *stork_api.Migration,
	objects []runtime.Unstructured,
) error {
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return err
	}

	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		resource := o.GetObjectKind().GroupVersionKind()
		switch resource.Kind {
		case "PersistentVolume":
			err := m.preparePVResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		case "PersistentVolumeClaim":
			err := m.preparePVCResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		case "Deployment", "StatefulSet", "DeploymentConfig", "IBPPeer", "IBPCA", "IBPConsole", "IBPOrderer", "ReplicaSet":
			err := m.prepareApplicationResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing %v resource %v: %v", o.GetObjectKind().GroupVersionKind().Kind, metadata.GetName(), err)
			}
		case "CronJob":
			err := m.prepareJobResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing %v resource %v: %v", o.GetObjectKind().GroupVersionKind().Kind, metadata.GetName(), err)
			}
		}

		// prepare CR resources
		for _, crd := range crdList.Items {
			for _, v := range crd.Resources {
				if v.Kind == resource.Kind &&
					v.Version == resource.Version &&
					v.Group == resource.Group {
					if err := m.prepareCRDClusterResource(migration, o, v.SuspendOptions); err != nil {
						return fmt.Errorf("error preparing %v resource %v: %v",
							o.GetObjectKind().GroupVersionKind().Kind, metadata.GetName(), err)
					}
				}
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
			m.recorder.Event(migration, eventType, string(status), eventMessage)
			return
		}
	}
}

func (m *MigrationController) getRemoteAdminConfig(migration *stork_api.Migration) (*kubernetes.Clientset, error) {
	remoteConfig, err := getClusterPairSchedulerConfig(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return nil, err
	}
	remoteAdminConfig := remoteConfig
	// Use the admin cluter pair for cluster scoped resources if it has been configured
	if migration.Spec.AdminClusterPair != "" {
		remoteAdminConfig, err = getClusterPairSchedulerConfig(migration.Spec.AdminClusterPair, m.migrationAdminNamespace)
		if err != nil {
			return nil, err
		}
	}

	adminClient, err := kubernetes.NewForConfig(remoteAdminConfig)
	if err != nil {
		return nil, err
	}
	return adminClient, nil
}

func (m *MigrationController) checkAndUpdateService(
	migration *stork_api.Migration,
	object runtime.Unstructured,
	objHash uint64,
) (bool, error) {
	var svc v1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &svc); err != nil {
		return false, fmt.Errorf("error converting unstructured obj to service resource: %v", err)
	}
	if _, ok := svc.Annotations[resourcecollector.SkipModifyResources]; ok {
		// older behaviour where we delete and create svc resources
		return false, nil
	}
	adminClient, err := m.getRemoteAdminConfig(migration)
	if err != nil {
		return false, err
	}

	// compare and decide if update to svc is required on dest cluster
	curr, err := adminClient.CoreV1().Services(svc.GetNamespace()).Get(context.TODO(), svc.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if curr.Annotations == nil {
		curr.Annotations = make(map[string]string)
	}
	if hash, ok := curr.Annotations[resourcecollector.StorkResourceHash]; ok {
		old, err := strconv.ParseUint(hash, 10, 64)
		if err != nil {
			return false, err
		}
		if old == objHash {
			log.MigrationLog(migration).Infof("skipping service update, no changes found since last migration %d/%d", old, objHash)
			return true, nil
		}
	}
	// update annotations
	for k, v := range svc.Annotations {
		curr.Annotations[k] = v
	}
	// update selectors
	if curr.Spec.Selector == nil {
		curr.Spec.Selector = make(map[string]string)
	}
	for k, v := range svc.Spec.Selector {
		curr.Spec.Selector[k] = v
	}
	_, err = adminClient.CoreV1().Services(svc.GetNamespace()).Update(context.TODO(), curr, metav1.UpdateOptions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (m *MigrationController) checkAndUpdateDefaultSA(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	var sourceSA v1.ServiceAccount
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &sourceSA); err != nil {
		return fmt.Errorf("error converting to serviceAccount: %v", err)
	}
	adminClient, err := m.getRemoteAdminConfig(migration)
	if err != nil {
		return err
	}

	log.MigrationLog(migration).Infof("Updating service account(namespace/name : %s/%s) with image pull secrets", sourceSA.GetNamespace(), sourceSA.GetName())
	// merge service account resource for default namespaces
	destSA, err := adminClient.CoreV1().ServiceAccounts(sourceSA.GetNamespace()).Get(context.TODO(), sourceSA.GetName(), metav1.GetOptions{})
	if err != nil {
		return err
	}

	for _, s := range sourceSA.ImagePullSecrets {
		found := false
		for _, d := range destSA.ImagePullSecrets {
			if d.Name == s.Name {
				found = true
				break
			}
		}
		if !found {
			destSA.ImagePullSecrets = append(destSA.ImagePullSecrets, s)
		}
	}
	// merge annotation for SA
	log.MigrationLog(migration).Infof("Updating service account(namespace/name : %s/%s) annotations", sourceSA.GetNamespace(), sourceSA.GetName())
	if destSA.Annotations != nil {
		if sourceSA.Annotations == nil {
			sourceSA.Annotations = make(map[string]string)
		}
		for k, v := range sourceSA.Annotations {
			destSA.Annotations[k] = v
		}
	}
	_, err = adminClient.CoreV1().ServiceAccounts(destSA.GetNamespace()).Update(context.TODO(), destSA, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (m *MigrationController) preparePVResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	var pv v1.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return err
	}
	// lets keep retain policy always before applying migration
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string)
	}
	pv.Annotations[PVReclaimAnnotation] = string(pv.Spec.PersistentVolumeReclaimPolicy)
	pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
	_, err := m.volDriver.UpdateMigratedPersistentVolumeSpec(&pv)
	if err != nil {
		return err
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pv)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}

func (m *MigrationController) preparePVCResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	var pvc v1.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
		return err
	}

	if pvc.Annotations != nil {
		delete(pvc.Annotations, storageClassAnnotation)
	}
	sc := ""
	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &sc
	}
	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return nil
}

// this method can be used for k8s object where we will need to set resource to true/false to disable them
// on migration
func (m *MigrationController) prepareJobResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	if *migration.Spec.StartApplications {
		return nil
	}
	content := object.UnstructuredContent()
	// set suspend to true to disable Cronjobs
	return unstructured.SetNestedField(content, true, "spec", "suspend")
}

func (m *MigrationController) prepareApplicationResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
) error {
	if *migration.Spec.StartApplications {
		return nil
	}

	content := object.UnstructuredContent()
	// Reset the replicas to 0 and store the current replicas in an annotation
	replicas, found, err := unstructured.NestedInt64(content, "spec", "replicas")
	if err != nil {
		return err
	}
	if !found {
		replicas = 1
	}

	err = unstructured.SetNestedField(content, int64(0), "spec", "replicas")
	if err != nil {
		return err
	}

	annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
	if err != nil {
		return err
	}
	if !found {
		annotations = make(map[string]string)
	}
	annotations[StorkMigrationReplicasAnnotation] = strconv.FormatInt(replicas, 10)
	return unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations")
}

func (m *MigrationController) prepareCRDClusterResource(
	migration *stork_api.Migration,
	object runtime.Unstructured,
	suspend stork_api.SuspendOptions,
) error {
	if *migration.Spec.StartApplications {
		return nil
	}
	content := object.UnstructuredContent()
	fields := strings.Split(suspend.Path, ".")
	var currVal string
	if len(fields) > 1 {
		var disableVersion interface{}
		if suspend.Type == "bool" {
			disableVersion = true
		} else if suspend.Type == "int" {
			curr, found, err := unstructured.NestedInt64(content, fields...)
			if err != nil || !found {
				return fmt.Errorf("unable to find suspend path, err: %v", err)
			}
			disableVersion = int64(0)
			currVal = fmt.Sprintf("%v", curr)
		} else if suspend.Type == "string" {
			curr, found, err := unstructured.NestedString(content, fields...)
			if err != nil || !found {
				return fmt.Errorf("unable to find suspend path, err: %v", err)
			}
			disableVersion = suspend.Value
			currVal = curr
		} else {
			return fmt.Errorf("invalid type %v to suspend cr", suspend.Type)
		}

		if err := unstructured.SetNestedField(content, disableVersion, fields...); err != nil {
			return err
		}
		annotations, found, err := unstructured.NestedStringMap(content, "metadata", "annotations")
		if err != nil {
			return err
		}
		if !found {
			annotations = make(map[string]string)
		}
		annotations[StorkMigrationCRDDeactivateAnnotation] = suspend.Value
		annotations[StorkMigrationCRDActivateAnnotation] = currVal
		return unstructured.SetNestedStringMap(content, annotations, "metadata", "annotations")
	}

	return nil
}

func (m *MigrationController) getPrunedAnnotations(annotations map[string]string) map[string]string {
	a := make(map[string]string)
	for k, v := range annotations {
		if !strings.Contains(k, cattleAnnotations) {
			a[k] = v
		}
	}
	return a
}

func (m *MigrationController) applyResources(
	migration *stork_api.Migration,
	objects []runtime.Unstructured,
	resKinds map[string]string,
) error {
	remoteConfig, err := getClusterPairSchedulerConfig(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return err
	}
	remoteAdminConfig := remoteConfig
	// Use the admin cluter pair for cluster scoped resources if it has been configured
	if migration.Spec.AdminClusterPair != "" {
		remoteAdminConfig, err = getClusterPairSchedulerConfig(migration.Spec.AdminClusterPair, m.migrationAdminNamespace)
		if err != nil {
			return err
		}
	}

	adminClient, err := kubernetes.NewForConfig(remoteAdminConfig)
	if err != nil {
		return err
	}
	// TODO: we should use k8s code generator logic to pluralize
	// crd resources instead of depending on inflect lib
	ruleset := inflect.NewDefaultRuleset()
	ruleset.AddPlural("quota", "quotas")
	ruleset.AddPlural("prometheus", "prometheuses")
	ruleset.AddPlural("mongodbcommunity", "mongodbcommunity")
	// create CRD on destination cluster
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		logrus.Warnf("unable to list crd registrations: %v", err)
		return err
	}
	for _, crd := range crdList.Items {
		for _, v := range crd.Resources {
			// only create relevant crds on dest cluster
			if _, ok := resKinds[v.Kind]; !ok {
				continue
			}
			config, err := rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("error getting cluster config: %v", err)
			}

			srcClnt, err := apiextensionsclient.NewForConfig(config)
			if err != nil {
				return err
			}
			destClnt, err := apiextensionsclient.NewForConfig(remoteAdminConfig)
			if err != nil {
				return err
			}
			crdName := ruleset.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
			crdvbeta1, err := srcClnt.ApiextensionsV1beta1().CustomResourceDefinitions().Get(context.TODO(), crdName, metav1.GetOptions{})
			if err == nil {
				crdvbeta1.ResourceVersion = ""
				if _, regErr := destClnt.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crdvbeta1, metav1.CreateOptions{}); regErr != nil && !errors.IsAlreadyExists(regErr) {
					log.MigrationLog(migration).Warnf("error registering crds %s, %v", crdvbeta1.GetName(), err)
				} else if regErr == nil {
					if err := k8sutils.ValidateCRD(destClnt, crdName); err != nil {
						log.MigrationLog(migration).Errorf("Unable to validate crds %v,%v", crdvbeta1.GetName(), err)
					}
					continue
				}
			}
			res, err := srcClnt.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crdName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					log.MigrationLog(migration).Warnf("CRDV1 not found %v for kind %v", crdName, v.Kind)
					continue
				}
				log.MigrationLog(migration).Errorf("unable to get customresourcedefination for %s, err: %v", crdName, err)
				return err
			}

			res.ResourceVersion = ""
			// if crds is applied as v1beta on k8s version 1.16+ it will have
			// preservedUnkownField set and api version converted to v1 ,
			// which cause issue while applying it on dest cluster,
			// since we will be applying v1 crds with non-valid schema

			// this converts `preserveUnknownFiels`(deprecated) to spec.Versions[*].xPreservedUnknown
			// equivalent
			var updatedVersions []apiextensionsv1.CustomResourceDefinitionVersion
			if res.Spec.PreserveUnknownFields {
				res.Spec.PreserveUnknownFields = false
				for _, version := range res.Spec.Versions {
					isTrue := true
					if version.Schema == nil {
						openAPISchema := &apiextensionsv1.CustomResourceValidation{
							OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{XPreserveUnknownFields: &isTrue},
						}
						version.Schema = openAPISchema
					} else {
						version.Schema.OpenAPIV3Schema.XPreserveUnknownFields = &isTrue
					}
					updatedVersions = append(updatedVersions, version)
				}
				res.Spec.Versions = updatedVersions
			}
			var regErr error
			if _, regErr = destClnt.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), res, metav1.CreateOptions{}); regErr != nil && !errors.IsAlreadyExists(regErr) {
				log.MigrationLog(migration).Errorf("error registering crds v1 %s, %v", res.GetName(), err)
			}
			if regErr == nil {
				// wait for crd to be ready
				if err := k8sutils.ValidateCRDV1(destClnt, res.GetName()); err != nil {
					log.MigrationLog(migration).Errorf("Unable to validate crds v1 %v,%v", res.GetName(), err)
				}
			}
		}
	}

	// First make sure all the namespaces are created on the
	// remote cluster
	for _, ns := range migration.Spec.Namespaces {
		namespace, err := core.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}

		// Don't create if the namespace already exists on the remote cluster
		_, err = adminClient.CoreV1().Namespaces().Get(context.TODO(), namespace.Name, metav1.GetOptions{})
		if err == nil {
			continue
		}

		annotations := m.getPrunedAnnotations(namespace.Annotations)
		_, err = adminClient.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        namespace.Name,
				Labels:      namespace.Labels,
				Annotations: annotations,
			},
		}, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
	}

	remoteInterface, err := dynamic.NewForConfig(remoteConfig)
	if err != nil {
		return err
	}
	remoteAdminInterface := remoteInterface
	if migration.Spec.AdminClusterPair != "" {
		remoteAdminInterface, err = dynamic.NewForConfig(remoteAdminConfig)
		if err != nil {
			return err
		}
	}
	var pvObjects, pvcObjects, updatedObjects []runtime.Unstructured
	pvMapping := make(map[string]v1.ObjectReference)
	// collect pv,pvc separately
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}
		switch objectType.GetKind() {
		case "PersistentVolume":
			pvObjects = append(pvObjects, o)
		case "PersistentVolumeClaim":
			pvcObjects = append(pvcObjects, o)
		default:
			updatedObjects = append(updatedObjects, o)
		}
	}
	// create/update pv object with updated policy
	for _, obj := range pvObjects {
		var pv v1.PersistentVolume
		var err error
		if err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &pv); err != nil {
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error unmarshalling pv resource: %v", err))
			continue
		}
		log.MigrationLog(migration).Infof("Applying %v %v", pv.Kind, pv.GetName())
		if pv.GetAnnotations() == nil {
			pv.Annotations = make(map[string]string)
		}
		pv.Annotations[StorkMigrationAnnotation] = "true"
		pv.Annotations[StorkMigrationName] = migration.GetName()
		pv.Annotations[StorkMigrationTime] = time.Now().Format(nameTimeSuffixFormat)
		_, err = adminClient.CoreV1().PersistentVolumes().Create(context.TODO(), &pv, metav1.CreateOptions{})
		if err != nil {
			if err != nil && errors.IsAlreadyExists(err) {
				var respPV *v1.PersistentVolume
				respPV, err = adminClient.CoreV1().PersistentVolumes().Get(context.TODO(), pv.Name, metav1.GetOptions{})
				if err == nil {
					// allow only annotation and reclaim policy update
					// TODO: idle way should be to use Patch
					if respPV.GetAnnotations() == nil {
						respPV.Annotations = make(map[string]string)
					}
					respPV.Annotations = pv.Annotations
					respPV.Spec.PersistentVolumeReclaimPolicy = pv.Spec.PersistentVolumeReclaimPolicy
					respPV.ResourceVersion = ""
					_, err = adminClient.CoreV1().PersistentVolumes().Update(context.TODO(), respPV, metav1.UpdateOptions{})
				}
			}
		}
		if err != nil {
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err))
			// fail migrations since we were not able to update pv reclaim object
			migration.Status.Stage = stork_api.MigrationStageFinal
			migration.Status.FinishTimestamp = metav1.Now()
			migration.Status.Status = stork_api.MigrationStatusFailed
			return m.client.Update(context.TODO(), migration)
		}
		m.updateResourceStatus(
			migration,
			obj,
			stork_api.MigrationStatusSuccessful,
			"Resource migrated successfully")
	}
	// apply pvc objects
	for _, obj := range pvcObjects {
		var pvc v1.PersistentVolumeClaim
		var err error
		if err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &pvc); err != nil {
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err))
			continue
		}
		objRef := v1.ObjectReference{
			Name:      pvc.GetName(),
			Namespace: pvc.GetNamespace(),
		}
		// skip if there is no change in pvc specs
		objHash, err := hashstructure.Hash(obj, &hashstructure.HashOptions{})
		if err != nil {
			msg := fmt.Errorf("unable to generate hash for an object %v %v, err: %v", pvc.Kind, pvc.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		}
		resp, err := adminClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
		if err == nil && resp != nil {
			if hash, ok := resp.Annotations[resourcecollector.StorkResourceHash]; ok {
				old, err := strconv.ParseUint(hash, 10, 64)
				if err == nil && old == objHash {
					log.MigrationLog(migration).Debugf("Skipping pvc update, no changes found since last migration %d/%d", old, objHash)
					m.updateResourceStatus(
						migration,
						obj,
						stork_api.MigrationStatusSuccessful,
						"Resource migrated successfully")
					objRef.UID = resp.GetUID()
					pvMapping[pvc.Spec.VolumeName] = objRef
					continue
				}
			}
		}
		pvMapping[pvc.Spec.VolumeName] = objRef
		deleteStart := metav1.Now()
		isDeleted := false
		err = adminClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			msg := fmt.Errorf("error deleting %v %v during migrate: %v", pvc.Kind, pvc.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		} else {
			for i := 0; i < deletedMaxRetries; i++ {
				obj, err := adminClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
				if err != nil && errors.IsNotFound(err) {
					isDeleted = true
					break
				}
				createTime := obj.GetCreationTimestamp()
				if deleteStart.Before(&createTime) {
					log.MigrationLog(migration).Warnf("Object[%v] got re-created after deletion. Not retrying deletion, deleteStart time:[%v], create time:[%v]",
						obj.GetName(), deleteStart, createTime)
					break
				}
				log.MigrationLog(migration).Infof("Object %v still present, retrying in %v", pvc.GetName(), deletedRetryInterval)
				time.Sleep(deletedRetryInterval)
			}
		}
		if !isDeleted {
			msg := fmt.Errorf("error in recreating pvc %s/%s during migration: %v", pvc.GetNamespace(), pvc.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		}
		if pvc.GetAnnotations() == nil {
			pvc.Annotations = make(map[string]string)
		}
		pvc.Annotations[StorkMigrationAnnotation] = "true"
		pvc.Annotations[StorkMigrationName] = migration.GetName()
		pvc.Annotations[StorkMigrationTime] = time.Now().Format(nameTimeSuffixFormat)
		pvc.Annotations[resourcecollector.StorkResourceHash] = strconv.FormatUint(objHash, 10)
		_, err = adminClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Create(context.TODO(), &pvc, metav1.CreateOptions{})
		if err != nil {
			msg := fmt.Errorf("error in recreating pvc %s/%s during migration: %v", pvc.GetNamespace(), pvc.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		}
		m.updateResourceStatus(
			migration,
			obj,
			stork_api.MigrationStatusSuccessful,
			"Resource migrated successfully")
	}
	// revert pv objects reclaim policy
	for _, obj := range pvObjects {
		var pv v1.PersistentVolume
		var pvc *v1.PersistentVolumeClaim
		var err error
		if err = runtime.DefaultUnstructuredConverter.FromUnstructured(obj.UnstructuredContent(), &pv); err != nil {
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err))
			continue
		}
		if pvcObj, ok := pvMapping[pv.Name]; ok {
			pvc, err = adminClient.CoreV1().PersistentVolumeClaims(pvcObj.Namespace).Get(context.TODO(), pvcObj.Name, metav1.GetOptions{})
			if err != nil {
				msg := fmt.Errorf("error in retriving pvc info %s/%s during migration: %v", pvc.GetNamespace(), pvc.GetName(), err)
				m.updateResourceStatus(
					migration,
					obj,
					stork_api.MigrationStatusFailed,
					fmt.Sprintf("Error applying resource: %v", msg))
				continue
			}
			if pvcObj.UID != pvc.GetUID() {
				respPV, err := adminClient.CoreV1().PersistentVolumes().Get(context.TODO(), pv.Name, metav1.GetOptions{})
				if err != nil {
					msg := fmt.Errorf("error in reading pv %s during migration: %v", pv.GetName(), err)
					m.updateResourceStatus(
						migration,
						obj,
						stork_api.MigrationStatusFailed,
						fmt.Sprintf("Error applying resource: %v", msg))
					continue
				}
				if respPV.Spec.ClaimRef == nil {
					respPV.Spec.ClaimRef = &v1.ObjectReference{
						Name:      pvc.GetName(),
						Namespace: pvc.GetNamespace(),
					}
				}
				respPV.Spec.ClaimRef.UID = pvc.GetUID()
				respPV.ResourceVersion = ""
				if _, err = adminClient.CoreV1().PersistentVolumes().Update(context.TODO(), respPV, metav1.UpdateOptions{}); err != nil {
					msg := fmt.Errorf("error in updating pvc UID in pv %s during migration: %v", pv.GetName(), err)
					m.updateResourceStatus(
						migration,
						obj,
						stork_api.MigrationStatusFailed,
						fmt.Sprintf("Error applying resource: %v", msg))
					continue
				}
				// wait for pvc object to be in bound state
				isBound := false
				for i := 0; i < deletedMaxRetries*2; i++ {
					var resp *v1.PersistentVolumeClaim
					resp, err = adminClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(context.TODO(), pvc.Name, metav1.GetOptions{})
					if err != nil {
						msg := fmt.Errorf("error in retriving pvc %s/%s during migration: %v", pvc.GetNamespace(), pvc.GetName(), err)
						m.updateResourceStatus(
							migration,
							obj,
							stork_api.MigrationStatusFailed,
							fmt.Sprintf("Error applying resource: %v", msg))
						continue
					}
					if resp.Status.Phase == v1.ClaimBound {
						isBound = true
						break
					}
					log.MigrationLog(migration).Infof("PVC Object %s not bound yet, retrying in %v", pvc.GetName(), deletedRetryInterval)
					time.Sleep(boundRetryInterval)
				}
				if !isBound {
					msg := fmt.Errorf("pvc %s/%s is not in bound state ", pvc.GetNamespace(), pvc.GetName())
					m.updateResourceStatus(
						migration,
						obj,
						stork_api.MigrationStatusFailed,
						fmt.Sprintf("Error applying resource: %v", msg))
					continue
				}
			}
		}
		respPV, err := adminClient.CoreV1().PersistentVolumes().Get(context.TODO(), pv.Name, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Errorf("error in reading pv %s during migration: %v", pv.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		}
		// update pv's reclaim policy
		if pv.Annotations != nil && pv.Annotations[PVReclaimAnnotation] != "" {
			respPV.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimPolicy(pv.Annotations[PVReclaimAnnotation])
		}
		if migration.Spec.IncludeVolumes != nil && !*migration.Spec.IncludeVolumes {
			respPV.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		}
		respPV.ResourceVersion = ""
		if _, err = adminClient.CoreV1().PersistentVolumes().Update(context.TODO(), respPV, metav1.UpdateOptions{}); err != nil {
			msg := fmt.Errorf("error in updating pv %s during migration: %v", pv.GetName(), err)
			m.updateResourceStatus(
				migration,
				obj,
				stork_api.MigrationStatusFailed,
				fmt.Sprintf("Error applying resource: %v", msg))
			continue
		}
		m.updateResourceStatus(
			migration,
			obj,
			stork_api.MigrationStatusSuccessful,
			"Resource migrated successfully")
	}

	// apply remaining objects
	for _, o := range updatedObjects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}
		resource := &metav1.APIResource{
			Name:       ruleset.Pluralize(strings.ToLower(objectType.GetKind())),
			Namespaced: len(metadata.GetNamespace()) > 0,
		}
		var dynamicClient dynamic.ResourceInterface
		if resource.Namespaced {
			dynamicClient = remoteInterface.Resource(
				o.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(metadata.GetNamespace())
		} else {
			dynamicClient = remoteAdminInterface.Resource(
				o.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name))
		}

		unstructured, ok := o.(*unstructured.Unstructured)
		if !ok {
			return fmt.Errorf("unable to cast object to unstructured: %v", o)
		}

		// set migration annotations
		migrAnnot := metadata.GetAnnotations()
		if migrAnnot == nil {
			migrAnnot = make(map[string]string)
		}
		migrAnnot[StorkMigrationAnnotation] = "true"
		migrAnnot[StorkMigrationName] = migration.GetName()
		migrAnnot[StorkMigrationTime] = time.Now().Format(nameTimeSuffixFormat)
		objHash, err := hashstructure.Hash(o, &hashstructure.HashOptions{})
		if err != nil {
			log.MigrationLog(migration).Warnf("unable to generate hash for an object %v %v, err: %v", objectType.GetKind(), metadata.GetName(), err)
		}
		migrAnnot[resourcecollector.StorkResourceHash] = strconv.FormatUint(objHash, 10)
		unstructured.SetAnnotations(migrAnnot)
		retries := 0
		log.MigrationLog(migration).Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
		for {
			_, err = dynamicClient.Create(context.TODO(), unstructured, metav1.CreateOptions{})
			if err != nil && (errors.IsAlreadyExists(err) || strings.Contains(err.Error(), portallocator.ErrAllocated.Error())) {
				switch objectType.GetKind() {
				case "ServiceAccount":
					err = m.checkAndUpdateDefaultSA(migration, o)
				case "Service":
					var skipUpdate bool
					skipUpdate, err = m.checkAndUpdateService(migration, o, objHash)
					if err == nil && skipUpdate {
						break
					}
					fallthrough
				default:
					// Delete the resource if it already exists on the destination
					// cluster and try creating again
					deleteStart := metav1.Now()
					err = dynamicClient.Delete(context.TODO(), metadata.GetName(), metav1.DeleteOptions{})
					if err != nil && !errors.IsNotFound(err) {
						log.MigrationLog(migration).Errorf("Error deleting %v %v during migrate: %v", objectType.GetKind(), metadata.GetName(), err)
					} else {
						// wait for resources to get deleted
						// 2 mins
						for i := 0; i < deletedMaxRetries; i++ {
							obj, err := dynamicClient.Get(context.TODO(), metadata.GetName(), metav1.GetOptions{})
							if err != nil && errors.IsNotFound(err) {
								break
							}
							createTime := obj.GetCreationTimestamp()
							if deleteStart.Before(&createTime) {
								log.MigrationLog(migration).Warnf("Object[%v] got re-created after deletion. So, Ignore wait. deleteStart time:[%v], create time:[%v]",
									obj.GetName(), deleteStart, createTime)
								break
							}
							log.MigrationLog(migration).Warnf("Object %v still present, retrying in %v", metadata.GetName(), deletedRetryInterval)
							time.Sleep(deletedRetryInterval)
						}
						_, err = dynamicClient.Create(context.TODO(), unstructured, metav1.CreateOptions{})
					}
				}
			}
			// Retry a few times for Unauthorized errors
			if err != nil && errors.IsUnauthorized(err) && retries < maxApplyRetries {
				retries++
				continue
			}
			break
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

func (m *MigrationController) cleanup(migration *stork_api.Migration) error {
	if migration.Status.Stage != stork_api.MigrationStageFinal {
		return m.volDriver.CancelMigration(migration)
	}
	return nil
}

func (m *MigrationController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.MigrationResourceName,
		Plural:  stork_api.MigrationResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.Migration{}).Name(),
	}
	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRD(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval)
}
