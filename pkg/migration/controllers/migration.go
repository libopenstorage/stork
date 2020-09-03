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
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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
	// Max number of times to retry applying resources on the desination
	maxApplyRetries   = 10
	cattleAnnotations = "cattle.io"
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
func (m *MigrationController) Reconcile(request reconcile.Request) (reconcile.Result, error) {
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
		err := m.migrateResources(migration)
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

func (m *MigrationController) migrateResources(migration *stork_api.Migration) error {
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

		resourceInfo := &stork_api.MigrationResourceInfo{
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
		resKinds[gvk.Kind] = gvk.Version
		resourceInfos = append(resourceInfos, resourceInfo)
	}
	migration.Status.Resources = resourceInfos
	err = m.client.Update(context.TODO(), migration)
	if err != nil {
		return err
	}

	err = m.prepareResources(migration, allObjects)
	if err != nil {
		m.recorder.Event(migration,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			fmt.Sprintf("Error preparing resource: %v", err))
		log.MigrationLog(migration).Errorf("Error preparing resources: %v", err)
		return err
	}
	err = m.applyResources(migration, allObjects, resKinds)
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
		case "Deployment", "StatefulSet", "DeploymentConfig", "IBPPeer", "IBPCA", "IBPConsole", "IBPOrderer":
			err := m.prepareApplicationResource(migration, o)
			if err != nil {
				return fmt.Errorf("error preparing %v resource %v: %v", o.GetObjectKind().GroupVersionKind().Kind, metadata.GetName(), err)
			}
		}

		// prepare CR resources
		crdList, err := storkops.Instance().ListApplicationRegistrations()
		if err != nil {
			return err
		}
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

	if sourceSA.GetName() != "default" {
		// delete and recreate service account
		if err := adminClient.CoreV1().ServiceAccounts(sourceSA.GetNamespace()).Delete(sourceSA.GetName(), &metav1.DeleteOptions{}); err != nil {
			return err
		}
		if _, err := adminClient.CoreV1().ServiceAccounts(sourceSA.GetNamespace()).Create(&sourceSA); err != nil {
			return err
		}
		return nil
	}

	log.MigrationLog(migration).Infof("Updating default service account(namespace : %v) with image pull secrets", sourceSA.GetNamespace())
	// merge service account resource for default namespaces
	destSA, err := adminClient.CoreV1().ServiceAccounts(sourceSA.GetNamespace()).Get(sourceSA.GetName(), metav1.GetOptions{})
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
	_, err = adminClient.CoreV1().ServiceAccounts(destSA.GetNamespace()).Update(destSA)
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
	// Set the reclaim policy to retain if the volumes are not being migrated
	if migration.Spec.IncludeVolumes != nil && !*migration.Spec.IncludeVolumes {
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
	}

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
			disableVersion = 0
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
			crdName := inflect.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
			res, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				log.MigrationLog(migration).Errorf("unable to get customresourcedefination for %s, err: %v", v.Kind, err)
				return err
			}
			clnt, err := apiextensions.NewForConfig(remoteAdminConfig)
			if err != nil {
				return err
			}
			res.ResourceVersion = ""
			if err := clnt.RegisterCRD(res); err != nil && !errors.IsAlreadyExists(err) {
				log.MigrationLog(migration).Errorf("error registering CRD %s, %v", res.GetName(), err)
				return err
			}
			if err := clnt.ValidateCRD(apiextensions.CustomResource{
				Plural: res.Spec.Names.Plural,
				Group:  res.Spec.Group}, validateCRDTimeout, validateCRDInterval); err != nil {
				log.MigrationLog(migration).Errorf("error validating CRD %s, %v", res.GetName(), err)
				return err
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
		_, err = adminClient.CoreV1().Namespaces().Get(namespace.Name, metav1.GetOptions{})
		if err == nil {
			continue
		}

		annotations := m.getPrunedAnnotations(namespace.Annotations)
		_, err = adminClient.CoreV1().Namespaces().Create(&v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        namespace.Name,
				Labels:      namespace.Labels,
				Annotations: annotations,
			},
		})
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
			Name:       inflect.Pluralize(strings.ToLower(objectType.GetKind())),
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

		log.MigrationLog(migration).Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
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
		unstructured.SetAnnotations(migrAnnot)
		retries := 0
		for {
			_, err = dynamicClient.Create(unstructured, metav1.CreateOptions{})
			if err != nil && (errors.IsAlreadyExists(err) || strings.Contains(err.Error(), portallocator.ErrAllocated.Error())) {
				switch objectType.GetKind() {
				// Don't want to delete the Volume resources
				case "PersistentVolumeClaim":
					err = nil
				case "PersistentVolume":
					if migration.Spec.IncludeVolumes == nil || *migration.Spec.IncludeVolumes {
						err = nil
					} else {
						_, err = dynamicClient.Update(unstructured, metav1.UpdateOptions{})
					}
				case "ServiceAccount":
					err = m.checkAndUpdateDefaultSA(migration, o)
				default:
					// Delete the resource if it already exists on the destination
					// cluster and try creating again
					err = dynamicClient.Delete(metadata.GetName(), &metav1.DeleteOptions{})
					if err == nil {
						_, err = dynamicClient.Create(unstructured, metav1.CreateOptions{})
					} else {
						log.MigrationLog(migration).Errorf("Error deleting %v %v during migrate: %v", objectType.GetKind(), metadata.GetName(), err)
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
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
