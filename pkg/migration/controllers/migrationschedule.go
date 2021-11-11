package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	nameTimeSuffixFormat string = "2006-01-02-150405"
	domainsRetryInterval        = 5 * time.Second
	skipResource                = "stork.libopenstorage.org/skip-resource"
	appsReplicas                = "stork.libopenstorage.org/replicas"
	domainsMaxRetries           = 5
)

// NewMigrationSchedule creates a new instance of MigrationScheduleController.
func NewMigrationSchedule(mgr manager.Manager, d volume.Driver, r record.EventRecorder) *MigrationScheduleController {
	return &MigrationScheduleController{
		client:    mgr.GetClient(),
		volDriver: d,
		recorder:  r,
	}
}

// MigrationScheduleController reconciles MigrationSchedule objects
type MigrationScheduleController struct {
	client runtimeclient.Client

	volDriver volume.Driver
	recorder  record.EventRecorder
}

// Init Initialize the migration schedule controller
func (m *MigrationScheduleController) Init(mgr manager.Manager) error {
	err := m.createCRD()
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, "migration-schedule-controller", m, &stork_api.MigrationSchedule{})
}

// Reconcile manages MigrationSchedule resources.
func (m *MigrationScheduleController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling MigrationSchedule %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	migrationSchedule := &stork_api.MigrationSchedule{}
	err := m.client.Get(context.TODO(), request.NamespacedName, migrationSchedule)
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

	if !controllers.ContainsFinalizer(migrationSchedule, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(migrationSchedule, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, m.client.Update(context.TODO(), migrationSchedule)
	}

	if err = m.handle(context.TODO(), migrationSchedule); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(m), migrationSchedule.Namespace, migrationSchedule.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func (m *MigrationScheduleController) handle(ctx context.Context, migrationSchedule *stork_api.MigrationSchedule) error {
	// Delete any migrations created by the schedule
	if migrationSchedule.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(migrationSchedule, controllers.FinalizerCleanup) {
			if err := m.deleteMigrations(migrationSchedule); err != nil {
				logrus.Errorf("%s: cleanup: %s", reflect.TypeOf(m), err)
			}
		}

		if migrationSchedule.GetFinalizers() != nil {
			controllers.RemoveFinalizer(migrationSchedule, controllers.FinalizerCleanup)
			return m.client.Update(ctx, migrationSchedule)
		}

		return nil
	}
	migrationSchedule.Spec = setScheduleDefaults(migrationSchedule.Spec)
	if migrationSchedule.GetAnnotations() != nil {
		if _, ok := migrationSchedule.GetAnnotations()[StorkMigrationAnnotation]; ok {
			// check status of all migrated app in cluster
			logrus.Infof("Migration schedule is on dr cluster")
			isActivated, err := getMigratedAppStatus(migrationSchedule)
			if err != nil {
				return err
			}
			migrationSchedule.Status.ApplicationActivated = isActivated
			msg := fmt.Sprintf("Setting AppActive status to: %v", isActivated)
			m.recorder.Event(migrationSchedule,
				v1.EventTypeWarning,
				"AppsActivated",
				msg)
			log.MigrationScheduleLog(migrationSchedule).Warn(msg)
			return m.client.Update(context.TODO(), migrationSchedule)
		}
	}
	if !(*migrationSchedule.Spec.Suspend) {
		remoteConfig, err := getClusterPairSchedulerConfig(migrationSchedule.Spec.Template.Spec.ClusterPair, migrationSchedule.Namespace)
		if err != nil {
			return err
		}
		remoteOps, err := storkops.NewForConfig(remoteConfig)
		if err != nil {
			m.recorder.Event(migrationSchedule,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				err.Error())
			return nil
		}
		coreOps, err := core.NewForConfig(remoteConfig)
		if err != nil {
			m.recorder.Event(migrationSchedule,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				err.Error())
			return nil
		}
		migrSched, err := remoteOps.GetMigrationSchedule(migrationSchedule.Name, migrationSchedule.Namespace)
		if errors.IsNotFound(err) {
			// TODO:generate event?
			namespace, err := core.Instance().GetNamespace(migrationSchedule.Namespace)
			if err != nil {
				return err
			}
			namespace.ResourceVersion = ""
			_, err = coreOps.CreateNamespace(namespace)
			if err != nil && !errors.IsAlreadyExists(err) {
				return err
			}
			// create new
			remoteMigrSched := migrationSchedule.DeepCopy()
			remoteMigrSched.ResourceVersion = ""
			if remoteMigrSched.Annotations == nil {
				remoteMigrSched.Annotations = make(map[string]string)
			}
			remoteMigrSched.Annotations[StorkMigrationAnnotation] = "true"
			suspend := true
			remoteMigrSched.Spec.Suspend = &suspend
			remoteMigrSched.Status = stork_api.MigrationScheduleStatus{}
			if _, err := remoteOps.CreateMigrationSchedule(remoteMigrSched); err != nil {
				return err
			}

		} else if err != nil {
			return err
		}
		if migrSched.Status.ApplicationActivated {
			suspend := true
			migrationSchedule.Spec.Suspend = &suspend
			msg := "Suspending migration schedule since migrated apps on remote cluster are active"
			m.recorder.Event(migrationSchedule,
				v1.EventTypeWarning,
				"Suspended",
				msg)
			log.MigrationScheduleLog(migrationSchedule).Warn(msg)
			// deactivate apps in namespace
			// TODO: suspend deploy/sts,crds
			return m.client.Update(context.TODO(), migrationSchedule)
		}
	}
	// First update the status of any pending migrations
	err := m.updateMigrationStatus(migrationSchedule)
	if err != nil {
		msg := fmt.Sprintf("Error updating migration status: %v", err)
		m.recorder.Event(migrationSchedule,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			msg)
		log.MigrationScheduleLog(migrationSchedule).Error(msg)
		return err
	}

	// Then check if any of the policies require a trigger if it is enabled
	if migrationSchedule.Spec.Suspend == nil || !*migrationSchedule.Spec.Suspend {
		var err error
		var clusterDomains *stork_api.ClusterDomains
		for i := 0; i < domainsMaxRetries; i++ {
			clusterDomains, err = m.volDriver.GetClusterDomains()
			if err == nil {
				break
			}
			time.Sleep(domainsRetryInterval)
		}
		// Ignore errors
		if err == nil {
			for _, domainInfo := range clusterDomains.ClusterDomainInfos {
				if domainInfo.Name == clusterDomains.LocalDomain &&
					domainInfo.State == stork_api.ClusterDomainInactive {
					suspend := true
					migrationSchedule.Spec.Suspend = &suspend
					msg := "Suspending migration schedule since local clusterdomain is inactive"
					m.recorder.Event(migrationSchedule,
						v1.EventTypeWarning,
						"Suspended",
						msg)
					log.MigrationScheduleLog(migrationSchedule).Warn(msg)
					return m.client.Update(context.TODO(), migrationSchedule)
				}
			}
		}

		policyType, start, err := m.shouldStartMigration(migrationSchedule)
		if err != nil {
			msg := fmt.Sprintf("Error checking if migration should be triggered: %v", err)
			m.recorder.Event(migrationSchedule,
				v1.EventTypeWarning,
				string(stork_api.MigrationStatusFailed),
				msg)
			log.MigrationScheduleLog(migrationSchedule).Error(msg)
			return nil
		}

		// Start a migration for a policy if required
		if start {
			err := m.startMigration(migrationSchedule, policyType)
			if err != nil {
				msg := fmt.Sprintf("Error triggering migration for schedule(%v): %v", policyType, err)
				m.recorder.Event(migrationSchedule,
					v1.EventTypeWarning,
					string(stork_api.MigrationStatusFailed),
					msg)
				log.MigrationScheduleLog(migrationSchedule).Error(msg)
				return err
			}
		}

	}

	// Finally, prune any old migrations that were triggered for this
	// schedule
	err = m.pruneMigrations(migrationSchedule)
	if err != nil {
		msg := fmt.Sprintf("Error pruning old migrations: %v", err)
		m.recorder.Event(migrationSchedule,
			v1.EventTypeWarning,
			string(stork_api.MigrationStatusFailed),
			msg)
		log.MigrationScheduleLog(migrationSchedule).Error(msg)
		return err
	}

	return nil
}

func (m *MigrationScheduleController) updateMigrationStatus(migrationSchedule *stork_api.MigrationSchedule) error {
	updated := false
	for _, policyMigration := range migrationSchedule.Status.Items {
		for _, migration := range policyMigration {
			// Get the updated status if we see it as not completed
			if !m.isMigrationComplete(migration.Status) {
				var updatedStatus stork_api.MigrationStatusType
				pendingMigration, err := storkops.Instance().GetMigration(migration.Name, migrationSchedule.Namespace)
				if err != nil {
					m.recorder.Event(migrationSchedule,
						v1.EventTypeWarning,
						string(stork_api.MigrationStatusFailed),
						fmt.Sprintf("Error getting status of migration %v: %v", migration.Name, err))
					updatedStatus = stork_api.MigrationStatusFailed
				} else {
					updatedStatus = pendingMigration.Status.Status
				}

				if updatedStatus == stork_api.MigrationStatusInitial {
					updatedStatus = stork_api.MigrationStatusPending
				}

				// Check again and update the status if it is completed
				migration.Status = updatedStatus
				if m.isMigrationComplete(migration.Status) {
					migration.FinishTimestamp = meta.NewTime(schedule.GetCurrentTime())
					if updatedStatus == stork_api.MigrationStatusSuccessful {
						m.recorder.Event(migrationSchedule,
							v1.EventTypeNormal,
							string(stork_api.MigrationStatusSuccessful),
							fmt.Sprintf("Scheduled migration (%v) completed successfully", migration.Name))
					} else {
						m.recorder.Event(migrationSchedule,
							v1.EventTypeWarning,
							string(stork_api.MigrationStatusFailed),
							fmt.Sprintf("Scheduled migration (%v) status %v", migration.Name, updatedStatus))
					}
				}
				updated = true
			}
		}
	}
	if updated {
		err := m.client.Update(context.TODO(), migrationSchedule)
		if err != nil {
			return err
		}
	}
	return nil
}

func setScheduleDefaults(spec stork_api.MigrationScheduleSpec) stork_api.MigrationScheduleSpec {
	if spec.Suspend == nil {
		defaultBool := false
		spec.Suspend = &defaultBool
	}
	return spec
}
func (m *MigrationScheduleController) isMigrationComplete(status stork_api.MigrationStatusType) bool {
	if status == stork_api.MigrationStatusPending ||
		status == stork_api.MigrationStatusInProgress {
		return false
	}
	return true
}

// Returns if a migration should be triggered given the status and times of the
// previous migrations. If a migration should be triggered it also returns the
// type of polivy that should trigger it.
func (m *MigrationScheduleController) shouldStartMigration(
	migrationSchedule *stork_api.MigrationSchedule,
) (stork_api.SchedulePolicyType, bool, error) {
	// Don't trigger a new migration if one is already in progress
	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		policyMigration, present := migrationSchedule.Status.Items[policyType]
		if present {
			for _, migration := range policyMigration {
				if !m.isMigrationComplete(migration.Status) {
					return stork_api.SchedulePolicyTypeInvalid, false, nil
				}
			}
		}
	}

	for _, policyType := range stork_api.GetValidSchedulePolicyTypes() {
		var latestMigrationTimestamp meta.Time
		policyMigration, present := migrationSchedule.Status.Items[policyType]
		if present {
			for _, migration := range policyMigration {
				if latestMigrationTimestamp.Before(&migration.CreationTimestamp) {
					latestMigrationTimestamp = migration.CreationTimestamp
				}
			}
		}
		trigger, err := schedule.TriggerRequired(
			migrationSchedule.Spec.SchedulePolicyName,
			migrationSchedule.Namespace,
			policyType,
			latestMigrationTimestamp,
		)
		if err != nil {
			return stork_api.SchedulePolicyTypeInvalid, false, err
		}
		if trigger {
			return policyType, true, nil
		}
	}
	return stork_api.SchedulePolicyTypeInvalid, false, nil
}

func (m *MigrationScheduleController) formatMigrationName(
	migrationSchedule *stork_api.MigrationSchedule,
	policyType stork_api.SchedulePolicyType,
) string {
	return strings.Join([]string{migrationSchedule.Name,
		strings.ToLower(string(policyType)),
		time.Now().Format(nameTimeSuffixFormat)}, "-")
}

func (m *MigrationScheduleController) startMigration(
	migrationSchedule *stork_api.MigrationSchedule,
	policyType stork_api.SchedulePolicyType,
) error {
	migrationName := m.formatMigrationName(migrationSchedule, policyType)
	if migrationSchedule.Status.Items == nil {
		migrationSchedule.Status.Items = make(map[stork_api.SchedulePolicyType][]*stork_api.ScheduledMigrationStatus)
	}
	if migrationSchedule.Status.Items[policyType] == nil {
		migrationSchedule.Status.Items[policyType] = make([]*stork_api.ScheduledMigrationStatus, 0)
	}
	migrationSchedule.Status.Items[policyType] = append(migrationSchedule.Status.Items[policyType],
		&stork_api.ScheduledMigrationStatus{
			Name:              migrationName,
			CreationTimestamp: meta.NewTime(schedule.GetCurrentTime()),
			Status:            stork_api.MigrationStatusPending,
		})
	err := m.client.Update(context.TODO(), migrationSchedule)
	if err != nil {
		return err
	}

	migration := &stork_api.Migration{
		ObjectMeta: meta.ObjectMeta{
			Name:      migrationName,
			Namespace: migrationSchedule.Namespace,
			OwnerReferences: []meta.OwnerReference{
				{
					Name:       migrationSchedule.Name,
					UID:        migrationSchedule.UID,
					Kind:       migrationSchedule.GetObjectKind().GroupVersionKind().Kind,
					APIVersion: migrationSchedule.GetObjectKind().GroupVersionKind().GroupVersion().String(),
				},
			},
		},
		Spec: migrationSchedule.Spec.Template.Spec,
	}

	if migration.Annotations == nil {
		migration.Annotations = make(map[string]string)
	}
	for k, v := range migrationSchedule.Annotations {
		migration.Annotations[k] = v
	}
	migration.Annotations[StorkMigrationName] = migrationSchedule.GetName()
	log.MigrationScheduleLog(migrationSchedule).Infof("Starting migration %s", migrationName)
	_, err = storkops.Instance().CreateMigration(migration)
	return err
}

func (m *MigrationScheduleController) pruneMigrations(migrationSchedule *stork_api.MigrationSchedule) error {
	updated := false
	for policyType, policyMigration := range migrationSchedule.Status.Items {
		// Keep only one successful migration status and all failed migrations
		// until there is a successful one
		numMigrations := len(policyMigration)
		deleteBefore := 0
		if numMigrations > 1 {
			// Start from the end and find the last successful migration
			for i := range policyMigration {
				if policyMigration[(numMigrations-i-1)].Status == stork_api.MigrationStatusSuccessful ||
					policyMigration[(numMigrations-i-1)].Status == stork_api.MigrationStatusPartialSuccess {
					deleteBefore = numMigrations - i - 1
					break
				}
			}
			for i := 0; i < deleteBefore; i++ {
				err := storkops.Instance().DeleteMigration(policyMigration[i].Name, migrationSchedule.Namespace)
				if err != nil {
					log.MigrationScheduleLog(migrationSchedule).Warnf("Error deleting %v: %v", policyMigration[i].Name, err)
				}
			}
			migrationSchedule.Status.Items[policyType] = policyMigration[deleteBefore:]
			if deleteBefore > 0 {
				updated = true
			}
		}
	}
	if updated {
		return m.client.Update(context.TODO(), migrationSchedule)
	}
	return nil

}

func (m *MigrationScheduleController) deleteMigrations(migrationSchedule *stork_api.MigrationSchedule) error {
	var lastError error
	for _, policyMigration := range migrationSchedule.Status.Items {
		for _, migration := range policyMigration {
			err := storkops.Instance().DeleteMigration(migration.Name, migrationSchedule.Namespace)
			if err != nil && !errors.IsNotFound(err) {
				log.MigrationScheduleLog(migrationSchedule).Warnf("Error deleting %v: %v", migration.Name, err)
				lastError = err
			}
		}
	}
	return lastError
}

func (m *MigrationScheduleController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.MigrationScheduleResourceName,
		Plural:  stork_api.MigrationScheduleResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.MigrationSchedule{}).Name(),
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

func getMigratedAppStatus(migrationSchedule *stork_api.MigrationSchedule) (bool, error) {
	for _, ns := range migrationSchedule.Spec.Template.Spec.Namespaces {
		deployList, err := apps.Instance().ListDeployments(ns, meta.ListOptions{LabelSelector: fmt.Sprintf("%v=%v", StorkMigrationAnnotation, "true")})
		if err != nil {
			return false, err
		}
		for _, deploy := range deployList.Items {
			logrus.Debugf("Checking deploy %v/%v", deploy.Name, deploy.GetLabels()[StorkMigrationAnnotation])
			if *deploy.Spec.Replicas > 0 {
				logrus.Warnf("Deploy active %v/%v", deploy.Name, deploy.Namespace)
				return true, nil
			}
			if deploy.Status.ObservedGeneration > 1 {
				logrus.Warnf("Deploy updated %v/%v", deploy.Name, deploy.Namespace)
				return true, nil
			}
		}

		// check statefulset status
		stsList, err := apps.Instance().ListStatefulSets(ns, meta.ListOptions{LabelSelector: fmt.Sprintf("%v=%v", StorkMigrationAnnotation, "true")})
		if err != nil {
			return false, err
		}
		for _, sts := range stsList.Items {
			if *sts.Spec.Replicas > 0 {
				return true, nil
			}
			if sts.Status.ObservedGeneration > 1 {
				logrus.Warnf("Deploy updated %v/%v", sts.Name, sts.Namespace)
				return true, nil
			}
		}
	}
	return false, nil
}
