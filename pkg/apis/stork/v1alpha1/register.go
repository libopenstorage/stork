package v1alpha1

import (
	"github.com/libopenstorage/stork/pkg/apis/stork"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: stork.GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder is the scheme builder for the types
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme applies all the stored functions to the scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

// Kind takes an unqualified kind and returns back a Group qualified GroupKind
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Rule{},
		&RuleList{},
		&ClusterPair{},
		&ClusterPairList{},
		&Migration{},
		&MigrationList{},
		&MigrationSchedule{},
		&MigrationScheduleList{},
		&GroupVolumeSnapshot{},
		&GroupVolumeSnapshotList{},
		&SchedulePolicy{},
		&SchedulePolicyList{},
		&NamespacedSchedulePolicy{},
		&NamespacedSchedulePolicyList{},
		&VolumeSnapshotSchedule{},
		&VolumeSnapshotScheduleList{},
		&ClusterDomainsStatus{},
		&ClusterDomainsStatusList{},
		&ClusterDomainUpdate{},
		&ClusterDomainUpdateList{},
		&ApplicationClone{},
		&ApplicationCloneList{},
		&ApplicationBackup{},
		&ApplicationBackupList{},
		&ApplicationRestore{},
		&ApplicationRestoreList{},
		&ApplicationRegistration{},
		&ApplicationRegistrationList{},
		&BackupLocation{},
		&BackupLocationList{},
		&VolumeSnapshotRestore{},
		&VolumeSnapshotRestoreList{},
		&ApplicationBackupSchedule{},
		&ApplicationBackupScheduleList{},
		&DataExport{},
		&DataExportList{},
	)

	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func init() {
	SchemeBuilder.Register(addKnownTypes)
}
