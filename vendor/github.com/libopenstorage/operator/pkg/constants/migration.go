package constants

const (
	// AnnotationMigrationApproved is used to take user's approval for portworx migration from
	// old installation method to the operator managed method.
	AnnotationMigrationApproved = "portworx.io/migration-approved"
	// AnnotationPauseComponentMigration is used to control when to start component migration
	AnnotationPauseComponentMigration = "portworx.io/pause-component-migration"

	// LabelPortworxDaemonsetMigration is used as Kubernetes node label to mark the state
	// of the node wrt portworx daemonSet migration
	LabelPortworxDaemonsetMigration = "portworx.io/daemonset-migration"
	// LabelValueMigrationPending state in which migration is pending and daemonset pod still runs
	LabelValueMigrationPending = "Pending"
	// LabelValueMigrationStarting state in which migration is about to start but waiting for
	// daemonset pod to be removed
	LabelValueMigrationStarting = "Starting"
	// LabelValueMigrationInProgress state in which migration is about to start the operator
	// managed portworx pod
	LabelValueMigrationInProgress = "InProgress"
	// LabelValueMigrationDone state in which migration of portworx pod has finished on that node
	LabelValueMigrationDone = "Done"
	// LabelValueMigrationSkip state telling the operator to skip and not wait for portworx
	// to be ready on that node
	LabelValueMigrationSkip = "Skip"

	// PhaseAwaitingApproval status when operator is waiting for user approval for migration
	PhaseAwaitingApproval = "AwaitingMigrationApproval"
	// PhaseMigrationInProgress status when migration is in progress
	PhaseMigrationInProgress = "MigrationInProgress"

	// PortworxDaemonSetName name of portworx DaemonSet
	PortworxDaemonSetName = "portworx"
)
