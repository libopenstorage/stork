package tests

type TestCaseLabel = string
type TestCaseName = string

// Test case names
const (
	CreateMultipleUsersAndGroups                                     TestCaseName = "CreateMultipleUsersAndGroups"
	DuplicateSharedBackup                                            TestCaseName = "DuplicateSharedBackup"
	DifferentAccessSameUser                                          TestCaseName = "DifferentAccessSameUser"
	ShareBackupWithUsersAndGroups                                    TestCaseName = "ShareBackupWithUsersAndGroups"
	ShareLargeNumberOfBackupsWithLargeNumberOfUsers                  TestCaseName = "ShareLargeNumberOfBackupsWithLargeNumberOfUsers"
	CancelClusterBackupShare                                         TestCaseName = "CancelClusterBackupShare"
	ShareBackupAndEdit                                               TestCaseName = "ShareBackupAndEdit"
	SharedBackupDelete                                               TestCaseName = "SharedBackupDelete"
	ClusterBackupShareToggle                                         TestCaseName = "ClusterBackupShareToggle"
	ShareBackupsAndClusterWithUser                                   TestCaseName = "ShareBackupsAndClusterWithUser"
	ShareBackupWithDifferentRoleUsers                                TestCaseName = "ShareBackupWithDifferentRoleUsers"
	DeleteSharedBackup                                               TestCaseName = "DeleteSharedBackup"
	ShareAndRemoveBackupLocation                                     TestCaseName = "ShareAndRemoveBackupLocation"
	ViewOnlyFullBackupRestoreIncrementalBackup                       TestCaseName = "ViewOnlyFullBackupRestoreIncrementalBackup"
	IssueMultipleRestoresWithNamespaceAndStorageClassMapping         TestCaseName = "IssueMultipleRestoresWithNamespaceAndStorageClassMapping"
	DeleteUsersRole                                                  TestCaseName = "DeleteUsersRole"
	IssueMultipleDeletesForSharedBackup                              TestCaseName = "IssueMultipleDeletesForSharedBackup"
	SwapShareBackup                                                  TestCaseName = "SwapShareBackup"
	NamespaceLabelledBackupSharedWithDifferentAccessMode             TestCaseName = "NamespaceLabelledBackupSharedWithDifferentAccessMode"
	BackupScheduleForOldAndNewNS                                     TestCaseName = "BackupScheduleForOldAndNewNS"
	ManualAndScheduledBackupUsingNamespaceAndResourceLabel           TestCaseName = "ManualAndScheduledBackupUsingNamespaceAndResourceLabel"
	ScheduleBackupWithAdditionAndRemovalOfNS                         TestCaseName = "ScheduleBackupWithAdditionAndRemovalOfNS"
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit              TestCaseName = "ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit"
	ManualAndScheduleBackupUsingNamespaceLabel                       TestCaseName = "ManualAndScheduleBackupUsingNamespaceLabel"
	NamespaceLabelledBackupOfEmptyNamespace                          TestCaseName = "NamespaceLabelledBackupOfEmptyNamespace"
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgress              TestCaseName = "DeleteNfsExecutorPodWhileBackupAndRestoreInProgress"
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject TestCaseName = "SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject"
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestore         TestCaseName = "NamespaceMoveFromProjectToProjectToNoProjectWhileRestore"
	MultipleProjectsAndNamespacesBackupAndRestore                    TestCaseName = "MultipleProjectsAndNamespacesBackupAndRestore"
	BackupRestartPX                                                  TestCaseName = "BackupRestartPX"
	KillStorkWithBackupsAndRestoresInProgress                        TestCaseName = "KillStorkWithBackupsAndRestoresInProgress"
	RestartBackupPodDuringBackupSharing                              TestCaseName = "RestartBackupPodDuringBackupSharing"
	CancelAllRunningBackupJobs                                       TestCaseName = "CancelAllRunningBackupJobs"
	ScaleMongoDBWhileBackupAndRestore                                TestCaseName = "ScaleMongoDBWhileBackupAndRestore"
	RebootNodesWhenBackupsAreInProgress                              TestCaseName = "RebootNodesWhenBackupsAreInProgress"
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress            TestCaseName = "ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress"
	CancelAllRunningRestoreJobs                                      TestCaseName = "CancelAllRunningRestoreJobs"
	DeleteSameNameObjectsByMultipleUsersFromAdmin                    TestCaseName = "DeleteSameNameObjectsByMultipleUsersFromAdmin"
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin TestCaseName = "DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin"
	DeleteObjectsByMultipleUsersFromNewAdmin                         TestCaseName = "DeleteObjectsByMultipleUsersFromNewAdmin"
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin            TestCaseName = "DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin"
	DeleteSharedBackupOfUserFromAdmin                                TestCaseName = "DeleteSharedBackupOfUserFromAdmin"
	DeleteBackupOfUserNonSharedRBAC                                  TestCaseName = "DeleteBackupOfUserNonSharedRBAC"
	DeleteBackupOfUserSharedRBAC                                     TestCaseName = "DeleteBackupOfUserSharedRBAC"
	UpdatesBackupOfUserFromAdmin                                     TestCaseName = "UpdatesBackupOfUserFromAdmin"
	DeleteBackupSharedByMultipleUsersFromAdmin                       TestCaseName = "DeleteBackupSharedByMultipleUsersFromAdmin"
	NodeCountForLicensing                                            TestCaseName = "NodeCountForLicensing"
	LicensingCountWithNodeLabelledBeforeClusterAddition              TestCaseName = "LicensingCountWithNodeLabelledBeforeClusterAddition"
	LicensingCountBeforeAndAfterBackupPodRestart                     TestCaseName = "LicensingCountBeforeAndAfterBackupPodRestart"
	BackupLocationWithEncryptionKey                                  TestCaseName = "BackupLocationWithEncryptionKey"
	ReplicaChangeWhileRestore                                        TestCaseName = "ReplicaChangeWhileRestore"
	ResizeOnRestoredVolume                                           TestCaseName = "ResizeOnRestoredVolume"
	RestoreEncryptedAndNonEncryptedBackups                           TestCaseName = "RestoreEncryptedAndNonEncryptedBackups"
	ResizeVolumeOnScheduleBackup                                     TestCaseName = "ResizeVolumeOnScheduleBackup"
	BackupClusterVerification                                        TestCaseName = "BackupClusterVerification"
	UserGroupManagement                                              TestCaseName = "UserGroupManagement"
	BasicBackupCreation                                              TestCaseName = "BasicBackupCreation"
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy     TestCaseName = "CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy"
	BasicSelectiveRestore                                            TestCaseName = "BasicSelectiveRestore"
	CustomResourceBackupAndRestore                                   TestCaseName = "CustomResourceBackupAndRestore"
	DeleteAllBackupObjects                                           TestCaseName = "DeleteAllBackupObjects"
	ScheduleBackupCreationAllNS                                      TestCaseName = "ScheduleBackupCreationAllNS"
	CustomResourceRestore                                            TestCaseName = "CustomResourceRestore"
	AllNSBackupWithIncludeNewNSOption                                TestCaseName = "AllNSBackupWithIncludeNewNSOption"
	BackupSyncBasicTest                                              TestCaseName = "BackupSyncBasicTest"
	BackupMultipleNsWithSameLabel                                    TestCaseName = "BackupMultipleNsWithSameLabel"
	MultipleCustomRestoreSameTimeDiffStorageClassMapping             TestCaseName = "MultipleCustomRestoreSameTimeDiffStorageClassMapping"
	AddMultipleNamespaceLabels                                       TestCaseName = "AddMultipleNamespaceLabels"
	MultipleInPlaceRestoreSameTime                                   TestCaseName = "MultipleInPlaceRestoreSameTime"
	CloudSnapsSafeWhenBackupLocationDeleteTest                       TestCaseName = "CloudSnapsSafeWhenBackupLocationDeleteTest"
	SetUnsetNSLabelDuringScheduleBackup                              TestCaseName = "SetUnsetNSLabelDuringScheduleBackup"
	BackupRestoreOnDifferentK8sVersions                              TestCaseName = "BackupRestoreOnDifferentK8sVersions"
	BackupCRsThenMultipleRestoresOnHigherK8sVersion                  TestCaseName = "BackupCRsThenMultipleRestoresOnHigherK8sVersion"
	ScheduleBackupDeleteAndRecreateNS                                TestCaseName = "ScheduleBackupDeleteAndRecreateNS"
	DeleteNSDeleteClusterRestore                                     TestCaseName = "DeleteNSDeleteClusterRestore"
	AlternateBackupBetweenNfsAndS3                                   TestCaseName = "AlternateBackupBetweenNfsAndS3"
	BackupNamespaceInNfsRestoredFromS3                               TestCaseName = "BackupNamespaceInNfsRestoredFromS3"
	DeleteS3ScheduleAndCreateNfsSchedule                             TestCaseName = "DeleteS3ScheduleAndCreateNfsSchedule"
	KubeAndPxNamespacesSkipOnAllNSBackup                             TestCaseName = "KubeAndPxNamespacesSkipOnAllNSBackup"
	MultipleBackupLocationWithSameEndpoint                           TestCaseName = "MultipleBackupLocationWithSameEndpoint"
	UpgradePxBackup                                                  TestCaseName = "UpgradePxBackupWithHelm"
	StorkUpgradeWithBackup                                           TestCaseName = "StorkUpgradeWithBackup"
	PXBackupEndToEndBackupAndRestoreWithUpgrade                      TestCaseName = "PXBackupEndToEndBackupAndRestoreWithUpgrade"
	IssueDeleteOfIncrementalBackupsAndRestore                        TestCaseName = "IssueDeleteOfIncrementalBackupsAndRestore"
	DeleteIncrementalBackupsAndRecreateNew                           TestCaseName = "DeleteIncrementalBackupsAndRecreateNew"
	DeleteBucketVerifyCloudBackupMissing                             TestCaseName = "DeleteBucketVerifyCloudBackupMissing"
	DeleteBackupAndCheckIfBucketIsEmpty                              TestCaseName = "DeleteBackupAndCheckIfBucketIsEmpty"
	KubevirtVMBackupRestoreWithDifferentStates                       TestCaseName = "KubevirtVMBackupRestoreWithDifferentStates"
	KubevirtUpgradeTest                                              TestCaseName = "KubevirtUpgradeTest"
	KubevirtVMBackupOrDeletionInProgress                             TestCaseName = "KubevirtVMBackupOrDeletionInProgress"
	KubevirtVMBackupRestoreWithNodeSelector                          TestCaseName = "KubevirtVMBackupRestoreWithNodeSelector"
	KubevirtVMWithFreezeUnfreeze                                     TestCaseName = "KubevirtVMWithFreezeUnfreeze"
	KubevirtInPlaceRestoreWithReplaceAndRetain                       TestCaseName = "KubevirtInPlaceRestoreWithReplaceAndRetain"
	KubevirtVMRestoreWithAfterChangingVMConfig                       TestCaseName = "KubevirtVMRestoreWithAfterChangingVMConfig"
	BackupAlternatingBetweenLockedAndUnlockedBuckets                 TestCaseName = "BackupAlternatingBetweenLockedAndUnlockedBuckets"
	LockedBucketResizeOnRestoredVolume                               TestCaseName = "LockedBucketResizeOnRestoredVolume"
	LockedBucketResizeVolumeOnScheduleBackup                         TestCaseName = "LockedBucketResizeVolumeOnScheduleBackup"
	DeleteLockedBucketUserObjectsFromAdmin                           TestCaseName = "DeleteLockedBucketUserObjectsFromAdmin"
	VerifyRBACForInfraAdmin                                          TestCaseName = "VerifyRBACForInfraAdmin"
	VerifyRBACForPxAdmin                                             TestCaseName = "VerifyRBACForPxAdmin"
	VerifyRBACForAppAdmin                                            TestCaseName = "VerifyRBACForAppAdmin"
	VerifyRBACForAppUser                                             TestCaseName = "VerifyRBACForAppUser"
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNS                 TestCaseName = "DefaultBackupRestoreWithKubevirtAndNonKubevirtNS"
)

// Test case labels
const (
	CreateMultipleUsersAndGroupsLabel                                     TestCaseLabel = "CreateMultipleUsersAndGroups"
	DuplicateSharedBackupLabel                                            TestCaseLabel = "DuplicateSharedBackup"
	DifferentAccessSameUserLabel                                          TestCaseLabel = "DifferentAccessSameUser"
	ShareBackupWithUsersAndGroupsLabel                                    TestCaseLabel = "ShareBackupWithUsersAndGroups"
	ShareLargeNumberOfBackupsWithLargeNumberOfUsersLabel                  TestCaseLabel = "ShareLargeNumberOfBackupsWithLargeNumberOfUsers"
	CancelClusterBackupShareLabel                                         TestCaseLabel = "CancelClusterBackupShare"
	ShareBackupAndEditLabel                                               TestCaseLabel = "ShareBackupAndEdit"
	SharedBackupDeleteLabel                                               TestCaseLabel = "SharedBackupDelete"
	ClusterBackupShareToggleLabel                                         TestCaseLabel = "ClusterBackupShareToggle"
	ShareBackupsAndClusterWithUserLabel                                   TestCaseLabel = "ShareBackupsAndClusterWithUser"
	ShareBackupWithDifferentRoleUsersLabel                                TestCaseLabel = "ShareBackupWithDifferentRoleUsers"
	DeleteSharedBackupLabel                                               TestCaseLabel = "DeleteSharedBackup"
	ShareAndRemoveBackupLocationLabel                                     TestCaseLabel = "ShareAndRemoveBackupLocation"
	ViewOnlyFullBackupRestoreIncrementalBackupLabel                       TestCaseLabel = "ViewOnlyFullBackupRestoreIncrementalBackup"
	IssueMultipleRestoresWithNamespaceAndStorageClassMappingLabel         TestCaseLabel = "IssueMultipleRestoresWithNamespaceAndStorageClassMapping"
	DeleteUsersRoleLabel                                                  TestCaseLabel = "DeleteUsersRole"
	IssueMultipleDeletesForSharedBackupLabel                              TestCaseLabel = "IssueMultipleDeletesForSharedBackup"
	SwapShareBackupLabel                                                  TestCaseLabel = "SwapShareBackup"
	NamespaceLabelledBackupSharedWithDifferentAccessModeLabel             TestCaseLabel = "NamespaceLabelledBackupSharedWithDifferentAccessMode"
	BackupScheduleForOldAndNewNSLabel                                     TestCaseLabel = "BackupScheduleForOldAndNewNS"
	ManualAndScheduledBackupUsingNamespaceAndResourceLabelLabel           TestCaseLabel = "ManualAndScheduledBackupUsingNamespaceAndResourceLabel"
	ScheduleBackupWithAdditionAndRemovalOfNSLabel                         TestCaseLabel = "ScheduleBackupWithAdditionAndRemovalOfNS"
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimitLabel              TestCaseLabel = "ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit"
	ManualAndScheduleBackupUsingNamespaceLabelLabel                       TestCaseLabel = "ManualAndScheduleBackupUsingNamespaceLabel"
	NamespaceLabelledBackupOfEmptyNamespaceLabel                          TestCaseLabel = "NamespaceLabelledBackupOfEmptyNamespace"
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgressLabel              TestCaseLabel = "DeleteNfsExecutorPodWhileBackupAndRestoreInProgress"
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProjectLabel TestCaseLabel = "SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject"
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestoreLabel         TestCaseLabel = "NamespaceMoveFromProjectToProjectToNoProjectWhileRestore"
	MultipleProjectsAndNamespacesBackupAndRestoreLabel                    TestCaseLabel = "MultipleProjectsAndNamespacesBackupAndRestore"
	BackupRestartPXLabel                                                  TestCaseLabel = "BackupRestartPX"
	KillStorkWithBackupsAndRestoresInProgressLabel                        TestCaseLabel = "KillStorkWithBackupsAndRestoresInProgress"
	RestartBackupPodDuringBackupSharingLabel                              TestCaseLabel = "RestartBackupPodDuringBackupSharing"
	CancelAllRunningBackupJobsLabel                                       TestCaseLabel = "CancelAllRunningBackupJobs"
	ScaleMongoDBWhileBackupAndRestoreLabel                                TestCaseLabel = "ScaleMongoDBWhileBackupAndRestore"
	RebootNodesWhenBackupsAreInProgressLabel                              TestCaseLabel = "RebootNodesWhenBackupsAreInProgress"
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgressLabel            TestCaseLabel = "ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress"
	CancelAllRunningRestoreJobsLabel                                      TestCaseLabel = "CancelAllRunningRestoreJobs"
	DeleteSameNameObjectsByMultipleUsersFromAdminLabel                    TestCaseLabel = "DeleteSameNameObjectsByMultipleUsersFromAdmin"
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdminLabel TestCaseLabel = "DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin"
	DeleteObjectsByMultipleUsersFromNewAdminLabel                         TestCaseLabel = "DeleteObjectsByMultipleUsersFromNewAdmin"
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdminLabel            TestCaseLabel = "DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin"
	DeleteSharedBackupOfUserFromAdminLabel                                TestCaseLabel = "DeleteSharedBackupOfUserFromAdmin"
	DeleteBackupOfUserNonSharedRBACLabel                                  TestCaseLabel = "DeleteBackupOfUserNonSharedRBAC"
	DeleteBackupOfUserSharedRBACLabel                                     TestCaseLabel = "DeleteBackupOfUserSharedRBAC"
	UpdatesBackupOfUserFromAdminLabel                                     TestCaseLabel = "UpdatesBackupOfUserFromAdmin"
	DeleteBackupSharedByMultipleUsersFromAdminLabel                       TestCaseLabel = "DeleteBackupSharedByMultipleUsersFromAdmin"
	NodeCountForLicensingLabel                                            TestCaseLabel = "NodeCountForLicensing"
	LicensingCountWithNodeLabelledBeforeClusterAdditionLabel              TestCaseLabel = "LicensingCountWithNodeLabelledBeforeClusterAddition"
	LicensingCountBeforeAndAfterBackupPodRestartLabel                     TestCaseLabel = "LicensingCountBeforeAndAfterBackupPodRestart"
	BackupLocationWithEncryptionKeyLabel                                  TestCaseLabel = "BackupLocationWithEncryptionKey"
	ReplicaChangeWhileRestoreLabel                                        TestCaseLabel = "ReplicaChangeWhileRestore"
	ResizeOnRestoredVolumeLabel                                           TestCaseLabel = "ResizeOnRestoredVolume"
	RestoreEncryptedAndNonEncryptedBackupsLabel                           TestCaseLabel = "RestoreEncryptedAndNonEncryptedBackups"
	ResizeVolumeOnScheduleBackupLabel                                     TestCaseLabel = "ResizeVolumeOnScheduleBackup"
	BackupClusterVerificationLabel                                        TestCaseLabel = "BackupClusterVerification"
	UserGroupManagementLabel                                              TestCaseLabel = "UserGroupManagement"
	BasicBackupCreationLabel                                              TestCaseLabel = "BasicBackupCreation"
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicyLabel     TestCaseLabel = "CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy"
	BasicSelectiveRestoreLabel                                            TestCaseLabel = "BasicSelectiveRestore"
	CustomResourceBackupAndRestoreLabel                                   TestCaseLabel = "CustomResourceBackupAndRestore"
	DeleteAllBackupObjectsLabel                                           TestCaseLabel = "DeleteAllBackupObjects"
	ScheduleBackupCreationAllNSLabel                                      TestCaseLabel = "ScheduleBackupCreationAllNS"
	CustomResourceRestoreLabel                                            TestCaseLabel = "CustomResourceRestore"
	AllNSBackupWithIncludeNewNSOptionLabel                                TestCaseLabel = "AllNSBackupWithIncludeNewNSOption"
	BackupSyncBasicTestLabel                                              TestCaseLabel = "BackupSyncBasicTest"
	BackupMultipleNsWithSameLabelLabel                                    TestCaseLabel = "BackupMultipleNsWithSameLabel"
	MultipleCustomRestoreSameTimeDiffStorageClassMappingLabel             TestCaseLabel = "MultipleCustomRestoreSameTimeDiffStorageClassMapping"
	AddMultipleNamespaceLabelsLabel                                       TestCaseLabel = "AddMultipleNamespaceLabels"
	MultipleInPlaceRestoreSameTimeLabel                                   TestCaseLabel = "MultipleInPlaceRestoreSameTime"
	CloudSnapsSafeWhenBackupLocationDeleteTestLabel                       TestCaseLabel = "CloudSnapsSafeWhenBackupLocationDeleteTest"
	SetUnsetNSLabelDuringScheduleBackupLabel                              TestCaseLabel = "SetUnsetNSLabelDuringScheduleBackup"
	BackupRestoreOnDifferentK8sVersionsLabel                              TestCaseLabel = "BackupRestoreOnDifferentK8sVersions"
	BackupCRsThenMultipleRestoresOnHigherK8sVersionLabel                  TestCaseLabel = "BackupCRsThenMultipleRestoresOnHigherK8sVersion"
	ScheduleBackupDeleteAndRecreateNSLabel                                TestCaseLabel = "ScheduleBackupDeleteAndRecreateNS"
	DeleteNSDeleteClusterRestoreLabel                                     TestCaseLabel = "DeleteNSDeleteClusterRestore"
	AlternateBackupBetweenNfsAndS3Label                                   TestCaseLabel = "AlternateBackupBetweenNfsAndS3"
	BackupNamespaceInNfsRestoredFromS3Label                               TestCaseLabel = "BackupNamespaceInNfsRestoredFromS3"
	DeleteS3ScheduleAndCreateNfsScheduleLabel                             TestCaseLabel = "DeleteS3ScheduleAndCreateNfsSchedule"
	KubeAndPxNamespacesSkipOnAllNSBackupLabel                             TestCaseLabel = "KubeAndPxNamespacesSkipOnAllNSBackup"
	MultipleBackupLocationWithSameEndpointLabel                           TestCaseLabel = "MultipleBackupLocationWithSameEndpoint"
	UpgradePxBackupLabel                                                  TestCaseLabel = "UpgradePxBackupWithHelm"
	StorkUpgradeWithBackupLabel                                           TestCaseLabel = "StorkUpgradeWithBackup"
	PXBackupEndToEndBackupAndRestoreWithUpgradeLabel                      TestCaseLabel = "PXBackupEndToEndBackupAndRestoreWithUpgrade"
	IssueDeleteOfIncrementalBackupsAndRestoreLabel                        TestCaseLabel = "IssueDeleteOfIncrementalBackupsAndRestore"
	DeleteIncrementalBackupsAndRecreateNewLabel                           TestCaseLabel = "DeleteIncrementalBackupsAndRecreateNew"
	DeleteBucketVerifyCloudBackupMissingLabel                             TestCaseLabel = "DeleteBucketVerifyCloudBackupMissing"
	DeleteBackupAndCheckIfBucketIsEmptyLabel                              TestCaseLabel = "DeleteBackupAndCheckIfBucketIsEmpty"
	KubevirtVMBackupRestoreWithDifferentStatesLabel                       TestCaseLabel = "KubevirtVMBackupRestoreWithDifferentStates"
	BackupAlternatingBetweenLockedAndUnlockedBucketsLabel                 TestCaseLabel = "BackupAlternatingBetweenLockedAndUnlockedBuckets"
	LockedBucketResizeOnRestoredVolumeLabel                               TestCaseLabel = "LockedBucketResizeOnRestoredVolume"
	LockedBucketResizeVolumeOnScheduleBackupLabel                         TestCaseLabel = "LockedBucketResizeVolumeOnScheduleBackup"
	DeleteLockedBucketUserObjectsFromAdminLabel                           TestCaseLabel = "DeleteLockedBucketUserObjectsFromAdmin"
	VerifyRBACForInfraAdminLabel                                          TestCaseLabel = "VerifyRBACForInfraAdmin"
	VerifyRBACForPxAdminLabel                                             TestCaseLabel = "VerifyRBACForPxAdmin"
	VerifyRBACForAppAdminLabel                                            TestCaseLabel = "VerifyRBACForAppAdmin"
	VerifyRBACForAppUserLabel                                             TestCaseLabel = "VerifyRBACForAppUser"
	KubevirtUpgradeTestLabel                                              TestCaseLabel = "KubevirtUpgradeTest"
	KubevirtVMBackupOrDeletionInProgressLabel                             TestCaseLabel = "KubevirtVMBackupOrDeletionInProgress"
	KubevirtVMBackupRestoreWithNodeSelectorLabel                          TestCaseLabel = "KubevirtVMBackupRestoreWithNodeSelector"
	KubevirtVMWithFreezeUnfreezeLabel                                     TestCaseLabel = "KubevirtVMWithFreezeUnfreeze"
	KubevirtInPlaceRestoreWithReplaceAndRetainLabel                       TestCaseLabel = "KubevirtInPlaceRestoreWithReplaceAndRetain"
	KubevirtVMRestoreWithAfterChangingVMConfigLabel                       TestCaseLabel = "KubevirtVMRestoreWithAfterChangingVMConfig"
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNSLabel                 TestCaseLabel = "DefaultBackupRestoreWithKubevirtAndNonKubevirtNS"
)

// Common Labels
const (
	PxBackupLabel = "px-backup"
)

// Priority labels
const (
	P0 = "p0"
	P1 = "p1"
	P2 = "p2"
)

// Test type labels
const (
	SystemTest      = "system-test"
	ScaleTest       = "scale-test"
	LongevityTest   = "longevity-test"
	PerformanceTest = "performance-test"
)

// Platform labels
const (
	Vanilla     = "vanilla"
	Openshift   = "openshift"
	Rancher     = "rancher"
	AKS         = "aks"
	EKS         = "eks"
	GKE         = "gke"
	IKS         = "iks"
	AnyPlatform = "any-platform"
)

// Parallel/Non-Parallel labels
const (
	ParallelLabel    = "parallel"
	NonParallelLabel = "non-parallel"
)

// Test duration labels
const (
	Slow = "slow"
	Fast = "fast"
)

// Backup labels
const (
	PxLabel   = "px"
	KDMPLabel = "kdmp"
	CsiLabel  = "csi"
	AnyBackup = "any-backup"
)

// StorkQualificationLabel Stork qualification labels
const (
	StorkQualificationLabel = "stork-qualification"
)

// Sanity labels
const (
	SanityLabel = "sanity"
)

// Disruptive labels
const (
	DisruptiveLabel = "disruptive"
)

// Skip test labels
const (
	SkipTestLabel = "skip-test"
)

// Volume labels
const (
	PortworxVolumeLabel = "portworx-volume"
	EBSVolumeLabel      = "ebs-volume"
	AnyVolumeLabel      = "any-volume"
)

// Backup location labels
const (
	NfsBackupLocationLabel = "nfs"
	S3BackupLocationLabel  = "s3"
)

// App labels
const (
	KubevirtAppLabel = "kubevirt-app"
)

var TestCaseLabelsMap = map[TestCaseName][]TestCaseLabel{
	CreateMultipleUsersAndGroups:                                     {CreateMultipleUsersAndGroupsLabel},
	DuplicateSharedBackup:                                            {DuplicateSharedBackupLabel},
	DifferentAccessSameUser:                                          {DifferentAccessSameUserLabel},
	ShareBackupWithUsersAndGroups:                                    {ShareBackupWithUsersAndGroupsLabel},
	ShareLargeNumberOfBackupsWithLargeNumberOfUsers:                  {ShareLargeNumberOfBackupsWithLargeNumberOfUsersLabel},
	CancelClusterBackupShare:                                         {CancelClusterBackupShareLabel},
	ShareBackupAndEdit:                                               {ShareBackupAndEditLabel},
	SharedBackupDelete:                                               {SharedBackupDeleteLabel},
	ClusterBackupShareToggle:                                         {ClusterBackupShareToggleLabel},
	ShareBackupsAndClusterWithUser:                                   {ShareBackupsAndClusterWithUserLabel},
	ShareBackupWithDifferentRoleUsers:                                {ShareBackupWithDifferentRoleUsersLabel},
	DeleteSharedBackup:                                               {DeleteSharedBackupLabel},
	ShareAndRemoveBackupLocation:                                     {ShareAndRemoveBackupLocationLabel},
	ViewOnlyFullBackupRestoreIncrementalBackup:                       {ViewOnlyFullBackupRestoreIncrementalBackupLabel},
	IssueMultipleRestoresWithNamespaceAndStorageClassMapping:         {IssueMultipleRestoresWithNamespaceAndStorageClassMappingLabel},
	DeleteUsersRole:                                                  {DeleteUsersRoleLabel},
	IssueMultipleDeletesForSharedBackup:                              {IssueMultipleDeletesForSharedBackupLabel},
	SwapShareBackup:                                                  {SwapShareBackupLabel},
	NamespaceLabelledBackupSharedWithDifferentAccessMode:             {NamespaceLabelledBackupSharedWithDifferentAccessModeLabel},
	BackupScheduleForOldAndNewNS:                                     {BackupScheduleForOldAndNewNSLabel},
	ManualAndScheduledBackupUsingNamespaceAndResourceLabel:           {ManualAndScheduledBackupUsingNamespaceAndResourceLabelLabel},
	ScheduleBackupWithAdditionAndRemovalOfNS:                         {ScheduleBackupWithAdditionAndRemovalOfNSLabel},
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit:              {ManualAndScheduleBackupUsingNSLabelWithMaxCharLimitLabel},
	ManualAndScheduleBackupUsingNamespaceLabel:                       {ManualAndScheduleBackupUsingNamespaceLabelLabel},
	NamespaceLabelledBackupOfEmptyNamespace:                          {NamespaceLabelledBackupOfEmptyNamespaceLabel},
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgress:              {DeleteNfsExecutorPodWhileBackupAndRestoreInProgressLabel},
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject: {SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProjectLabel},
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestore:         {NamespaceMoveFromProjectToProjectToNoProjectWhileRestoreLabel},
	MultipleProjectsAndNamespacesBackupAndRestore:                    {MultipleProjectsAndNamespacesBackupAndRestoreLabel},
	BackupRestartPX:                                                  {BackupRestartPXLabel},
	KillStorkWithBackupsAndRestoresInProgress:                        {KillStorkWithBackupsAndRestoresInProgressLabel},
	RestartBackupPodDuringBackupSharing:                              {RestartBackupPodDuringBackupSharingLabel},
	CancelAllRunningBackupJobs:                                       {CancelAllRunningBackupJobsLabel},
	ScaleMongoDBWhileBackupAndRestore:                                {ScaleMongoDBWhileBackupAndRestoreLabel},
	RebootNodesWhenBackupsAreInProgress:                              {RebootNodesWhenBackupsAreInProgressLabel},
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress:            {ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgressLabel},
	CancelAllRunningRestoreJobs:                                      {CancelAllRunningRestoreJobsLabel},
	DeleteSameNameObjectsByMultipleUsersFromAdmin:                    {DeleteSameNameObjectsByMultipleUsersFromAdminLabel},
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin: {DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdminLabel},
	DeleteObjectsByMultipleUsersFromNewAdmin:                         {DeleteObjectsByMultipleUsersFromNewAdminLabel},
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin:            {DeleteFailedInProgressBackupAndRestoreOfUserFromAdminLabel},
	DeleteSharedBackupOfUserFromAdmin:                                {DeleteSharedBackupOfUserFromAdminLabel},
	DeleteBackupOfUserNonSharedRBAC:                                  {DeleteBackupOfUserNonSharedRBACLabel},
	DeleteBackupOfUserSharedRBAC:                                     {DeleteBackupOfUserSharedRBACLabel},
	UpdatesBackupOfUserFromAdmin:                                     {UpdatesBackupOfUserFromAdminLabel},
	DeleteBackupSharedByMultipleUsersFromAdmin:                       {DeleteBackupSharedByMultipleUsersFromAdminLabel},
	NodeCountForLicensing:                                            {NodeCountForLicensingLabel},
	LicensingCountWithNodeLabelledBeforeClusterAddition:              {LicensingCountWithNodeLabelledBeforeClusterAdditionLabel},
	LicensingCountBeforeAndAfterBackupPodRestart:                     {LicensingCountBeforeAndAfterBackupPodRestartLabel},
	BackupLocationWithEncryptionKey:                                  {BackupLocationWithEncryptionKeyLabel},
	ReplicaChangeWhileRestore:                                        {ReplicaChangeWhileRestoreLabel},
	ResizeOnRestoredVolume:                                           {ResizeOnRestoredVolumeLabel},
	RestoreEncryptedAndNonEncryptedBackups:                           {RestoreEncryptedAndNonEncryptedBackupsLabel},
	ResizeVolumeOnScheduleBackup:                                     {ResizeVolumeOnScheduleBackupLabel},
	BackupClusterVerification:                                        {BackupClusterVerificationLabel},
	UserGroupManagement:                                              {UserGroupManagementLabel},
	BasicBackupCreation:                                              {BasicBackupCreationLabel},
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy:     {CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicyLabel},
	BasicSelectiveRestore:                                            {BasicSelectiveRestoreLabel},
	CustomResourceBackupAndRestore:                                   {CustomResourceBackupAndRestoreLabel},
	DeleteAllBackupObjects:                                           {DeleteAllBackupObjectsLabel},
	ScheduleBackupCreationAllNS:                                      {ScheduleBackupCreationAllNSLabel},
	CustomResourceRestore:                                            {CustomResourceRestoreLabel},
	AllNSBackupWithIncludeNewNSOption:                                {AllNSBackupWithIncludeNewNSOptionLabel},
	BackupSyncBasicTest:                                              {BackupSyncBasicTestLabel},
	BackupMultipleNsWithSameLabel:                                    {BackupMultipleNsWithSameLabelLabel},
	MultipleCustomRestoreSameTimeDiffStorageClassMapping:             {MultipleCustomRestoreSameTimeDiffStorageClassMappingLabel},
	AddMultipleNamespaceLabels:                                       {AddMultipleNamespaceLabelsLabel},
	MultipleInPlaceRestoreSameTime:                                   {MultipleInPlaceRestoreSameTimeLabel},
	CloudSnapsSafeWhenBackupLocationDeleteTest:                       {CloudSnapsSafeWhenBackupLocationDeleteTestLabel},
	SetUnsetNSLabelDuringScheduleBackup:                              {SetUnsetNSLabelDuringScheduleBackupLabel},
	BackupRestoreOnDifferentK8sVersions:                              {BackupRestoreOnDifferentK8sVersionsLabel},
	BackupCRsThenMultipleRestoresOnHigherK8sVersion:                  {BackupCRsThenMultipleRestoresOnHigherK8sVersionLabel},
	ScheduleBackupDeleteAndRecreateNS:                                {ScheduleBackupDeleteAndRecreateNSLabel},
	DeleteNSDeleteClusterRestore:                                     {DeleteNSDeleteClusterRestoreLabel},
	AlternateBackupBetweenNfsAndS3:                                   {AlternateBackupBetweenNfsAndS3Label},
	BackupNamespaceInNfsRestoredFromS3:                               {BackupNamespaceInNfsRestoredFromS3Label},
	DeleteS3ScheduleAndCreateNfsSchedule:                             {DeleteS3ScheduleAndCreateNfsScheduleLabel},
	KubeAndPxNamespacesSkipOnAllNSBackup:                             {KubeAndPxNamespacesSkipOnAllNSBackupLabel},
	MultipleBackupLocationWithSameEndpoint:                           {MultipleBackupLocationWithSameEndpointLabel},
	UpgradePxBackup:                                                  {UpgradePxBackupLabel},
	StorkUpgradeWithBackup:                                           {StorkUpgradeWithBackupLabel},
	PXBackupEndToEndBackupAndRestoreWithUpgrade:                      {PXBackupEndToEndBackupAndRestoreWithUpgradeLabel},
	IssueDeleteOfIncrementalBackupsAndRestore:                        {IssueDeleteOfIncrementalBackupsAndRestoreLabel},
	DeleteIncrementalBackupsAndRecreateNew:                           {DeleteIncrementalBackupsAndRecreateNewLabel},
	DeleteBucketVerifyCloudBackupMissing:                             {DeleteBucketVerifyCloudBackupMissingLabel},
	DeleteBackupAndCheckIfBucketIsEmpty:                              {DeleteBackupAndCheckIfBucketIsEmptyLabel},
	KubevirtVMBackupRestoreWithDifferentStates:                       {KubevirtVMBackupRestoreWithDifferentStatesLabel, KubevirtAppLabel},
	BackupAlternatingBetweenLockedAndUnlockedBuckets:                 {BackupAlternatingBetweenLockedAndUnlockedBucketsLabel},
	LockedBucketResizeOnRestoredVolume:                               {LockedBucketResizeOnRestoredVolumeLabel},
	LockedBucketResizeVolumeOnScheduleBackup:                         {LockedBucketResizeVolumeOnScheduleBackupLabel},
	DeleteLockedBucketUserObjectsFromAdmin:                           {DeleteLockedBucketUserObjectsFromAdminLabel},
	VerifyRBACForInfraAdmin:                                          {VerifyRBACForInfraAdminLabel},
	VerifyRBACForPxAdmin:                                             {VerifyRBACForPxAdminLabel},
	VerifyRBACForAppAdmin:                                            {VerifyRBACForAppAdminLabel},
	VerifyRBACForAppUser:                                             {VerifyRBACForAppUserLabel},
	KubevirtUpgradeTest:                                              {KubevirtUpgradeTestLabel, KubevirtAppLabel},
	KubevirtVMBackupOrDeletionInProgress:                             {KubevirtVMBackupOrDeletionInProgressLabel, KubevirtAppLabel},
	KubevirtVMBackupRestoreWithNodeSelector:                          {KubevirtVMBackupRestoreWithNodeSelectorLabel, KubevirtAppLabel},
	KubevirtVMWithFreezeUnfreeze:                                     {KubevirtVMWithFreezeUnfreezeLabel, KubevirtAppLabel},
	KubevirtInPlaceRestoreWithReplaceAndRetain:                       {KubevirtInPlaceRestoreWithReplaceAndRetainLabel, KubevirtAppLabel},
	KubevirtVMRestoreWithAfterChangingVMConfig:                       {KubevirtVMRestoreWithAfterChangingVMConfigLabel, KubevirtAppLabel},
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNS:                 {DefaultBackupRestoreWithKubevirtAndNonKubevirtNSLabel, KubevirtAppLabel},
}
