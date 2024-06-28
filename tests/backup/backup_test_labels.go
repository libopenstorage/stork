package tests

type TestCaseLabel = string
type TestCaseName = string

// Test case names
const (
	CreateMultipleUsersAndGroups                                                       TestCaseName = "CreateMultipleUsersAndGroups"
	DuplicateSharedBackup                                                              TestCaseName = "DuplicateSharedBackup"
	DifferentAccessSameUser                                                            TestCaseName = "DifferentAccessSameUser"
	ShareBackupWithUsersAndGroups                                                      TestCaseName = "ShareBackupWithUsersAndGroups"
	ShareLargeNumberOfBackupsWithLargeNumberOfUsers                                    TestCaseName = "ShareLargeNumberOfBackupsWithLargeNumberOfUsers"
	CancelClusterBackupShare                                                           TestCaseName = "CancelClusterBackupShare"
	ShareBackupAndEdit                                                                 TestCaseName = "ShareBackupAndEdit"
	SharedBackupDelete                                                                 TestCaseName = "SharedBackupDelete"
	ClusterBackupShareToggle                                                           TestCaseName = "ClusterBackupShareToggle"
	ShareBackupsAndClusterWithUser                                                     TestCaseName = "ShareBackupsAndClusterWithUser"
	ShareBackupWithDifferentRoleUsers                                                  TestCaseName = "ShareBackupWithDifferentRoleUsers"
	DeleteSharedBackup                                                                 TestCaseName = "DeleteSharedBackup"
	ShareAndRemoveBackupLocation                                                       TestCaseName = "ShareAndRemoveBackupLocation"
	ViewOnlyFullBackupRestoreIncrementalBackup                                         TestCaseName = "ViewOnlyFullBackupRestoreIncrementalBackup"
	IssueMultipleRestoresWithNamespaceAndStorageClassMapping                           TestCaseName = "IssueMultipleRestoresWithNamespaceAndStorageClassMapping"
	DeleteUsersRole                                                                    TestCaseName = "DeleteUsersRole"
	IssueMultipleDeletesForSharedBackup                                                TestCaseName = "IssueMultipleDeletesForSharedBackup"
	SwapShareBackup                                                                    TestCaseName = "SwapShareBackup"
	NamespaceLabelledBackupSharedWithDifferentAccessMode                               TestCaseName = "NamespaceLabelledBackupSharedWithDifferentAccessMode"
	BackupScheduleForOldAndNewNS                                                       TestCaseName = "BackupScheduleForOldAndNewNS"
	ManualAndScheduledBackupUsingNamespaceAndResourceLabel                             TestCaseName = "ManualAndScheduledBackupUsingNamespaceAndResourceLabel"
	ScheduleBackupWithAdditionAndRemovalOfNS                                           TestCaseName = "ScheduleBackupWithAdditionAndRemovalOfNS"
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit                                TestCaseName = "ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit"
	NamespaceLabelledBackupOfEmptyNamespace                                            TestCaseName = "NamespaceLabelledBackupOfEmptyNamespace"
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgress                                TestCaseName = "DeleteNfsExecutorPodWhileBackupAndRestoreInProgress"
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject                   TestCaseName = "SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject"
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestore                           TestCaseName = "NamespaceMoveFromProjectToProjectToNoProjectWhileRestore"
	MultipleProjectsAndNamespacesBackupAndRestore                                      TestCaseName = "MultipleProjectsAndNamespacesBackupAndRestore"
	BackupRestartPX                                                                    TestCaseName = "BackupRestartPX"
	KillStorkWithBackupsAndRestoresInProgress                                          TestCaseName = "KillStorkWithBackupsAndRestoresInProgress"
	RestartBackupPodDuringBackupSharing                                                TestCaseName = "RestartBackupPodDuringBackupSharing"
	CancelAllRunningBackupJobs                                                         TestCaseName = "CancelAllRunningBackupJobs"
	ScaleMongoDBWhileBackupAndRestore                                                  TestCaseName = "ScaleMongoDBWhileBackupAndRestore"
	RebootNodesWhenBackupsAreInProgress                                                TestCaseName = "RebootNodesWhenBackupsAreInProgress"
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress                              TestCaseName = "ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress"
	CancelAllRunningRestoreJobs                                                        TestCaseName = "CancelAllRunningRestoreJobs"
	DeleteSameNameObjectsByMultipleUsersFromAdmin                                      TestCaseName = "DeleteSameNameObjectsByMultipleUsersFromAdmin"
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin                   TestCaseName = "DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin"
	DeleteObjectsByMultipleUsersFromNewAdmin                                           TestCaseName = "DeleteObjectsByMultipleUsersFromNewAdmin"
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin                              TestCaseName = "DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin"
	DeleteSharedBackupOfUserFromAdmin                                                  TestCaseName = "DeleteSharedBackupOfUserFromAdmin"
	DeleteBackupOfUserNonSharedRBAC                                                    TestCaseName = "DeleteBackupOfUserNonSharedRBAC"
	DeleteBackupOfUserSharedRBAC                                                       TestCaseName = "DeleteBackupOfUserSharedRBAC"
	UpdatesBackupOfUserFromAdmin                                                       TestCaseName = "UpdatesBackupOfUserFromAdmin"
	DeleteBackupSharedByMultipleUsersFromAdmin                                         TestCaseName = "DeleteBackupSharedByMultipleUsersFromAdmin"
	NodeCountForLicensing                                                              TestCaseName = "NodeCountForLicensing"
	LicensingCountWithNodeLabelledBeforeClusterAddition                                TestCaseName = "LicensingCountWithNodeLabelledBeforeClusterAddition"
	LicensingCountBeforeAndAfterBackupPodRestart                                       TestCaseName = "LicensingCountBeforeAndAfterBackupPodRestart"
	BackupLocationWithEncryptionKey                                                    TestCaseName = "BackupLocationWithEncryptionKey"
	ReplicaChangeWhileRestore                                                          TestCaseName = "ReplicaChangeWhileRestore"
	ResizeOnRestoredVolume                                                             TestCaseName = "ResizeOnRestoredVolume"
	RestoreEncryptedAndNonEncryptedBackups                                             TestCaseName = "RestoreEncryptedAndNonEncryptedBackups"
	ResizeVolumeOnScheduleBackup                                                       TestCaseName = "ResizeVolumeOnScheduleBackup"
	BackupClusterVerification                                                          TestCaseName = "BackupClusterVerification"
	UserGroupManagement                                                                TestCaseName = "UserGroupManagement"
	BasicBackupCreation                                                                TestCaseName = "BasicBackupCreation"
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy                       TestCaseName = "CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy"
	BasicSelectiveRestore                                                              TestCaseName = "BasicSelectiveRestore"
	CustomResourceBackupAndRestore                                                     TestCaseName = "CustomResourceBackupAndRestore"
	DeleteAllBackupObjects                                                             TestCaseName = "DeleteAllBackupObjects"
	ScheduleBackupCreationAllNS                                                        TestCaseName = "ScheduleBackupCreationAllNS"
	CustomResourceRestore                                                              TestCaseName = "CustomResourceRestore"
	AllNSBackupWithIncludeNewNSOption                                                  TestCaseName = "AllNSBackupWithIncludeNewNSOption"
	BackupSyncBasicTest                                                                TestCaseName = "BackupSyncBasicTest"
	BackupMultipleNsWithSameLabel                                                      TestCaseName = "BackupMultipleNsWithSameLabel"
	MultipleCustomRestoreSameTimeDiffStorageClassMapping                               TestCaseName = "MultipleCustomRestoreSameTimeDiffStorageClassMapping"
	AddMultipleNamespaceLabels                                                         TestCaseName = "AddMultipleNamespaceLabels"
	MultipleInPlaceRestoreSameTime                                                     TestCaseName = "MultipleInPlaceRestoreSameTime"
	CloudSnapsSafeWhenBackupLocationDeleteTest                                         TestCaseName = "CloudSnapsSafeWhenBackupLocationDeleteTest"
	SetUnsetNSLabelDuringScheduleBackup                                                TestCaseName = "SetUnsetNSLabelDuringScheduleBackup"
	BackupRestoreOnDifferentK8sVersions                                                TestCaseName = "BackupRestoreOnDifferentK8sVersions"
	BackupCRsThenMultipleRestoresOnHigherK8sVersion                                    TestCaseName = "BackupCRsThenMultipleRestoresOnHigherK8sVersion"
	ScheduleBackupDeleteAndRecreateNS                                                  TestCaseName = "ScheduleBackupDeleteAndRecreateNS"
	DeleteNSDeleteClusterRestore                                                       TestCaseName = "DeleteNSDeleteClusterRestore"
	AlternateBackupBetweenNfsAndS3                                                     TestCaseName = "AlternateBackupBetweenNfsAndS3"
	BackupNamespaceInNfsRestoredFromS3                                                 TestCaseName = "BackupNamespaceInNfsRestoredFromS3"
	DeleteS3ScheduleAndCreateNfsSchedule                                               TestCaseName = "DeleteS3ScheduleAndCreateNfsSchedule"
	KubeAndPxNamespacesSkipOnAllNSBackup                                               TestCaseName = "KubeAndPxNamespacesSkipOnAllNSBackup"
	MultipleBackupLocationWithSameEndpoint                                             TestCaseName = "MultipleBackupLocationWithSameEndpoint"
	UpgradePxBackup                                                                    TestCaseName = "UpgradePxBackupWithHelm"
	StorkUpgradeWithBackup                                                             TestCaseName = "StorkUpgradeWithBackup"
	PXBackupEndToEndBackupAndRestoreWithUpgrade                                        TestCaseName = "PXBackupEndToEndBackupAndRestoreWithUpgrade"
	IssueDeleteOfIncrementalBackupsAndRestore                                          TestCaseName = "IssueDeleteOfIncrementalBackupsAndRestore"
	DeleteIncrementalBackupsAndRecreateNew                                             TestCaseName = "DeleteIncrementalBackupsAndRecreateNew"
	DeleteBucketVerifyCloudBackupMissing                                               TestCaseName = "DeleteBucketVerifyCloudBackupMissing"
	DeleteBackupAndCheckIfBucketIsEmpty                                                TestCaseName = "DeleteBackupAndCheckIfBucketIsEmpty"
	KubevirtVMBackupRestoreWithDifferentStates                                         TestCaseName = "KubevirtVMBackupRestoreWithDifferentStates"
	KubevirtUpgradeTest                                                                TestCaseName = "KubevirtUpgradeTest"
	KubevirtVMBackupOrDeletionInProgress                                               TestCaseName = "KubevirtVMBackupOrDeletionInProgress"
	KubevirtVMBackupRestoreWithNodeSelector                                            TestCaseName = "KubevirtVMBackupRestoreWithNodeSelector"
	KubevirtVMWithFreezeUnfreeze                                                       TestCaseName = "KubevirtVMWithFreezeUnfreeze"
	KubevirtInPlaceRestoreWithReplaceAndRetain                                         TestCaseName = "KubevirtInPlaceRestoreWithReplaceAndRetain"
	KubevirtVMRestoreWithAfterChangingVMConfig                                         TestCaseName = "KubevirtVMRestoreWithAfterChangingVMConfig"
	BackupAlternatingBetweenLockedAndUnlockedBuckets                                   TestCaseName = "BackupAlternatingBetweenLockedAndUnlockedBuckets"
	LockedBucketResizeOnRestoredVolume                                                 TestCaseName = "LockedBucketResizeOnRestoredVolume"
	LockedBucketResizeVolumeOnScheduleBackup                                           TestCaseName = "LockedBucketResizeVolumeOnScheduleBackup"
	DeleteLockedBucketUserObjectsFromAdmin                                             TestCaseName = "DeleteLockedBucketUserObjectsFromAdmin"
	VerifyRBACForInfraAdmin                                                            TestCaseName = "VerifyRBACForInfraAdmin"
	VerifyRBACForPxAdmin                                                               TestCaseName = "VerifyRBACForPxAdmin"
	VerifyRBACForAppAdmin                                                              TestCaseName = "VerifyRBACForAppAdmin"
	VerifyRBACForAppUser                                                               TestCaseName = "VerifyRBACForAppUser"
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNS                                   TestCaseName = "DefaultBackupRestoreWithKubevirtAndNonKubevirtNS"
	KubevirtScheduledVMDelete                                                          TestCaseName = "KubevirtScheduledVMDelete"
	CustomBackupRestoreWithKubevirtAndNonKubevirtNS                                    TestCaseName = "CustomBackupRestoreWithKubevirtAndNonKubevirtNS"
	BackupAndRestoreSyncDR                                                             TestCaseName = "BackupAndRestoreSyncDR"
	ExcludeInvalidDirectoryFileBackup                                                  TestCaseName = "ExcludeInvalidDirectoryFileBackup"
	ExcludeDirectoryFileBackup                                                         TestCaseName = "ExcludeDirectoryFileBackup"
	MultipleProvisionerCsiSnapshotDeleteBackupAndRestore                               TestCaseName = "MultipleProvisionerCsiSnapshotDeleteBackupAndRestore"
	MultipleMemberProjectBackupAndRestoreForSingleNamespace                            TestCaseName = "MultipleMemberProjectBackupAndRestoreForSingleNamespace"
	BackupNetworkErrorTest                                                             TestCaseName = "BackupNetworkErrorTest"
	IssueMultipleBackupsAndRestoreInterleavedCopies                                    TestCaseName = "IssueMultipleBackupsAndRestoreInterleavedCopies"
	ValidateFiftyVolumeBackups                                                         TestCaseName = "ValidateFiftyVolumeBackups"
	BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies TestCaseName = "BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies"
	PXBackupClusterUpgradeTest                                                         TestCaseName = "PXBackupClusterUpgradeTest"
	BackupToLockedBucketWithSharedObjects                                              TestCaseName = "BackupToLockedBucketWithSharedObjects"
	RemoveJSONFilesFromNFSBackupLocation                                               TestCaseName = "RemoveJSONFilesFromNFSBackupLocation"
	CloudSnapshotMissingValidationForNFSLocation                                       TestCaseName = "CloudSnapshotMissingValidationForNFSLocation"
	MultipleProvisionerCsiKdmpBackupAndRestore                                         TestCaseName = "MultipleProvisionerCsiKdmpBackupAndRestore"
	KubevirtVMMigrationTest                                                            TestCaseName = "KubevirtVMMigrationTest"
	EnableNsAndClusterLevelPSAWithBackupAndRestore                                     TestCaseName = "EnableNsAndClusterLevelPSAWithBackupAndRestore"
	DummyPSATestcase                                                                   TestCaseName = "DummyPSATestcase"
	BackupCSIVolumesWithPartialSuccess                                                 TestCaseName = "BackupCSIVolumesWithPartialSuccess"
	RestoreFromHigherPrivilegedNamespaceToLower                                        TestCaseName = "RestoreFromHigherPrivilegedNamespaceToLower"
	BackupStateTransitionForScheduledBackups                                           TestCaseName = "BackupStateTransitionForScheduledBackups"
)

// Test case labels
const (
	CreateMultipleUsersAndGroupsLabel                                                       TestCaseLabel = "CreateMultipleUsersAndGroups"
	DuplicateSharedBackupLabel                                                              TestCaseLabel = "DuplicateSharedBackup"
	DifferentAccessSameUserLabel                                                            TestCaseLabel = "DifferentAccessSameUser"
	ShareBackupWithUsersAndGroupsLabel                                                      TestCaseLabel = "ShareBackupWithUsersAndGroups"
	ShareLargeNumberOfBackupsWithLargeNumberOfUsersLabel                                    TestCaseLabel = "ShareLargeNumberOfBackupsWithLargeNumberOfUsers"
	CancelClusterBackupShareLabel                                                           TestCaseLabel = "CancelClusterBackupShare"
	ShareBackupAndEditLabel                                                                 TestCaseLabel = "ShareBackupAndEdit"
	SharedBackupDeleteLabel                                                                 TestCaseLabel = "SharedBackupDelete"
	ClusterBackupShareToggleLabel                                                           TestCaseLabel = "ClusterBackupShareToggle"
	ShareBackupsAndClusterWithUserLabel                                                     TestCaseLabel = "ShareBackupsAndClusterWithUser"
	ShareBackupWithDifferentRoleUsersLabel                                                  TestCaseLabel = "ShareBackupWithDifferentRoleUsers"
	DeleteSharedBackupLabel                                                                 TestCaseLabel = "DeleteSharedBackup"
	ShareAndRemoveBackupLocationLabel                                                       TestCaseLabel = "ShareAndRemoveBackupLocation"
	ViewOnlyFullBackupRestoreIncrementalBackupLabel                                         TestCaseLabel = "ViewOnlyFullBackupRestoreIncrementalBackup"
	IssueMultipleRestoresWithNamespaceAndStorageClassMappingLabel                           TestCaseLabel = "IssueMultipleRestoresWithNamespaceAndStorageClassMapping"
	DeleteUsersRoleLabel                                                                    TestCaseLabel = "DeleteUsersRole"
	IssueMultipleDeletesForSharedBackupLabel                                                TestCaseLabel = "IssueMultipleDeletesForSharedBackup"
	SwapShareBackupLabel                                                                    TestCaseLabel = "SwapShareBackup"
	NamespaceLabelledBackupSharedWithDifferentAccessModeLabel                               TestCaseLabel = "NamespaceLabelledBackupSharedWithDifferentAccessMode"
	BackupScheduleForOldAndNewNSLabel                                                       TestCaseLabel = "BackupScheduleForOldAndNewNS"
	ManualAndScheduledBackupUsingNamespaceAndResourceLabelLabel                             TestCaseLabel = "ManualAndScheduledBackupUsingNamespaceAndResourceLabel"
	ScheduleBackupWithAdditionAndRemovalOfNSLabel                                           TestCaseLabel = "ScheduleBackupWithAdditionAndRemovalOfNS"
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimitLabel                                TestCaseLabel = "ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit"
	NamespaceLabelledBackupOfEmptyNamespaceLabel                                            TestCaseLabel = "NamespaceLabelledBackupOfEmptyNamespace"
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgressLabel                                TestCaseLabel = "DeleteNfsExecutorPodWhileBackupAndRestoreInProgress"
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProjectLabel                   TestCaseLabel = "SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject"
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestoreLabel                           TestCaseLabel = "NamespaceMoveFromProjectToProjectToNoProjectWhileRestore"
	MultipleProjectsAndNamespacesBackupAndRestoreLabel                                      TestCaseLabel = "MultipleProjectsAndNamespacesBackupAndRestore"
	BackupRestartPXLabel                                                                    TestCaseLabel = "BackupRestartPX"
	KillStorkWithBackupsAndRestoresInProgressLabel                                          TestCaseLabel = "KillStorkWithBackupsAndRestoresInProgress"
	RestartBackupPodDuringBackupSharingLabel                                                TestCaseLabel = "RestartBackupPodDuringBackupSharing"
	CancelAllRunningBackupJobsLabel                                                         TestCaseLabel = "CancelAllRunningBackupJobs"
	ScaleMongoDBWhileBackupAndRestoreLabel                                                  TestCaseLabel = "ScaleMongoDBWhileBackupAndRestore"
	RebootNodesWhenBackupsAreInProgressLabel                                                TestCaseLabel = "RebootNodesWhenBackupsAreInProgress"
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgressLabel                              TestCaseLabel = "ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress"
	CancelAllRunningRestoreJobsLabel                                                        TestCaseLabel = "CancelAllRunningRestoreJobs"
	DeleteSameNameObjectsByMultipleUsersFromAdminLabel                                      TestCaseLabel = "DeleteSameNameObjectsByMultipleUsersFromAdmin"
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdminLabel                   TestCaseLabel = "DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin"
	DeleteObjectsByMultipleUsersFromNewAdminLabel                                           TestCaseLabel = "DeleteObjectsByMultipleUsersFromNewAdmin"
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdminLabel                              TestCaseLabel = "DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin"
	DeleteSharedBackupOfUserFromAdminLabel                                                  TestCaseLabel = "DeleteSharedBackupOfUserFromAdmin"
	DeleteBackupOfUserNonSharedRBACLabel                                                    TestCaseLabel = "DeleteBackupOfUserNonSharedRBAC"
	DeleteBackupOfUserSharedRBACLabel                                                       TestCaseLabel = "DeleteBackupOfUserSharedRBAC"
	UpdatesBackupOfUserFromAdminLabel                                                       TestCaseLabel = "UpdatesBackupOfUserFromAdmin"
	DeleteBackupSharedByMultipleUsersFromAdminLabel                                         TestCaseLabel = "DeleteBackupSharedByMultipleUsersFromAdmin"
	NodeCountForLicensingLabel                                                              TestCaseLabel = "NodeCountForLicensing"
	LicensingCountWithNodeLabelledBeforeClusterAdditionLabel                                TestCaseLabel = "LicensingCountWithNodeLabelledBeforeClusterAddition"
	LicensingCountBeforeAndAfterBackupPodRestartLabel                                       TestCaseLabel = "LicensingCountBeforeAndAfterBackupPodRestart"
	BackupLocationWithEncryptionKeyLabel                                                    TestCaseLabel = "BackupLocationWithEncryptionKey"
	ReplicaChangeWhileRestoreLabel                                                          TestCaseLabel = "ReplicaChangeWhileRestore"
	ResizeOnRestoredVolumeLabel                                                             TestCaseLabel = "ResizeOnRestoredVolume"
	RestoreEncryptedAndNonEncryptedBackupsLabel                                             TestCaseLabel = "RestoreEncryptedAndNonEncryptedBackups"
	ResizeVolumeOnScheduleBackupLabel                                                       TestCaseLabel = "ResizeVolumeOnScheduleBackup"
	BackupClusterVerificationLabel                                                          TestCaseLabel = "BackupClusterVerification"
	UserGroupManagementLabel                                                                TestCaseLabel = "UserGroupManagement"
	BasicBackupCreationLabel                                                                TestCaseLabel = "BasicBackupCreation"
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicyLabel                       TestCaseLabel = "CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy"
	BasicSelectiveRestoreLabel                                                              TestCaseLabel = "BasicSelectiveRestore"
	CustomResourceBackupAndRestoreLabel                                                     TestCaseLabel = "CustomResourceBackupAndRestore"
	DeleteAllBackupObjectsLabel                                                             TestCaseLabel = "DeleteAllBackupObjects"
	ScheduleBackupCreationAllNSLabel                                                        TestCaseLabel = "ScheduleBackupCreationAllNS"
	CustomResourceRestoreLabel                                                              TestCaseLabel = "CustomResourceRestore"
	AllNSBackupWithIncludeNewNSOptionLabel                                                  TestCaseLabel = "AllNSBackupWithIncludeNewNSOption"
	BackupSyncBasicTestLabel                                                                TestCaseLabel = "BackupSyncBasicTest"
	BackupMultipleNsWithSameLabelLabel                                                      TestCaseLabel = "BackupMultipleNsWithSameLabel"
	MultipleCustomRestoreSameTimeDiffStorageClassMappingLabel                               TestCaseLabel = "MultipleCustomRestoreSameTimeDiffStorageClassMapping"
	AddMultipleNamespaceLabelsLabel                                                         TestCaseLabel = "AddMultipleNamespaceLabels"
	MultipleInPlaceRestoreSameTimeLabel                                                     TestCaseLabel = "MultipleInPlaceRestoreSameTime"
	CloudSnapsSafeWhenBackupLocationDeleteTestLabel                                         TestCaseLabel = "CloudSnapsSafeWhenBackupLocationDeleteTest"
	SetUnsetNSLabelDuringScheduleBackupLabel                                                TestCaseLabel = "SetUnsetNSLabelDuringScheduleBackup"
	BackupRestoreOnDifferentK8sVersionsLabel                                                TestCaseLabel = "BackupRestoreOnDifferentK8sVersions"
	BackupCRsThenMultipleRestoresOnHigherK8sVersionLabel                                    TestCaseLabel = "BackupCRsThenMultipleRestoresOnHigherK8sVersion"
	ScheduleBackupDeleteAndRecreateNSLabel                                                  TestCaseLabel = "ScheduleBackupDeleteAndRecreateNS"
	DeleteNSDeleteClusterRestoreLabel                                                       TestCaseLabel = "DeleteNSDeleteClusterRestore"
	AlternateBackupBetweenNfsAndS3Label                                                     TestCaseLabel = "AlternateBackupBetweenNfsAndS3"
	BackupNamespaceInNfsRestoredFromS3Label                                                 TestCaseLabel = "BackupNamespaceInNfsRestoredFromS3"
	DeleteS3ScheduleAndCreateNfsScheduleLabel                                               TestCaseLabel = "DeleteS3ScheduleAndCreateNfsSchedule"
	KubeAndPxNamespacesSkipOnAllNSBackupLabel                                               TestCaseLabel = "KubeAndPxNamespacesSkipOnAllNSBackup"
	MultipleBackupLocationWithSameEndpointLabel                                             TestCaseLabel = "MultipleBackupLocationWithSameEndpoint"
	UpgradePxBackupLabel                                                                    TestCaseLabel = "UpgradePxBackupWithHelm"
	StorkUpgradeWithBackupLabel                                                             TestCaseLabel = "StorkUpgradeWithBackup"
	PXBackupEndToEndBackupAndRestoreWithUpgradeLabel                                        TestCaseLabel = "PXBackupEndToEndBackupAndRestoreWithUpgrade"
	IssueDeleteOfIncrementalBackupsAndRestoreLabel                                          TestCaseLabel = "IssueDeleteOfIncrementalBackupsAndRestore"
	DeleteIncrementalBackupsAndRecreateNewLabel                                             TestCaseLabel = "DeleteIncrementalBackupsAndRecreateNew"
	DeleteBucketVerifyCloudBackupMissingLabel                                               TestCaseLabel = "DeleteBucketVerifyCloudBackupMissing"
	DeleteBackupAndCheckIfBucketIsEmptyLabel                                                TestCaseLabel = "DeleteBackupAndCheckIfBucketIsEmpty"
	KubevirtVMBackupRestoreWithDifferentStatesLabel                                         TestCaseLabel = "KubevirtVMBackupRestoreWithDifferentStates"
	BackupAlternatingBetweenLockedAndUnlockedBucketsLabel                                   TestCaseLabel = "BackupAlternatingBetweenLockedAndUnlockedBuckets"
	LockedBucketResizeOnRestoredVolumeLabel                                                 TestCaseLabel = "LockedBucketResizeOnRestoredVolume"
	LockedBucketResizeVolumeOnScheduleBackupLabel                                           TestCaseLabel = "LockedBucketResizeVolumeOnScheduleBackup"
	DeleteLockedBucketUserObjectsFromAdminLabel                                             TestCaseLabel = "DeleteLockedBucketUserObjectsFromAdmin"
	VerifyRBACForInfraAdminLabel                                                            TestCaseLabel = "VerifyRBACForInfraAdmin"
	VerifyRBACForPxAdminLabel                                                               TestCaseLabel = "VerifyRBACForPxAdmin"
	VerifyRBACForAppAdminLabel                                                              TestCaseLabel = "VerifyRBACForAppAdmin"
	VerifyRBACForAppUserLabel                                                               TestCaseLabel = "VerifyRBACForAppUser"
	KubevirtUpgradeTestLabel                                                                TestCaseLabel = "KubevirtUpgradeTest"
	KubevirtVMBackupOrDeletionInProgressLabel                                               TestCaseLabel = "KubevirtVMBackupOrDeletionInProgress"
	KubevirtVMBackupRestoreWithNodeSelectorLabel                                            TestCaseLabel = "KubevirtVMBackupRestoreWithNodeSelector"
	KubevirtVMWithFreezeUnfreezeLabel                                                       TestCaseLabel = "KubevirtVMWithFreezeUnfreeze"
	KubevirtInPlaceRestoreWithReplaceAndRetainLabel                                         TestCaseLabel = "KubevirtInPlaceRestoreWithReplaceAndRetain"
	KubevirtVMRestoreWithAfterChangingVMConfigLabel                                         TestCaseLabel = "KubevirtVMRestoreWithAfterChangingVMConfig"
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNSLabel                                   TestCaseLabel = "DefaultBackupRestoreWithKubevirtAndNonKubevirtNS"
	KubevirtScheduledVMDeleteLabel                                                          TestCaseLabel = "KubevirtScheduledVMDelete"
	CustomBackupRestoreWithKubevirtAndNonKubevirtNSLabel                                    TestCaseLabel = "CustomBackupRestoreWithKubevirtAndNonKubevirtNS"
	BackupAndRestoreSyncDRLabel                                                             TestCaseLabel = "BackupAndRestoreSyncDR"
	ExcludeInvalidDirectoryFileBackupLabel                                                  TestCaseLabel = "ExcludeInvalidDirectoryFileBackup"
	ExcludeDirectoryFileBackupLabel                                                         TestCaseLabel = "ExcludeDirectoryFileBackup"
	MultipleProvisionerCsiSnapshotDeleteBackupAndRestoreLabel                               TestCaseLabel = "MultipleProvisionerCsiSnapshotDeleteBackupAndRestore"
	MultipleMemberProjectBackupAndRestoreForSingleNamespaceLabel                            TestCaseLabel = "MultipleMemberProjectBackupAndRestoreForSingleNamespace"
	BackupNetworkErrorTestLabel                                                             TestCaseLabel = "BackupNetworkErrorTest"
	IssueMultipleBackupsAndRestoreInterleavedCopiesLabel                                    TestCaseLabel = "IssueMultipleBackupsAndRestoreInterleavedCopies"
	ValidateFiftyVolumeBackupsLabel                                                         TestCaseLabel = "ValidateFiftyVolumeBackups"
	BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPoliciesLabel TestCaseLabel = "BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies"
	PXBackupClusterUpgradeTestLabel                                                         TestCaseLabel = "PXBackupClusterUpgradeTest"
	BackupToLockedBucketWithSharedObjectsLabel                                              TestCaseLabel = "BackupToLockedBucketWithSharedObjects"
	RemoveJSONFilesFromNFSBackupLocationLabel                                               TestCaseLabel = "RemoveJSONFilesFromNFSBackupLocation"
	CloudSnapshotMissingValidationForNFSLocationLabel                                       TestCaseLabel = "CloudSnapshotMissingValidationForNFSLocation"
	MultipleProvisionerCsiKdmpBackupAndRestoreLabel                                         TestCaseLabel = "MultipleProvisionerCsiKdmpBackupAndRestore"
	KubevirtVMMigrationTestLabel                                                            TestCaseLabel = "KubevirtVMMigrationTest"
	BackupCSIVolumesWithPartialSuccessLabel                                                 TestCaseLabel = "BackupCSIVolumesWithPartialSuccess"
	BackupStateTransitionForScheduledBackupsLabel                                           TestCaseLabel = "BackupStateTransitionForScheduledBackups"
	EnableNsAndClusterLevelPSAWithBackupAndRestoreLabel                                     TestCaseLabel = "EnableNsAndClusterLevelPSAWithBackupAndRestore"
	RestoreFromHigherPrivilegedNamespaceToLowerLabel                                        TestCaseLabel = "RestoreFromHigherPrivilegedNamespaceToLower"
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
	ROKS        = "roks"
	AnyPlatform = "any-platform"
)

// PipelineLabels
const (
	vanillaPipelineS3Nightly                   = "vanilla-pipeline-s3"
	vanillaPipelineNfsNightly                  = "vanilla-pipeline-nfs"
	vanillaPipelineWithDifferentK8sVersionsS3  = "vanilla-pipeline-with-different-k8s-versions-s3"
	vanillaPipelineWithDifferentK8sVersionsNfs = "vanilla-pipeline-with-different-k8s-versions-nfs"
	vanillaPipelineWithS3LockedBucket          = "vanilla-pipeline-with-s3-locked-bucket"
	vanillaPipelineS3Upgrade                   = "vanilla-pipeline-s3-upgrade"
	vanillaFacdPipelineS3                      = "vanilla-facd-pipeline-s3"
	vanillaFacdPipelineNfs                     = "vanilla-facd-pipeline-nfs"
	vanillaFadaPipelineS3                      = "vanilla-fada-pipeline-s3"
	vanillaFadaPipelineNfs                     = "vanilla-fada-pipeline-nfs"
	vanillaFBDAPipelineS3                      = "vanilla-fbda-pipeline-s3"
	vanillaFBDAPipelineNfs                     = "vanilla-fbda-pipeline-nfs"
	ibmNonPXIKSPipelineS3                      = "ibm-nonpx-iks-pipeline-s3"
	ibmPxIKSPipelineS3                         = "ibm-px-iks-pipeline-s3"
	ibmNonPxRoksPipelineS3                     = "ibm-nonpx-roks-pipeline-s3"
	ibmPxRoksPipelineS3                        = "ibm-px-roks-pipeline-s3"
	ibmNonPxIksPipelineS3Upgrade               = "ibm-nonpx-iks-pipeline-s3-upgrade"
	ibmPxIksPipelineS3Upgrade                  = "ibm-px-iks-pipeline-s3-upgrade"
	ocpPxPipelineS3Upgrade                     = "ocp-px-pipeline-s3-upgrade"
	ibmNonPxRoksPipelineS3Upgrade              = "ibm-nonpx-roks-pipeline-s3-upgrade"
	VanillaPipelineS3StorkUpgrade              = "vanilla-pipeline-s3-stork-upgrade"
	rkePipelineNightly                         = "rke-pipeline-nightly"
)

// Aetos lN labels
const (
	aetosl1aws                     = "aetos-l1-aws"
	aetosl1nfs                     = "aetos-l1-nfs"
	aetosl3awsVanilla              = "aetos-l3-aws-vanilla"
	aetosl3nfsVanilla              = "aetos-l3-nfs-vanilla"
	aetosl3awsrke                  = "aetos-l3-aws-rke"
	aetosl3nfsrke                  = "aetos-l3-nfs-rke"
	aetosl3awsstorkupgrade         = "aetos-l3-aws-stork-upgrade"
	aetosl3awsPXBackupupgrade      = "aetos-l3-aws-px-backup-upgrade"
	aetosl3awsPXBackupStorkupgrade = "aetos-l3-aws-px-backup-stork-upgrade"
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
	CreateMultipleUsersAndGroups:                                     {CreateMultipleUsersAndGroupsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DuplicateSharedBackup:                                            {DuplicateSharedBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DifferentAccessSameUser:                                          {DifferentAccessSameUserLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ShareBackupWithUsersAndGroups:                                    {ShareBackupWithUsersAndGroupsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly},
	ShareLargeNumberOfBackupsWithLargeNumberOfUsers:                  {ShareLargeNumberOfBackupsWithLargeNumberOfUsersLabel},
	CancelClusterBackupShare:                                         {CancelClusterBackupShareLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ShareBackupAndEdit:                                               {ShareBackupAndEditLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly},
	SharedBackupDelete:                                               {SharedBackupDeleteLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ClusterBackupShareToggle:                                         {ClusterBackupShareToggleLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ShareBackupsAndClusterWithUser:                                   {ShareBackupsAndClusterWithUserLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ShareBackupWithDifferentRoleUsers:                                {ShareBackupWithDifferentRoleUsersLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteSharedBackup:                                               {DeleteSharedBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ShareAndRemoveBackupLocation:                                     {ShareAndRemoveBackupLocationLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ViewOnlyFullBackupRestoreIncrementalBackup:                       {ViewOnlyFullBackupRestoreIncrementalBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	IssueMultipleRestoresWithNamespaceAndStorageClassMapping:         {IssueMultipleRestoresWithNamespaceAndStorageClassMappingLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteUsersRole:                                                  {DeleteUsersRoleLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	IssueMultipleDeletesForSharedBackup:                              {IssueMultipleDeletesForSharedBackupLabel, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	SwapShareBackup:                                                  {SwapShareBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	NamespaceLabelledBackupSharedWithDifferentAccessMode:             {NamespaceLabelledBackupSharedWithDifferentAccessModeLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	BackupScheduleForOldAndNewNS:                                     {BackupScheduleForOldAndNewNSLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ManualAndScheduledBackupUsingNamespaceAndResourceLabel:           {ManualAndScheduledBackupUsingNamespaceAndResourceLabelLabel, aetosl1aws, aetosl1nfs, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ScheduleBackupWithAdditionAndRemovalOfNS:                         {ScheduleBackupWithAdditionAndRemovalOfNSLabel, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ManualAndScheduleBackupUsingNSLabelWithMaxCharLimit:              {ManualAndScheduleBackupUsingNSLabelWithMaxCharLimitLabel, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	NamespaceLabelledBackupOfEmptyNamespace:                          {NamespaceLabelledBackupOfEmptyNamespaceLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmNonPxRoksPipelineS3, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteNfsExecutorPodWhileBackupAndRestoreInProgress:              {DeleteNfsExecutorPodWhileBackupAndRestoreInProgressLabel},
	SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProject: {SingleNamespaceBackupRestoreToNamespaceInSameAndDifferentProjectLabel, aetosl3awsrke, aetosl3nfsrke},
	NamespaceMoveFromProjectToProjectToNoProjectWhileRestore:         {NamespaceMoveFromProjectToProjectToNoProjectWhileRestoreLabel, aetosl3awsrke, aetosl3nfsrke},
	MultipleProjectsAndNamespacesBackupAndRestore:                    {MultipleProjectsAndNamespacesBackupAndRestoreLabel, aetosl3awsrke, aetosl3nfsrke},
	BackupRestartPX:                                                  {BackupRestartPXLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	KillStorkWithBackupsAndRestoresInProgress:                        {KillStorkWithBackupsAndRestoresInProgressLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	RestartBackupPodDuringBackupSharing:                              {RestartBackupPodDuringBackupSharingLabel, aetosl3awsVanilla, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CancelAllRunningBackupJobs:                                       {CancelAllRunningBackupJobsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ScaleMongoDBWhileBackupAndRestore:                                {ScaleMongoDBWhileBackupAndRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	RebootNodesWhenBackupsAreInProgress:                              {RebootNodesWhenBackupsAreInProgressLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress:            {ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgressLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CancelAllRunningRestoreJobs:                                      {CancelAllRunningRestoreJobsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteSameNameObjectsByMultipleUsersFromAdmin:                    {DeleteSameNameObjectsByMultipleUsersFromAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdmin: {DeleteUserBackupsAndRestoresOfDeletedAndInActiveClusterFromAdminLabel, aetosl3awsVanilla, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsVanilla, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteObjectsByMultipleUsersFromNewAdmin:                         {DeleteObjectsByMultipleUsersFromNewAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteFailedInProgressBackupAndRestoreOfUserFromAdmin:            {DeleteFailedInProgressBackupAndRestoreOfUserFromAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteSharedBackupOfUserFromAdmin:                                {DeleteSharedBackupOfUserFromAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteBackupOfUserNonSharedRBAC:                                  {DeleteBackupOfUserNonSharedRBACLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteBackupOfUserSharedRBAC:                                     {DeleteBackupOfUserSharedRBACLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	UpdatesBackupOfUserFromAdmin:                                     {UpdatesBackupOfUserFromAdminLabel, aetosl3awsVanilla, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteBackupSharedByMultipleUsersFromAdmin:                       {DeleteBackupSharedByMultipleUsersFromAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	NodeCountForLicensing:                                            {NodeCountForLicensingLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	LicensingCountWithNodeLabelledBeforeClusterAddition:              {LicensingCountWithNodeLabelledBeforeClusterAdditionLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	LicensingCountBeforeAndAfterBackupPodRestart:                     {LicensingCountBeforeAndAfterBackupPodRestartLabel, aetosl3awsVanilla, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly},
	BackupLocationWithEncryptionKey:                                  {BackupLocationWithEncryptionKeyLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ReplicaChangeWhileRestore:                                        {ReplicaChangeWhileRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ResizeOnRestoredVolume:                                           {ResizeOnRestoredVolumeLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	RestoreEncryptedAndNonEncryptedBackups:                           {RestoreEncryptedAndNonEncryptedBackupsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ResizeVolumeOnScheduleBackup:                                     {ResizeVolumeOnScheduleBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly},
	BackupClusterVerification:                                        {BackupClusterVerificationLabel},
	UserGroupManagement:                                              {UserGroupManagementLabel, aetosl1aws, aetosl1nfs},
	BasicBackupCreation:                                              {BasicBackupCreationLabel, aetosl1aws, aetosl1nfs, aetosl3awsrke, aetosl3nfsrke, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy:     {CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicyLabel},
	BasicSelectiveRestore:                                            {BasicSelectiveRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CustomResourceBackupAndRestore:                                   {CustomResourceBackupAndRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly},
	DeleteAllBackupObjects:                                           {DeleteAllBackupObjectsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	ScheduleBackupCreationAllNS:                                      {ScheduleBackupCreationAllNSLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CustomResourceRestore:                                            {CustomResourceRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	AllNSBackupWithIncludeNewNSOption:                                {AllNSBackupWithIncludeNewNSOptionLabel, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	BackupSyncBasicTest:                                              {BackupSyncBasicTestLabel, aetosl3awsVanilla, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, vanillaPipelineS3Nightly},
	BackupMultipleNsWithSameLabel:                                    {BackupMultipleNsWithSameLabelLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	MultipleCustomRestoreSameTimeDiffStorageClassMapping:             {MultipleCustomRestoreSameTimeDiffStorageClassMappingLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	AddMultipleNamespaceLabels:                                       {AddMultipleNamespaceLabelsLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	MultipleInPlaceRestoreSameTime:                                   {MultipleInPlaceRestoreSameTimeLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	CloudSnapsSafeWhenBackupLocationDeleteTest:                       {CloudSnapsSafeWhenBackupLocationDeleteTestLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	SetUnsetNSLabelDuringScheduleBackup:                              {SetUnsetNSLabelDuringScheduleBackupLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	BackupRestoreOnDifferentK8sVersions:                              {BackupRestoreOnDifferentK8sVersionsLabel, vanillaPipelineWithDifferentK8sVersionsS3, vanillaPipelineWithDifferentK8sVersionsNfs},
	BackupCRsThenMultipleRestoresOnHigherK8sVersion:                  {BackupCRsThenMultipleRestoresOnHigherK8sVersionLabel, vanillaPipelineWithDifferentK8sVersionsS3, vanillaPipelineWithDifferentK8sVersionsNfs},
	ScheduleBackupDeleteAndRecreateNS:                                {ScheduleBackupDeleteAndRecreateNSLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteNSDeleteClusterRestore:                                     {DeleteNSDeleteClusterRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, ibmNonPXIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmNonPxIksPipelineS3Upgrade, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, ibmNonPxRoksPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	AlternateBackupBetweenNfsAndS3:                                   {AlternateBackupBetweenNfsAndS3Label, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	BackupNamespaceInNfsRestoredFromS3:                               {BackupNamespaceInNfsRestoredFromS3Label, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteS3ScheduleAndCreateNfsSchedule:                             {DeleteS3ScheduleAndCreateNfsScheduleLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	KubeAndPxNamespacesSkipOnAllNSBackup:                             {KubeAndPxNamespacesSkipOnAllNSBackupLabel, aetosl3awsVanilla, aetosl3nfsVanilla, vanillaFacdPipelineS3, vanillaFacdPipelineNfs, vanillaFadaPipelineS3, vanillaFadaPipelineNfs, vanillaFBDAPipelineS3, vanillaFBDAPipelineNfs, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	MultipleBackupLocationWithSameEndpoint:                           {MultipleBackupLocationWithSameEndpointLabel},
	UpgradePxBackup:                                                  {UpgradePxBackupLabel},
	StorkUpgradeWithBackup:                                           {StorkUpgradeWithBackupLabel},
	PXBackupEndToEndBackupAndRestoreWithUpgrade:                      {PXBackupEndToEndBackupAndRestoreWithUpgradeLabel},
	IssueDeleteOfIncrementalBackupsAndRestore:                        {IssueDeleteOfIncrementalBackupsAndRestoreLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteIncrementalBackupsAndRecreateNew:                           {DeleteIncrementalBackupsAndRecreateNewLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteBucketVerifyCloudBackupMissing:                             {DeleteBucketVerifyCloudBackupMissingLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmNonPXIKSPipelineS3, ibmPxIKSPipelineS3, ibmNonPxRoksPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	DeleteBackupAndCheckIfBucketIsEmpty:                              {DeleteBackupAndCheckIfBucketIsEmptyLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3awsPXBackupupgrade, aetosl3awsstorkupgrade, aetosl3awsPXBackupStorkupgrade, aetosl3nfsrke, aetosl3nfsVanilla, ibmPxIKSPipelineS3, ibmPxRoksPipelineS3, ibmPxIksPipelineS3Upgrade, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	KubevirtVMBackupRestoreWithDifferentStates:                       {KubevirtVMBackupRestoreWithDifferentStatesLabel, KubevirtAppLabel},
	BackupAlternatingBetweenLockedAndUnlockedBuckets:                 {BackupAlternatingBetweenLockedAndUnlockedBucketsLabel, vanillaPipelineWithS3LockedBucket},
	LockedBucketResizeOnRestoredVolume:                               {LockedBucketResizeOnRestoredVolumeLabel, vanillaPipelineWithS3LockedBucket},
	LockedBucketResizeVolumeOnScheduleBackup:                         {LockedBucketResizeVolumeOnScheduleBackupLabel, vanillaPipelineWithS3LockedBucket},
	DeleteLockedBucketUserObjectsFromAdmin:                           {DeleteLockedBucketUserObjectsFromAdminLabel},
	VerifyRBACForInfraAdmin:                                          {VerifyRBACForInfraAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	VerifyRBACForPxAdmin:                                             {VerifyRBACForPxAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	VerifyRBACForAppAdmin:                                            {VerifyRBACForAppAdminLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	VerifyRBACForAppUser:                                             {VerifyRBACForAppUserLabel, aetosl3awsVanilla, aetosl3awsrke, aetosl3nfsrke, aetosl3nfsVanilla, ocpPxPipelineS3Upgrade, vanillaPipelineS3Upgrade, VanillaPipelineS3StorkUpgrade, vanillaPipelineS3Nightly, vanillaPipelineNfsNightly},
	KubevirtUpgradeTest:                                              {KubevirtUpgradeTestLabel, KubevirtAppLabel},
	KubevirtVMBackupOrDeletionInProgress:                             {KubevirtVMBackupOrDeletionInProgressLabel, KubevirtAppLabel},
	KubevirtVMBackupRestoreWithNodeSelector:                          {KubevirtVMBackupRestoreWithNodeSelectorLabel, KubevirtAppLabel},
	KubevirtVMWithFreezeUnfreeze:                                     {KubevirtVMWithFreezeUnfreezeLabel, KubevirtAppLabel},
	KubevirtInPlaceRestoreWithReplaceAndRetain:                       {KubevirtInPlaceRestoreWithReplaceAndRetainLabel, KubevirtAppLabel},
	KubevirtVMRestoreWithAfterChangingVMConfig:                       {KubevirtVMRestoreWithAfterChangingVMConfigLabel, KubevirtAppLabel},
	DefaultBackupRestoreWithKubevirtAndNonKubevirtNS:                 {DefaultBackupRestoreWithKubevirtAndNonKubevirtNSLabel, KubevirtAppLabel},
	KubevirtScheduledVMDelete:                                        {KubevirtScheduledVMDeleteLabel, KubevirtAppLabel},
	CustomBackupRestoreWithKubevirtAndNonKubevirtNS:                  {CustomBackupRestoreWithKubevirtAndNonKubevirtNSLabel, KubevirtAppLabel},
	ExcludeInvalidDirectoryFileBackup:                                {ExcludeInvalidDirectoryFileBackupLabel},
	ExcludeDirectoryFileBackup:                                       {ExcludeDirectoryFileBackupLabel},
	MultipleMemberProjectBackupAndRestoreForSingleNamespace:          {MultipleMemberProjectBackupAndRestoreForSingleNamespaceLabel},
	MultipleProvisionerCsiSnapshotDeleteBackupAndRestore:             {MultipleProvisionerCsiSnapshotDeleteBackupAndRestoreLabel},
	BackupNetworkErrorTest:                                           {BackupNetworkErrorTestLabel},
	IssueMultipleBackupsAndRestoreInterleavedCopies:                  {IssueMultipleBackupsAndRestoreInterleavedCopiesLabel},
	ValidateFiftyVolumeBackups:                                       {ValidateFiftyVolumeBackupsLabel},
	PXBackupClusterUpgradeTest:                                       {PXBackupClusterUpgradeTestLabel},
	BackupToLockedBucketWithSharedObjects:                            {BackupToLockedBucketWithSharedObjectsLabel, vanillaPipelineWithS3LockedBucket},
	RemoveJSONFilesFromNFSBackupLocation:                             {RemoveJSONFilesFromNFSBackupLocationLabel},
	CloudSnapshotMissingValidationForNFSLocation:                     {CloudSnapshotMissingValidationForNFSLocationLabel},
	MultipleProvisionerCsiKdmpBackupAndRestore:                       {MultipleProvisionerCsiKdmpBackupAndRestoreLabel},
	KubevirtVMMigrationTest:                                          {KubevirtVMMigrationTestLabel, KubevirtAppLabel},
	BackupCSIVolumesWithPartialSuccess:                               {BackupCSIVolumesWithPartialSuccessLabel},
	BackupStateTransitionForScheduledBackups:                         {BackupStateTransitionForScheduledBackupsLabel},
	EnableNsAndClusterLevelPSAWithBackupAndRestore:                   {EnableNsAndClusterLevelPSAWithBackupAndRestoreLabel},
	RestoreFromHigherPrivilegedNamespaceToLower:                      {RestoreFromHigherPrivilegedNamespaceToLowerLabel, rkePipelineNightly},
}
