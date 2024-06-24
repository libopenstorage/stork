package tests

import (
	"fmt"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// EnableNsAndClusterLevelPSAWithBackupAndRestore verifies backup and restore of applications with namespace and cluster level PSA enabled on Vanilla Cluster
var _ = Describe("{EnableNsAndClusterLevelPSAWithBackupAndRestore}", Label(TestCaseLabelsMap[EnableNsAndClusterLevelPSAWithBackupAndRestore]...), func() {
	var (
		backupNames          []string
		backupNames2         []string
		restoreNames         []string
		appList              = Inst().AppList
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		label                map[string]string
		preRuleNameList      []string
		postRuleNameList     []string
		providers            []string
		sourceScNameList     []string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationMap    map[string]string
		sourceClusterUid     string
		scName               string
		params               map[string]string
		backupNSMap          map[string]string
		controlChannel       chan string
		errorGroup           *errgroup.Group
		backupNamesAllNs     []string
		restoredNamespaces   []string
	)
	storageClassMapping := make(map[string]string)
	AppContextsMapping := make(map[string]*scheduler.Context)
	providers = GetBackupProviders()
	scheduledAppContexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	preRuleNameList = make([]string, 0)
	postRuleNameList = make([]string, 0)
	originalList := Inst().AppList
	label = make(map[string]string)
	backupLocationMap = make(map[string]string)
	psaFlag := false
	backupNSMap = make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("EnableNsAndClusterLevelPSAWithBackupAndRestore", "Enable Namespace and cluster level PSA with Backup and Restore", nil, 299243, Kshithijiyer, Q2FY25)

		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		psaApp := make([]string, 0)
		for _, psalevel := range []string{"restricted", "baseline", "privileged"} {
			if psalevel == "restricted" {
				appList := Inst().AppList
				log.InfoD("The app list at the start of the testcase is %v", Inst().AppList)
				for _, app := range appList {
					psaApp = append(psaApp, PSAAppMap[app])
				}
				log.Infof("The PSA app list is %v", psaApp)
				Inst().AppList = psaApp
			}
			label["pod-security.kubernetes.io/enforce"] = psalevel

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d-%s-%d", TaskNamePrefix, 0, psalevel, i)
				namespace := fmt.Sprintf("%s-%d", psalevel, i)
				err := CreateNamespaceAndAssignLabels(namespace, label)
				dash.VerifyFatal(err, nil, "Creating namespace and assigning labels")
				appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
				for _, ctx := range appContexts {
					ctx.ReadinessTimeout = AppReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
					scheduledAppContexts = append(scheduledAppContexts, ctx)
					AppContextsMapping[namespace] = ctx
				}
			}
			if psalevel == "restricted" {
				Inst().AppList = originalList
			}
		}
	})

	It("Enable Namespace and cluster level PSA with Backup and Restore", func() {

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, fmt.Sprintf("Verifying pre rule %s for backup", ruleName))
				if ruleName != "" {
					preRuleNameList = append(preRuleNameList, ruleName)
				}
			}

			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, fmt.Sprintf("Verifying post rule %s for backup", ruleName))
				if ruleName != "" {
					postRuleNameList = append(postRuleNameList, ruleName)
				}
			}
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, RandomString(10))
				backupLocationName := fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creating backup location")
			}
		})

		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Taking backup of all the namespaces created with namespace level PSA", func() {
			log.InfoD("Taking backup of all the namespaces created with namespace level PSA")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			labelSelectors := make(map[string]string)
			for backupLocationUID, backupLocationName := range backupLocationMap {
				for _, namespace := range bkpNamespaces {
					wg.Add(1)
					go func(namespace string) {
						defer wg.Done()
						defer GinkgoRecover()
						backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, RandomString(10))
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						preRuleUid, preRule := "", ""
						if len(preRuleNameList) > 0 {
							preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
							log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameList[0])
							preRule = preRuleNameList[0]
						}
						postRuleUid, postRule := "", ""
						if len(postRuleNameList) > 0 {
							postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
							log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleNameList[0])
							postRule = postRuleNameList[0]
						}
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, sourceClusterUid, preRule, preRuleUid, postRule, postRuleUid)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] of namespace [%s]", backupName, namespace))
						mutex.Lock()
						backupNames = append(backupNames, backupName)
						backupNSMap[backupName] = namespace
						mutex.Unlock()
					}(namespace)
				}
			}
			wg.Wait()
		})

		Step("Create new storage class for restore", func() {
			log.InfoD("Getting storage class of the source cluster")
			for _, appNamespaces := range bkpNamespaces {
				pvcs, err := core.Instance().GetPersistentVolumeClaims(appNamespaces, make(map[string]string))
				log.FailOnError(err, "Getting PVC on source cluster")
				singlePvc := pvcs.Items[0]
				storageClass, err := core.Instance().GetStorageClassForPVC(&singlePvc)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting SC %v from PVC in source cluster",
					storageClass.Name))
				sourceScNameList = append(sourceScNameList, storageClass.Name)
			}

			err := SetDestinationKubeConfig()
			dash.VerifyFatal(err, nil, "Setting destination kubeconfig")

			for _, sc := range sourceScNameList {
				scName = fmt.Sprintf("replica-sc-%v", RandomString(3))
				v1obj := metaV1.ObjectMeta{
					Name: scName,
				}
				reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
				bindMode := storageApi.VolumeBindingImmediate
				scObj := storageApi.StorageClass{
					ObjectMeta:        v1obj,
					Provisioner:       k8s.CsiProvisioner,
					Parameters:        params,
					ReclaimPolicy:     &reclaimPolicyDelete,
					VolumeBindingMode: &bindMode,
				}
				_, err = storage.Instance().CreateStorageClass(&scObj)
				log.FailOnError(err, "Creating sc on dest cluster")
				storageClassMapping[sc] = scName
			}

			err = SetSourceKubeConfig()
			dash.VerifyFatal(err, nil, "Setting source kubeconfig")
		})

		Step("Default restore of applications by replacing the existing resources with NS level PSA", func() {
			log.InfoD("Default restore of applications by replacing the existing resources with NS level PSA")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer wg.Done()
					defer GinkgoRecover()
					defaultRestoreName := fmt.Sprintf("%s-%s-default", RestoreNamePrefix, backupName)
					err = CreateRestoreWithReplacePolicyWithValidation(defaultRestoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete, scheduledAppContexts)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating default restore for backup with replace policy [%s]", defaultRestoreName))
					mutex.Lock()
					restoreNames = append(restoreNames, defaultRestoreName)
					mutex.Unlock()
				}(backupName)
			}
			wg.Wait()
		})

		Step("Restore of applications with NS and StorageClass mapping with NS level PSA", func() {
			log.InfoD("Restore of applications with NS and StorageClass mapping with NS level PSA")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			for backupName, backupNamespace := range backupNSMap {
				appContextsToRestore := make([]*scheduler.Context, 0)
				namespaceMapping := make(map[string]string)
				customRestoreName := fmt.Sprintf("%s-%s-custom-ns-sc", RestoreNamePrefix, backupName)
				namespaceMapping[backupNamespace] = backupNamespace + "-restored-1"
				restoredNamespaces = append(restoredNamespaces, backupNamespace+"-restored-1")
				appContextsToRestore = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNamespace})
				err = CreateRestoreWithReplacePolicyWithValidation(customRestoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, storageClassMapping, ReplacePolicyDelete, appContextsToRestore)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Create restore with NS and StorageClass mapping with NS level PSA"))
				restoreNames = append(restoreNames, customRestoreName)
			}
		})

		for _, psaLevel := range []string{"restricted", "baseline", "privileged"} {
			Step(fmt.Sprintf("Setup PSA level to %s", psaLevel), func() {

				err := SwitchBothKubeConfigANDContext("destination")
				dash.VerifyFatal(err, nil, "Setting destination kubeconfig and context")

				err = ConfigureClusterLevelPSA(psaLevel, []string{})
				dash.VerifyFatal(err, nil, "Setting cluster level PSA configuration")

				err = VerifyClusterlevelPSA()
				dash.VerifyFatal(err, nil, "Verify cluster level PSA configuration")

				err = SwitchBothKubeConfigANDContext("source")
				dash.VerifyFatal(err, nil, "Setting source kubeconfig")

				err = ConfigureClusterLevelPSA(psaLevel, []string{})
				dash.VerifyFatal(err, nil, "Setting cluster level PSA configuration")

				err = VerifyClusterlevelPSA()
				dash.VerifyFatal(err, nil, "Verify cluster level PSA configuration")
				psaFlag = true

			})

			Step(fmt.Sprintf("Taking backup of all the namespaces created with namespace level PSA and Cluster level PSA Set to %s", psaLevel), func() {
				log.InfoD(fmt.Sprintf("Taking backup of all the namespaces created with namespace level PSA and Cluster level PSA Set to %s", psaLevel))

				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")

				var wg sync.WaitGroup
				var mutex sync.Mutex
				labelSelectors := make(map[string]string)
				for backupLocationUID, backupLocationName := range backupLocationMap {
					for _, namespace := range bkpNamespaces {
						wg.Add(1)
						go func(namespace string) {
							defer wg.Done()
							defer GinkgoRecover()
							backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, RandomString(10), psaLevel)
							preRuleUid, preRule := "", ""
							if len(preRuleNameList) > 0 {
								preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
								log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameList[0])
								preRule = preRuleNameList[0]
							}
							postRuleUid, postRule := "", ""
							if len(postRuleNameList) > 0 {
								postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
								log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleNameList[0])
								postRule = postRuleNameList[0]
							}
							err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, bkpNamespaces, labelSelectors, BackupOrgID, sourceClusterUid, preRule, preRuleUid, postRule, postRuleUid, ctx)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] on namespace [%s]", backupName, namespace))
							err := BackupSuccessCheck(backupName, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s success state", backupName))
							mutex.Lock()
							backupNames2 = append(backupNames2, backupName)
							backupNSMap[backupName] = namespace
							mutex.Unlock()
						}(namespace)
					}
				}
				wg.Wait()
			})

			Step(fmt.Sprintf("Default restore of applications by replacing the existing resources with NS level PSA and Cluster level PSA Set to %s", psaLevel), func() {
				log.InfoD(fmt.Sprintf("Default restore of applications by replacing the existing resources with NS level PSA and Cluster level PSA Set to %s", psaLevel))
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				var wg sync.WaitGroup
				var mutex sync.Mutex
				for _, backupName := range backupNames {
					wg.Add(1)
					go func(backupName string) {
						defer wg.Done()
						defer GinkgoRecover()
						defaultRestoreName := fmt.Sprintf("%s-%s-%s-default-2", RestoreNamePrefix, backupName, psaLevel)
						err = CreateRestoreWithReplacePolicyWithValidation(defaultRestoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete, scheduledAppContexts)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creating default restore for manual backup with replace policy [%s]", defaultRestoreName))
						mutex.Lock()
						restoreNames = append(restoreNames, defaultRestoreName)
						mutex.Unlock()
					}(backupName)
				}
				wg.Wait()
			})

			Step(fmt.Sprintf("Restore of applications with NS and StorageClass mapping with NS level PSA with cluster level Set to %s", psaLevel), func() {
				log.InfoD(fmt.Sprintf("Restore of applications with NS and StorageClass mapping with NS level PSA with cluster level Set to %s", psaLevel))
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")

				for backupName, backupNamespace := range backupNSMap {
					// If cluster level is restricted and namespace level is baseline or privileged skip the restore as the apps won't come up
					if psaLevel == "restricted" && (strings.Contains(backupNamespace, "baseline") || strings.Contains(backupNamespace, "privileged")) {
						continue
					}
					appContextsToRestore := make([]*scheduler.Context, 0)
					namespaceMapping := make(map[string]string)
					customRestoreName := fmt.Sprintf("%s-%s-%s-custom-ns-sc-2", RestoreNamePrefix, backupName, psaLevel)
					namespaceMapping[backupNamespace] = backupNamespace + "-restored-2"
					restoredNamespaces = append(restoredNamespaces, backupNamespace+"-restored-2")
					appContextsToRestore = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNamespace})

					err = CreateRestoreWithReplacePolicyWithValidation(customRestoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, storageClassMapping, ReplacePolicyDelete, appContextsToRestore)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Create restore with NS and StorageClass mapping with NS level PSA [%s]", customRestoreName))

					restoreNames = append(restoreNames, customRestoreName)
				}
			})

			Step(fmt.Sprintf("Restore of applications with NS and StorageClass mapping with NS level PSA with cluster level Set to %s with pre-exisitng namespace", psaLevel), func() {
				log.InfoD(fmt.Sprintf("Restore of applications with NS and StorageClass mapping with NS level PSA with cluster level Set to %s with pre-exisitng namespace", psaLevel))
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				for backupName, backupNamespace := range backupNSMap {
					appContextsToRestore := make([]*scheduler.Context, 0)
					namespaceMapping := make(map[string]string)
					customRestoreName := fmt.Sprintf("%s-%s-%s-custom-ns-sc-pre-existing", RestoreNamePrefix, backupName, psaLevel)
					namespaceMapping[backupNamespace] = backupNamespace + "-restored-with-labels"
					restoredNamespaces = append(restoredNamespaces, backupNamespace+"-restored-with-labels")
					label["pod-security.kubernetes.io/enforce"] = strings.Split(backupNamespace, "-")[0]
					err = CreateNamespaceAndAssignLabels(backupNamespace+"-restored-with-labels", label)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating namespace and assigning labels"))
					appContextsToRestore = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNamespace})
					err = CreateRestoreWithReplacePolicyWithValidation(customRestoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, storageClassMapping, ReplacePolicyDelete, appContextsToRestore)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Create restore with NS and StorageClass mapping with NS level PSA [%s]", customRestoreName))
					restoreNames = append(restoreNames, customRestoreName)
				}
			})

			Step("Revert Cluster Level PSA settings", func() {
				err := SetDestinationKubeConfig()
				dash.VerifyFatal(err, nil, "Setting destination kubeconfig")
				err = RevertClusterLevelPSA()
				dash.VerifyFatal(err, nil, "Revert cluster level PSA configuration")

				err = SetSourceKubeConfig()
				dash.VerifyFatal(err, nil, "Setting source kubeconfig")
				err = RevertClusterLevelPSA()
				dash.VerifyFatal(err, nil, "Revert cluster level PSA configuration")
				psaFlag = false
			})

			Step("Taking backup of all the namespaces created with namespace level PSA and no cluster level settings in a single backup", func() {
				log.InfoD("Taking backup of all the namespaces created with namespace level PSA and no cluster level settings in a single backup")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				labelSelectors := make(map[string]string)

				for backupLocationUID, backupLocationName := range backupLocationMap {
					backupName := fmt.Sprintf("%s-%v-%s", BackupNamePrefix, RandomString(10), psaLevel)
					err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, bkpNamespaces, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
					err := BackupSuccessCheck(backupName, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s success state", backupName))
					backupNamesAllNs = append(backupNamesAllNs, backupName)
				}
			})

			Step("Default restore of applications by replacing the existing resources with NS level PSA and no cluster level setting", func() {
				log.InfoD("Default restore of applications by replacing the existing resources with NS level PSA and no cluster level setting")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")

				for _, backupName := range backupNamesAllNs {
					defaultRestoreName := fmt.Sprintf("%s-%s-%s-%s", RestoreNamePrefix, backupName, psaLevel, RandomString(10))
					err = CreateRestoreWithReplacePolicyWithValidation(defaultRestoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete, scheduledAppContexts)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating default restore for manual backup with replace policy [%s]", defaultRestoreName))
					restoreNames = append(restoreNames, defaultRestoreName)
				}
			})
		}
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)

		defer func() {
			err := SetSourceKubeConfig()
			dash.VerifyFatal(err, nil, "Setting source kubeconfig")
		}()

		// Make sure to revert the cluster level PSA settings
		defer func() {
			if psaFlag {
				err := SetDestinationKubeConfig()
				dash.VerifyFatal(err, nil, "Setting destination kubeconfig")
				err = RevertClusterLevelPSA()
				dash.VerifyFatal(err, nil, "Revert cluster level PSA configuration")

				err = SetSourceKubeConfig()
				dash.VerifyFatal(err, nil, "Setting source kubeconfig")
				err = RevertClusterLevelPSA()
				dash.VerifyFatal(err, nil, "Revert cluster level PSA configuration")
			}

			log.InfoD("Setting the original app list back post testcase")
			Inst().AppList = originalList
		}()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		if len(preRuleNameList) > 0 {
			for _, ruleName := range preRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(BackupOrgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup pre rules [%s]", ruleName))
			}
		}
		if len(postRuleNameList) > 0 {
			for _, ruleName := range postRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(BackupOrgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup post rules [%s]", ruleName))
			}
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.Info("Destroying scheduled apps on source cluster")
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		log.InfoD("switching to destination context")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "failed to switch to context to destination cluster")

		log.InfoD("Destroying restored apps on destination clusters")
		restoredAppContexts := make([]*scheduler.Context, 0)
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		DestroyApps(restoredAppContexts, opts)

		err = DeleteNamespaces(restoredNamespaces)
		log.FailOnError(err, "failed to delete restored namespaces")

		log.InfoD("switching to default context")
		err = SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		backupDriver := Inst().Backup
		log.Info("Deleting backed up namespaces")
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, BackupOrgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
			log.FailOnError(err, "Backup [%s] could not be deleted", backupName)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying [%s] backup deletion is successful", backupName))
		}

		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)

	})
})
