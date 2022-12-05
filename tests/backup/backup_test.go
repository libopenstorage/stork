package tests

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/log"

	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/portworx/torpedo/tests"

	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	bucketName string
)

func TearDownBackupRestore(bkpNamespaces []string, restoreNamespaces []string) {
	for _, bkpNamespace := range bkpNamespaces {
		BackupName := fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespace)
		backupUID := getBackupUID(OrgID, BackupName)
		DeleteBackup(BackupName, backupUID, OrgID)
	}
	for _, restoreNamespace := range restoreNamespaces {
		RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, restoreNamespace)
		DeleteRestore(RestoreName, OrgID)
	}

	provider := GetProvider()
	DeleteCluster(destinationClusterName, OrgID)
	DeleteCluster(sourceClusterName, OrgID)
	DeleteBackupLocation(backupLocationName, OrgID)
	DeleteCloudCredential(CredName, OrgID, CloudCredUID)
	DeleteBucket(provider, BucketName)
}

// This testcase verifies if the backup pods are in Ready state or not
var _ = Describe("{BackupClusterVerification}", func() {
	JustBeforeEach(func() {
		log.Infof("No pre-setup required for this testcase")
		StartTorpedoTest("Backup: BackupClusterVerification", "Validating backup cluster pods", nil, 0)
	})
	It("Backup Cluster Verification", func() {
		Step("Check the status of backup pods", func() {
			log.InfoD("Check the status of backup pods")
			status := ValidateBackupCluster()
			dash.VerifyFatal(status, true, "Backup Cluster Verification successful?")
		})
		//Will add CRD verification here
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.Infof("No cleanup required for this testcase")
	})
})

// This is a sample test case to verify User/Group Management and role mapping
var _ = Describe("{UserGroupManagement}", func() {
	JustBeforeEach(func() {
		log.Infof("No pre-setup required for this testcase")
		StartTorpedoTest("Backup: UserGroupManagement", "Creating users and adding them to groups", nil, 0)
	})
	It("User and group role mappings", func() {
		Step("Create Users", func() {
			err := backup.AddUser("testuser1", "test", "user1", "testuser1@localhost.com", "Password1")
			log.FailOnError(err, "Failed to create user")
		})
		Step("Create Groups", func() {
			err := backup.AddGroup("testgroup1")
			log.FailOnError(err, "Failed to create group")
		})
		Step("Add users to group", func() {
			err := backup.AddGroupToUser("testuser1", "testgroup1")
			log.FailOnError(err, "Failed to assign group to user")
		})
		Step("Assign role to groups", func() {
			err := backup.AddRoleToGroup("testgroup1", backup.ApplicationOwner, "testing from torpedo")
			log.FailOnError(err, "Failed to assign group to user")
		})
		Step("Verify Application Owner role permissions for user", func() {
			isUserRoleMapped, err := ValidateUserRole("testuser1", backup.ApplicationOwner)
			log.FailOnError(err, "User does not contain the expected role")
			dash.VerifyFatal(isUserRoleMapped, true, "Verifying the user role mapping")
		})
		Step("Update role to groups", func() {
			err := backup.DeleteRoleFromGroup("testgroup1", backup.ApplicationOwner, "removing role from testgroup1")
			log.FailOnError(err, "Failed to delete role from group")
			err = backup.AddRoleToGroup("testgroup1", backup.ApplicationUser, "testing from torpedo")
			log.FailOnError(err, "Failed to add role to group")
		})
		Step("Verify Application User role permissions for user", func() {
			isUserRoleMapped, err := ValidateUserRole("testuser1", backup.ApplicationUser)
			log.FailOnError(err, "User does not contain the expected role")
			dash.VerifyFatal(isUserRoleMapped, true, "Verifying the user role mapping")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.Infof("Cleanup started")
		err := backup.DeleteUser("testuser1")
		log.FailOnError(err, "Failed to delete user(s)")
		err = backup.DeleteGroup("testgroup1")
		log.FailOnError(err, "Failed to delete group(s)")
		log.Infof("Cleanup done")
	})
})

// This testcase verifies basic backup rule,backup location, cloud setting
var _ = Describe("{BasicBackupCreation}", func() {
	var (
		ps       = make(map[string]map[string]string)
		app_list = Inst().AppList
	)
	var contexts []*scheduler.Context
	labelSelectores := make(map[string]string)
	var CloudCredUID_list []string
	var appContexts []*scheduler.Context
	var backup_location_name string
	var bkpNamespaces []string
	var cluster_uid string
	var cluster_status api.ClusterInfo_StatusInfo_Status
	bkpNamespaces = make([]string, 0)
	var pre_rule_uid string
	var post_rule_uid string
	var pre_rule_status bool
	var post_rule_status bool
	var namespaceMapping map[string]string
	namespaceMapping = make(map[string]string)
	providers := getProviders()
	JustBeforeEach(func() {
		StartTorpedoTest("Backup: BasicBackupCreation", "Deploying backup", nil, 0)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not ")
		for i := 0; i < len(app_list); i++ {
			if Contains(post_rule_app, app_list[i]) {
				if _, ok := app_parameters[app_list[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(pre_rule_app, app_list[i]) {
				if _, ok := app_parameters[app_list[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})
	It("Basic Backup Creation", func() {
		Step("Validate applications and get their labels", func() {
			ValidateApplications(contexts)
			log.Infof("Create list of pod selector for the apps deployed")
			for _, ctx := range appContexts {
				for _, specObj := range ctx.App.SpecList {
					if obj, ok := specObj.(*appsapi.Deployment); ok {
						if Contains(app_list, obj.Name) {
							ps[obj.Name] = obj.Spec.Template.Labels
						}
					} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
						if Contains(app_list, obj.Name) {
							ps[obj.Name] = obj.Spec.Template.Labels
						}
					}
				}
			}
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			pre_rule_status, pre_rule_uid = CreateRuleForBackup("backup-pre-rule", "default", app_list, "pre", ps)
			dash.VerifyFatal(pre_rule_status, true, "Verifying pre rule for backup")
			log.InfoD("Creating post rule for deployed apps")
			post_rule_status, post_rule_uid = CreateRuleForBackup("backup-post-rule", "default", app_list, "post", ps)
			dash.VerifyFatal(post_rule_status, true, "Verifying Post rule for backup")
		})

		Step("Creating bucket,backup location and cloud setting", func() {
			log.InfoD("Creating bucket,backup location and cloud setting")
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", "bucket", provider)
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				backup_location_name = fmt.Sprintf("%s-%s", "location", provider)
				CloudCredUID = uuid.New()
				CloudCredUID_list = append(CloudCredUID_list, CloudCredUID)
				BackupLocationUID = uuid.New()
				CreateBucket(provider, bucketName)
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
				time.Sleep(time.Minute * 1)
				CreateBackupLocation(provider, backup_location_name, BackupLocationUID, CredName, CloudCredUID, bucketName, orgID)
			}
		})

		Step("Creating backup schedule policies", func() {
			log.InfoD("Creating backup interval schedule policy")
			interval_schedule_policy_info := CreateIntervalSchedulePolicy(5, 15, 2)
			interval_policy_status := Backupschedulepolicy("interval", uuid.New(), orgID, interval_schedule_policy_info)
			dash.VerifyFatal(interval_policy_status, nil, "Creating interval schedule policy")

			log.InfoD("Creating backup daily schedule policy")
			daily_schedule_policy_info := CreateDailySchedulePolicy(1, "9:00AM", 2)
			daily_policy_status := Backupschedulepolicy("daily", uuid.New(), orgID, daily_schedule_policy_info)
			dash.VerifyFatal(daily_policy_status, nil, "Creating daily schedule policy")

			log.InfoD("Creating backup weekly schedule policy")
			weekly_schedule_policy_info := CreateWeeklySchedulePolicy(1, Friday, "9:10AM", 2)
			weekly_policy_status := Backupschedulepolicy("weekly", uuid.New(), orgID, weekly_schedule_policy_info)
			dash.VerifyFatal(weekly_policy_status, nil, "Creating weekly schedule policy")

			log.InfoD("Creating backup monthly schedule policy")
			monthly_schedule_policy_info := CreateMonthlySchedulePolicy(1, 29, "9:20AM", 2)
			monthly_policy_status := Backupschedulepolicy("monthly", uuid.New(), orgID, monthly_schedule_policy_info)
			dash.VerifyFatal(monthly_policy_status, nil, "Creating monthly schedule policy")
		})

		Step("Register cluster for backup", func() {
			cluster_status, cluster_uid = RegisterBackupCluster(orgID, "", "")
			dash.VerifyFatal(cluster_status, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Taking backup of applications", func() {
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				CreateBackup(backupName, sourceClusterName, backup_location_name, BackupLocationUID, []string{namespace},
					labelSelectores, orgID, cluster_uid, "backup-pre-rule", pre_rule_uid, "backup-post-rule", post_rule_uid)
			}
		})

		Step("Restoring the backed up application", func() {
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				CreateRestore("test-restore", backupName, namespaceMapping, destinationClusterName, orgID)
			}
		})

	})
	JustAfterEach(func() {
		policy_list := []string{"interval", "daily", "weekly", "monthly"}
		defer EndTorpedoTest()
		teardown_status := TeardownForTestcase(contexts, providers, CloudCredUID_list, policy_list)
		dash.VerifyFatal(teardown_status, true, "Testcase teardown status")
	})
})

// This test performs basic test of starting an application, backing it up and killing stork while
// performing backup.
var _ = Describe("{BackupCreateKillStorkRestore}", func() {
	var (
		contexts         []*scheduler.Context
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backupcreaterestore"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)

	BeforeEach(func() {
		StartTorpedoTest("BackupCreateKillStorkRestore", "Validate Backup Create and Kill Stork and Restore", nil, 0)
		wantAllAfterSuiteActions = false
	})

	It("has to connect and check the backup setup", func() {
		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})

		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})

		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 5)

		// TODO(stgleb): Add multi-namespace backup when ready in px-backup
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				sourceClusterName, namespace, backupName), func() {
				CreateBackup(backupName,
					sourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectores, orgID, "", "", "", "", "")
			})
		}

		Step("Kill stork during backup", func() {
			// setup task to delete stork pods as soon as it starts doing backup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				backupUID := getBackupUID(orgID, backupName)
				req := &api.BackupInspectRequest{
					Name:  backupName,
					OrgId: orgID,
					Uid:   backupUID,
				}

				log.Infof("backup %s wait for running", backupName)
				err := Inst().Backup.WaitForBackupRunning(context.Background(),
					req, BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)

				if err != nil {
					log.Warnf("backup %s wait for running err %v",
						backupName, err)
					continue
				} else {
					break
				}
			}

			ctx := &scheduler.Context{
				App: &spec.AppSpec{
					SpecList: []interface{}{
						&appsapi.Deployment{
							ObjectMeta: meta_v1.ObjectMeta{
								Name:      storkDeploymentName,
								Namespace: storkDeploymentNamespace,
							},
						},
					},
				},
			}
			log.Infof("Execute task for killing stork")
			err := Inst().S.DeleteTasks(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
		})

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
				err := Inst().Backup.WaitForBackupCompletion(
					context.Background(),
					backupName, orgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err))
			})
		}

		Step("teardown all applications on source cluster before switching context to destination cluster", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{
					SkipClusterScopedObjects:                    true,
					scheduler.OptionsWaitForResourceLeakCleanup: true,
					scheduler.OptionsWaitForDestroy:             true,
				})
			}
		})

		destClusterConfigPath, err := GetDestinationClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

		SetClusterContext(destClusterConfigPath)
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Create restore %s:%s:%s from backup %s:%s:%s",
				destinationClusterName, namespace, restoreName,
				sourceClusterName, namespace, backupName), func() {
				CreateRestore(restoreName, backupName, namespaceMapping,
					destinationClusterName, orgID)
			})
		}

		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for restore %s:%s to complete",
				namespace, restoreName), func() {

				err := Inst().Backup.WaitForRestoreCompletion(context.Background(), restoreName, orgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
						restoreName, err))
			})
		}

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step("Validate Restored applications", func() {
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)

			// Populate contexts
			for _, ctx := range contexts {
				ctx.SkipClusterScopedObject = true
				ctx.SkipVolumeValidation = true
			}
			ValidateRestoredApplications(contexts, volumeParams)
		})

		Step("teardown all restored apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

		Step("teardown backup objects", func() {
			TearDownBackupRestore(bkpNamespaces, bkpNamespaces)
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

// This performs scale test of px-backup and kills stork in the middle of
// backup process.
var _ = Describe("{MultiProviderBackupKillStork}", func() {
	var (
		kubeconfigs    string
		kubeconfigList []string
	)

	contexts := make(map[string][]*scheduler.Context)
	bkpNamespaces := make(map[string][]string)
	labelSelectores := make(map[string]string)
	namespaceMapping := make(map[string]string)
	taskNamePrefix := "backup-multi-provider"
	providerUID := make(map[string]string)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("MultiProviderBackupKillStork", "Performs scale test of px-backup and kills stork in the middle", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		providers := getProviders()

		Step("Setup backup", func() {
			kubeconfigs = os.Getenv("KUBECONFIGS")

			if len(kubeconfigs) == 0 {
				Expect(kubeconfigs).NotTo(BeEmpty(),
					fmt.Sprintf("KUBECONFIGS %s must not be empty", kubeconfigs))
			}

			kubeconfigList = strings.Split(kubeconfigs, ",")
			// Validate user has provided at least 1 kubeconfig for cluster
			if len(kubeconfigList) == 0 {
				Expect(kubeconfigList).NotTo(BeEmpty(),
					fmt.Sprintf("kubeconfigList %v must have at least one", kubeconfigList))
			}

			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			DumpKubeconfigs(kubeconfigList)

			for _, provider := range providers {
				log.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
					provider, Inst().InstanceID)
				bucketName = fmt.Sprintf("%s-%s-%s", BucketNamePrefix, provider, Inst().InstanceID)
				CredName := fmt.Sprintf("%s-%s", CredName, provider)
				CloudCredUID = uuid.New()
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				providerUID[provider] = uuid.New()

				CreateBucket(provider, bucketName)
				CreateOrganization(orgID)
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
				CreateBackupLocation(provider, backupLocation, providerUID[provider], CredName, CloudCredUID, BucketName, orgID)
				CreateProviderClusterObject(provider, kubeconfigList, CredName, orgID)
			}
		})

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(Inst().MinRunTimeMins) * time.Minute)

		for time.Now().Before(end) {
			Step("Deploy applications", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					log.Infof("Set context to %s", providerClusterConfigPath)
					SetClusterContext(providerClusterConfigPath)

					providerContexts := make([]*scheduler.Context, 0)
					providerNamespaces := make([]string, 0)

					// Rescan specs for each provider to reload provider specific specs
					log.Infof("Rescan specs for provider %s", provider)
					err = Inst().S.RescanSpecs(Inst().SpecDir, provider)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to rescan specs from %s for storage provider %s. Error: [%v]",
							Inst().SpecDir, provider, err))

					log.Infof("Start deploy applications for provider %s", provider)
					for i := 0; i < Inst().GlobalScaleFactor; i++ {
						taskName := fmt.Sprintf("%s-%s-%d", taskNamePrefix, provider, i)
						log.Infof("Task name %s\n", taskName)
						appContexts := ScheduleApplications(taskName)
						providerContexts = append(providerContexts, appContexts...)

						for _, ctx := range appContexts {
							namespace := GetAppNamespace(ctx, taskName)
							providerNamespaces = append(providerNamespaces, namespace)
						}
					}

					contexts[provider] = providerContexts
					bkpNamespaces[provider] = providerNamespaces
				}
			})

			Step("Validate applications", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					// In case of non-portworx volume provider skip volume validation until
					// other volume providers are implemented.
					for _, ctx := range contexts[provider] {
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = BackupRestoreCompletionTimeoutMin * time.Minute
					}

					log.Infof("validate applications for provider %s", provider)
					ValidateApplications(contexts[provider])
				}
			})

			log.Info("Wait for IO to proceed\n")
			time.Sleep(time.Minute * 5)

			// Perform all backup operations concurrently
			// TODO(stgleb): Add multi-namespace backup when ready in px-backup
			for _, provider := range providers {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				ctx, _ := context.WithTimeout(context.Background(),
					BackupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider,
							namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						// NOTE: We don't use CreateBackup/Restore method here since it has ginkgo assertion
						// which must be called inside of goroutine with GinkgoRecover https://onsi.github.io/ginkgo/#marking-specs-as-failed
						Step(fmt.Sprintf("Create backup full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							backupDriver := Inst().Backup
							bkpCreateRequest := &api.BackupCreateRequest{
								CreateMetadata: &api.CreateMetadata{
									Name:  backupName,
									OrgId: orgID,
								},
								BackupLocation: backupLocation,
								Cluster:        clusterName,
								Namespaces:     []string{namespace},
								LabelSelectors: labelSelectores,
							}
							//ctx, err := backup.GetPxCentralAdminCtx()
							ctx, err := backup.GetAdminCtxFromSecret()
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
									err))
							_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
							errChan <- err
						})
					}(provider, namespace)
				}

				for i := 0; i < len(bkpNamespaces[provider]); i++ {
					select {
					case <-ctx.Done():
						Expect(ctx.Err()).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
					case err := <-errChan:
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
					}
				}
			}

			Step("Kill stork during backup", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					log.Infof("Kill stork during backup for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						backupUID := getBackupUID(orgID, backupName)
						req := &api.BackupInspectRequest{
							Name:  backupName,
							OrgId: orgID,
							Uid:   backupUID,
						}

						log.Infof("backup %s wait for running", backupName)
						err := Inst().Backup.WaitForBackupRunning(context.Background(),
							req, BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					killStork()
				}
			})

			// wait until all backups are completed, there is no need to parallel here
			for provider, namespaces := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				for _, namespace := range namespaces {
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for backup %s to complete in organization %s",
						backupName, orgID), func() {
						err := Inst().Backup.WaitForBackupCompletion(
							context.Background(),
							backupName, orgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
								backupName, err))
					})
				}
			}

			Step("teardown all applications on source cluster before switching context to destination cluster", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					log.Infof("Set config to %s", providerClusterConfigPath)
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						TearDownContext(ctx, map[string]bool{
							SkipClusterScopedObjects:                    true,
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})

			for provider := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				ctx, _ := context.WithTimeout(context.Background(),
					BackupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
						restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						Step(fmt.Sprintf("Create restore full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							// NOTE: We don't use CreateBackup/Restore method here since it has ginkgo assertion
							// which must be called inside of gorutuine with GinkgoRecover https://onsi.github.io/ginkgo/#marking-specs-as-failed
							backupDriver := Inst().Backup
							createRestoreReq := &api.RestoreCreateRequest{
								CreateMetadata: &api.CreateMetadata{
									Name:  restoreName,
									OrgId: orgID,
								},
								Backup:           backupName,
								Cluster:          clusterName,
								NamespaceMapping: namespaceMapping,
							}
							//ctx, err := backup.GetPxCentralAdminCtx()
							ctx, err := backup.GetAdminCtxFromSecret()
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
									err))
							_, err = backupDriver.CreateRestore(ctx, createRestoreReq)

							errChan <- err
						})
					}(provider, namespace)
				}

				for i := 0; i < len(bkpNamespaces[provider]); i++ {
					select {
					case <-ctx.Done():
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
					case err := <-errChan:
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
					}
				}
			}

			Step("Kill stork during restore", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					log.Infof("Kill stork during restore for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						req := &api.RestoreInspectRequest{
							Name:  restoreName,
							OrgId: orgID,
						}

						log.Infof("restore %s wait for running", restoreName)
						err := Inst().Backup.WaitForRestoreRunning(context.Background(),
							req, BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					log.Infof("Kill stork task")
					killStork()
				}
			})

			for provider, providerNamespaces := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				for _, namespace := range providerNamespaces {
					restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						err := Inst().Backup.WaitForRestoreCompletion(context.Background(),
							restoreName, orgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
								restoreName, err))
					})
				}
			}

			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				// Populate contexts
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						ctx.SkipClusterScopedObject = true
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = BackupRestoreCompletionTimeoutMin * time.Minute

						err := Inst().S.WaitForRunning(ctx, defaultTimeout, defaultRetryInterval)
						Expect(err).NotTo(HaveOccurred())
					}

					ValidateApplications(contexts[provider])
				}
			})

			Step("teardown all restored apps", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						TearDownContext(ctx, map[string]bool{
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})

			Step("teardown backup and restore objects", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					log.Infof("teardown backup and restore objects for provider %s", provider)
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					ctx, _ := context.WithTimeout(context.Background(),
						BackupRestoreCompletionTimeoutMin*time.Minute)
					errChan := make(chan error)

					for _, namespace := range providerNamespaces {
						go func(provider, namespace string) {
							clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
							backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
							orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
								provider, Inst().InstanceID)
							Step(fmt.Sprintf("Delete backup full name %s:%s:%s",
								clusterName, namespace, backupName), func() {
								backupDriver := Inst().Backup
								backupUID := getBackupUID(orgID, backupName)
								bkpDeleteRequest := &api.BackupDeleteRequest{
									Name:  backupName,
									OrgId: orgID,
									Uid:   backupUID,
								}
								//	ctx, err = backup.GetPxCentralAdminCtx()
								ctx, err = backup.GetAdminCtxFromSecret()
								Expect(err).NotTo(HaveOccurred(),
									fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
										err))
								_, err = backupDriver.DeleteBackup(ctx, bkpDeleteRequest)

								ctx, _ := context.WithTimeout(context.Background(),
									BackupRestoreCompletionTimeoutMin*time.Minute)

								if err = backupDriver.WaitForBackupDeletion(ctx, backupName, orgID,
									BackupRestoreCompletionTimeoutMin*time.Minute,
									RetrySeconds*time.Second); err != nil {
									errChan <- err
									return
								}

								errChan <- err
							})
						}(provider, namespace)

						go func(provider, namespace string) {
							clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
							restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
							orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
								provider, Inst().InstanceID)
							Step(fmt.Sprintf("Delete restore full name %s:%s:%s",
								clusterName, namespace, restoreName), func() {
								backupDriver := Inst().Backup
								deleteRestoreReq := &api.RestoreDeleteRequest{
									OrgId: orgID,
									Name:  restoreName,
								}
								//ctx, err = backup.GetPxCentralAdminCtx()
								ctx, err = backup.GetAdminCtxFromSecret()
								Expect(err).NotTo(HaveOccurred(),
									fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
										err))
								_, err = backupDriver.DeleteRestore(ctx, deleteRestoreReq)

								ctx, _ := context.WithTimeout(context.Background(),
									BackupRestoreCompletionTimeoutMin*time.Minute)

								log.Infof("Wait for restore %s is deleted", restoreName)
								if err = backupDriver.WaitForRestoreDeletion(ctx, restoreName, orgID,
									BackupRestoreCompletionTimeoutMin*time.Minute,
									RetrySeconds*time.Second); err != nil {
									errChan <- err
									return
								}

								errChan <- err
							})
						}(provider, namespace)
					}

					for i := 0; i < len(providerNamespaces)*2; i++ {
						select {
						case <-ctx.Done():
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
						case err := <-errChan:
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
						}
					}
				}
			})
		}

		Step("teardown backup objects for test", func() {
			for _, provider := range providers {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				log.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix), provider, Inst().InstanceID)
				bucketName := fmt.Sprintf("%s-%s-%s", BucketNamePrefix, provider, Inst().InstanceID)
				CredName := fmt.Sprintf("%s-%s", CredName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				clusterName := fmt.Sprintf("%s-%s", clusterName, provider)

				DeleteCluster(clusterName, orgID)
				DeleteBackupLocation(backupLocation, orgID)
				DeleteCloudCredential(CredName, orgID, CloudCredUID)
				DeleteBucket(provider, bucketName)
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

func killStork() {
	ctx := &scheduler.Context{
		App: &spec.AppSpec{
			SpecList: []interface{}{
				&appsapi.Deployment{
					ObjectMeta: meta_v1.ObjectMeta{
						Name:      storkDeploymentName,
						Namespace: storkDeploymentNamespace,
					},
				},
			},
		},
	}
	log.Infof("Execute task for killing stork")
	err := Inst().S.DeleteTasks(ctx, nil)
	Expect(err).NotTo(HaveOccurred())
}

// This test crashes volume driver (PX) while backup is in progress
var _ = Describe("{BackupCrashVolDriver}", func() {
	var contexts []*scheduler.Context
	var namespaceMapping map[string]string
	taskNamePrefix := "backupcrashvoldriver"
	labelSelectors := make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	bkpNamespaces := make([]string, 0)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupCrashVolDriver", "crashes volume driver (PX) while backup is in progress", nil, 0)
	})

	It("has to complete backup and restore", func() {
		// Set cluster context to cluster where torpedo is running
		SetClusterContext("")
		SetupBackup(taskNamePrefix)

		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)

				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}
			// Override default App readiness time out of 5 mins with 10 mins
			for _, ctx := range contexts {
				ctx.ReadinessTimeout = appReadinessTimeout
			}
			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})

		for _, bkpNamespace := range bkpNamespaces {
			BackupName := fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespace)

			Step(fmt.Sprintf("Create Backup [%s]", BackupName), func() {
				CreateBackup(BackupName, sourceClusterName, backupLocationName, BackupLocationUID,
					[]string{bkpNamespace}, labelSelectors, OrgID, "", "", "", "", "")
			})

			triggerFn := func() (bool, error) {
				backupUID := getBackupUID(OrgID, BackupName)
				backupInspectReq := &api.BackupInspectRequest{
					Name:  BackupName,
					OrgId: OrgID,
					Uid:   backupUID,
				}
				//ctx, err := backup.GetPxCentralAdminCtx()
				ctx, err := backup.GetAdminCtxFromSecret()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
						err))
				err = Inst().Backup.WaitForBackupRunning(ctx, backupInspectReq, defaultTimeout, defaultRetryInterval)
				if err != nil {
					log.Warnf("[TriggerCheck]: Got error while checking if backup [%s] has started.\n Error : [%v]\n",
						BackupName, err)
					return false, err
				}
				log.Infof("[TriggerCheck]: backup [%s] has started.\n",
					BackupName)
				return true, nil
			}

			triggerOpts := &driver_api.TriggerOptions{
				TriggerCb: triggerFn,
			}

			bkpNode := GetNodesForBackup(BackupName, bkpNamespace,
				OrgID, sourceClusterName, triggerOpts)
			Expect(len(bkpNode)).NotTo(Equal(0),
				fmt.Sprintf("Did not found any node on which backup [%v] is running.",
					BackupName))

			Step(fmt.Sprintf("Kill volume driver %s on node [%v] after backup [%s] starts",
				Inst().V.String(), bkpNode[0].Name, BackupName), func() {
				// Just kill storage driver on one of the node where volume backup is in progress
				Inst().V.StopDriver(bkpNode[0:1], true, triggerOpts)
			})

			Step(fmt.Sprintf("Wait for Backup [%s] to complete", BackupName), func() {
				//ctx, err := backup.GetPxCentralAdminCtx()
				ctx, err := backup.GetAdminCtxFromSecret()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
						err))
				err = Inst().Backup.WaitForBackupCompletion(ctx, BackupName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
						BackupName, err))
			})
		}

		for _, bkpNamespace := range bkpNamespaces {
			BackupName := fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespace)
			RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, bkpNamespace)
			Step(fmt.Sprintf("Create Restore [%s]", RestoreName), func() {
				CreateRestore(RestoreName, BackupName,
					namespaceMapping, destinationClusterName, OrgID)
			})

			Step(fmt.Sprintf("Wait for Restore [%s] to complete", RestoreName), func() {
				//ctx, err := backup.GetPxCentralAdminCtx()
				ctx, err := backup.GetAdminCtxFromSecret()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
						err))
				err = Inst().Backup.WaitForRestoreCompletion(ctx, RestoreName, OrgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
						RestoreName, err))
			})
		}

		Step("teardown all applications on source cluster before switching context to destination cluster", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{
					SkipClusterScopedObjects: true,
				})
			}
		})

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step(fmt.Sprintf("Validate Restored applications"), func() {
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)

			for _, ctx := range contexts {
				err = Inst().S.WaitForRunning(ctx, defaultTimeout, defaultRetryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
			// TODO: Restored PVCs are created by stork-snapshot StorageClass
			// And not by respective app's StorageClass. Need to fix below function
			ValidateRestoredApplications(contexts, volumeParams)
		})

		Step("teardown all restored apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

		Step("teardown backup objects", func() {
			TearDownBackupRestore(bkpNamespaces, bkpNamespaces)
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{BackupRestoreSimultaneous}", func() {
	var (
		contexts           []*scheduler.Context
		bkpNamespaces      []string
		namespaceMapping   map[string]string
		taskNamePrefix     = "backuprestoresimultaneous"
		successfulBackups  int
		successfulRestores int
	)

	labelSelectors := make(map[string]string)
	namespaceMapping = make(map[string]string)
	bkpNamespaceErrors := make(map[string]error)
	volumeParams := make(map[string]map[string]string)
	restoreNamespaces := make([]string, 0)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreSimultaneous", "Backup Restore Simultaneously", nil, 0)
	})

	It("has to perform simultaneous backups and restores", func() {
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))

		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})

		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				sourceClusterName, namespace, backupName), func() {
				err = CreateBackupGetErr(backupName,
					sourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectors, OrgID)
				if err != nil {
					bkpNamespaceErrors[namespace] = err
				}
			})
		}

		var wg sync.WaitGroup
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Warnf("Skipping waiting for backup %s because %s", backupName, error)
			} else {
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, backupName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
						err = Inst().Backup.WaitForBackupCompletion(
							ctx,
							backupName, OrgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						if err != nil {
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
								backupName, err)
						}
					})
				}(&wg, namespace, backupName)
			}
		}
		wg.Wait()

		successfulBackups = len(bkpNamespaces) - len(bkpNamespaceErrors)

		if successfulBackups == len(bkpNamespaces) {
			Step("teardown all applications on source cluster before switching context to destination cluster", func() {
				for _, ctx := range contexts {
					TearDownContext(ctx, map[string]bool{
						SkipClusterScopedObjects:                    true,
						scheduler.OptionsWaitForResourceLeakCleanup: true,
						scheduler.OptionsWaitForDestroy:             true,
					})
				}
			})
		}

		destClusterConfigPath, err := GetDestinationClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

		SetClusterContext(destClusterConfigPath)
		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Infof("Skipping create restore %s because %s", restoreName, error)
			} else {
				restoreNamespaces = append(restoreNamespaces, namespace)
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				Step(fmt.Sprintf("Create restore %s:%s:%s from backup %s:%s:%s",
					destinationClusterName, namespace, restoreName,
					sourceClusterName, namespace, backupName), func() {
					err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
						destinationClusterName, OrgID)
					if err != nil {
						bkpNamespaceErrors[namespace] = err
					}
				})
			}
		}

		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Infof("Skipping waiting for restore %s because %s", restoreName, error)
			} else {
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, restoreName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						if err != nil {
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
								restoreName, err)
						}
					})
				}(&wg, namespace, restoreName)
			}
		}
		wg.Wait()

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step("Validate Restored applications", func() {
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)

			// Populate contexts
			for _, ctx := range contexts {
				ctx.SkipClusterScopedObject = true
				ctx.SkipVolumeValidation = true
			}

			ValidateRestoredApplicationsGetErr(contexts, volumeParams, bkpNamespaceErrors)
		})

		successfulRestores = len(bkpNamespaces) - len(bkpNamespaceErrors)

		if len(bkpNamespaceErrors) == 0 {
			Step("teardown all restored apps", func() {
				for _, ctx := range contexts {
					TearDownContext(ctx, nil)
				}
			})

			Step("teardown backup objects", func() {
				TearDownBackupRestore(bkpNamespaces, restoreNamespaces)
			})
		}

		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, len(bkpNamespaces))
			log.Infof("%d/%d restores succeeded.", successfulRestores, successfulBackups)
		})

		Step("view errors", func() {
			log.Infof("There were %d errors during this test", len(bkpNamespaceErrors))

			var combinedErrors []string
			for namespace, err := range bkpNamespaceErrors {
				errString := fmt.Sprintf("%s: %s", namespace, err.Error())
				combinedErrors = append(combinedErrors, errString)
			}

			if len(combinedErrors) > 0 {
				err = fmt.Errorf(strings.Join(combinedErrors, "\n"))
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{BackupRestoreOverPeriod}", func() {
	var (
		numBackups             = 0
		successfulBackups      = 0
		successfulBackupNames  []string
		numRestores            = 0
		successfulRestores     = 0
		successfulRestoreNames []string
	)
	var (
		contexts         []*scheduler.Context //for restored apps
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backuprestoreperiod"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	namespaceContextMap := make(map[string][]*scheduler.Context)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreOverPeriod", "Backup and Restore Over a period of time", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)

		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)
		Step("Deploy applications", func() {
			successfulBackupNames = make([]string, 0)
			successfulRestoreNames = make([]string, 0)
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					namespaceContextMap[namespace] = append(namespaceContextMap[namespace], ctx)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})
		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 2)

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(5) * time.Minute)
		counter := 0
		for time.Now().Before(end) {
			counter++
			aliveBackup := make(map[string]bool)
			aliveRestore := make(map[string]bool)
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				continue
			}

			SetClusterContext(sourceClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				numBackups++
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				aliveBackup[namespace] = true
				Step(fmt.Sprintf("Create backup full name %s:%s:%s",
					sourceClusterName, namespace, backupName), func() {
					err = CreateBackupGetErr(backupName,
						sourceClusterName, backupLocationName, BackupLocationUID,
						[]string{namespace}, labelSelectores, OrgID)
					if err != nil {
						aliveBackup[namespace] = false
						log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
					}
				})
			}
			for _, namespace := range bkpNamespaces {
				if !aliveBackup[namespace] {
					continue
				}
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
					err = Inst().Backup.WaitForBackupCompletion(
						ctx,
						backupName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						log.Infof("Backup [%s] completed successfully", backupName)
						successfulBackups++
					} else {
						log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
							backupName, err)
						aliveBackup[namespace] = false
					}
				})
			}

			// Set kubeconfig to destination for restore
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				if !aliveBackup[namespace] {
					continue
				}
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				numRestores++
				aliveRestore[namespace] = true
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Create restore full name %s:%s:%s",
					destinationClusterName, namespace, restoreName), func() {
					err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
						destinationClusterName, OrgID)
					if err != nil {
						log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
							restoreName, OrgID, clusterName, err)
						aliveRestore[namespace] = false
					}
				})
			}
			for _, namespace := range bkpNamespaces {
				if !aliveRestore[namespace] {
					continue
				}
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Wait for restore %s:%s to complete",
					namespace, restoreName), func() {
					err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						log.Infof("Restore [%s] completed successfully", restoreName)
						successfulRestores++
					} else {
						log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
							restoreName, err)
						aliveRestore[namespace] = false
					}
				})
			}
			for namespace, alive := range aliveBackup {
				if alive {
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					successfulBackupNames = append(successfulBackupNames, backupName)
				}
			}
			remainingContexts := make([]*scheduler.Context, 0)
			for namespace, alive := range aliveRestore {
				if alive {
					restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
					successfulRestoreNames = append(successfulRestoreNames, restoreName)
					for _, ctx := range namespaceContextMap[namespace] {
						remainingContexts = append(remainingContexts, ctx)
					}
				}
			}
			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				destClusterConfigPath, err := GetDestinationClusterConfigPath()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

				SetClusterContext(destClusterConfigPath)

				// Populate contexts
				for _, ctx := range remainingContexts {
					ctx.SkipClusterScopedObject = true
					ctx.SkipVolumeValidation = true
				}
				// TODO check why PX-Backup does not copy group params correctly after restore
				for _, param := range volumeParams {
					if _, ok := param["backupGroupCheckSkip"]; !ok {
						param["backupGroupCheckSkip"] = "true"
					}
				}
				ValidateRestoredApplications(remainingContexts, volumeParams)
			})
			if successfulRestores == numRestores {
				Step("teardown all restored apps", func() {
					for _, ctx := range remainingContexts {
						TearDownContext(ctx, nil)
					}
				})
			}
		}

		if successfulBackups == numBackups && successfulRestores == numRestores {
			Step("teardown applications on source cluster", func() {
				sourceClusterConfigPath, err := GetSourceClusterConfigPath()
				if err != nil {
					log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				} else {
					SetClusterContext(sourceClusterConfigPath)
					for _, ctx := range contexts {
						TearDownContext(ctx, map[string]bool{
							SkipClusterScopedObjects:                    true,
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})
			Step("teardown backup/restore objects", func() {
				TearDownBackupRestoreSpecific(successfulBackupNames, successfulRestoreNames)
			})
		}
		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			log.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{BackupRestoreOverPeriodSimultaneous}", func() {
	var (
		numBackups             int32 = 0
		successfulBackups      int32 = 0
		successfulBackupNames  []string
		numRestores            int32 = 0
		successfulRestores     int32 = 0
		successfulRestoreNames []string
	)
	var (
		contexts         []*scheduler.Context //for restored apps
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backuprestoreperiodsimultaneous"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	namespaceContextMap := make(map[string][]*scheduler.Context)
	combinedErrors := make([]string, 0)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreOverPeriodSimultaneous", "BackupRestoreOverPeriodSimultaneous", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)
		Step("Deploy applications", func() {
			successfulBackupNames = make([]string, 0)
			successfulRestoreNames = make([]string, 0)
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					namespaceContextMap[namespace] = append(namespaceContextMap[namespace], ctx)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})
		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 2)

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(5) * time.Minute)
		counter := 0
		for time.Now().Before(end) {
			counter++
			bkpNamespaceErrors := make(map[string]error)
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				continue
			}
			/*Expect(err).NotTo(HaveOccurred(),
			  fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))*/
			SetClusterContext(sourceClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				go func(namespace string) {
					atomic.AddInt32(&numBackups, 1)
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					Step(fmt.Sprintf("Create backup full name %s:%s:%s",
						sourceClusterName, namespace, backupName), func() {
						err = CreateBackupGetErr(backupName,
							sourceClusterName, backupLocationName, BackupLocationUID,
							[]string{namespace}, labelSelectores, OrgID)
						if err != nil {
							//aliveBackup[namespace] = false
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
						}
					})
				}(namespace)
			}
			var wg sync.WaitGroup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Warnf("Skipping waiting for backup %s because %s", backupName, error)
					continue
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, backupName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
						//ctx, err := backup.GetPxCentralAdminCtx()
						ctx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForBackupCompletion(
								ctx,
								backupName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								log.Infof("Backup [%s] completed successfully", backupName)
								atomic.AddInt32(&successfulBackups, 1)
							} else {
								log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
									backupName, err)
								bkpNamespaceErrors[namespace] = err
							}
						}
					})
				}(&wg, namespace, backupName)
			}
			wg.Wait()
			for _, namespace := range bkpNamespaces {
				_, ok := bkpNamespaceErrors[namespace]
				if !ok {
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					successfulBackupNames = append(successfulBackupNames, backupName)
				}
			}
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Infof("Skipping create restore %s because %s", restoreName, error)
					continue
				}
				go func(namespace string) {
					atomic.AddInt32(&numRestores, 1)
					Step(fmt.Sprintf("Create restore full name %s:%s:%s",
						destinationClusterName, namespace, restoreName), func() {
						err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
							destinationClusterName, OrgID)
						if err != nil {
							log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
								restoreName, OrgID, clusterName, err)
							bkpNamespaceErrors[namespace] = err
						}
					})
				}(namespace)
			}
			for _, namespace := range bkpNamespaces {
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Infof("Skipping waiting for restore %s because %s", restoreName, error)
					continue
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, restoreName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						//ctx, err := backup.GetPxCentralAdminCtx()
						ctx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								log.Infof("Restore [%s] completed successfully", restoreName)
								atomic.AddInt32(&successfulRestores, 1)
							} else {
								log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
									restoreName, err)
								bkpNamespaceErrors[namespace] = err
							}
						}
					})
				}(&wg, namespace, restoreName)
			}
			wg.Wait()
			remainingContexts := make([]*scheduler.Context, 0)
			for _, namespace := range bkpNamespaces {
				_, ok := bkpNamespaceErrors[namespace]
				if !ok {
					restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
					successfulRestoreNames = append(successfulRestoreNames, restoreName)
					for _, ctx := range namespaceContextMap[namespace] {
						remainingContexts = append(remainingContexts, ctx)
					}
				}
			}
			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				destClusterConfigPath, err := GetDestinationClusterConfigPath()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

				SetClusterContext(destClusterConfigPath)

				// Populate contexts
				for _, ctx := range remainingContexts {
					ctx.SkipClusterScopedObject = true
					ctx.SkipVolumeValidation = true
				}
				ValidateRestoredApplicationsGetErr(remainingContexts, volumeParams, bkpNamespaceErrors)
			})
			Step("teardown all restored apps", func() {
				for _, ctx := range remainingContexts {
					TearDownContext(ctx, nil)
				}
			})
			for namespace, err := range bkpNamespaceErrors {
				errString := fmt.Sprintf("%s:%d - %s", namespace, counter, err.Error())
				combinedErrors = append(combinedErrors, errString)
			}
		}
		Step("teardown applications on source cluster", func() {
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
			} else {
				SetClusterContext(sourceClusterConfigPath)
				for _, ctx := range contexts {
					TearDownContext(ctx, map[string]bool{
						SkipClusterScopedObjects:                    true,
						scheduler.OptionsWaitForResourceLeakCleanup: true,
						scheduler.OptionsWaitForDestroy:             true,
					})
				}
			}
		})
		Step("teardown backup/restore objects", func() {
			TearDownBackupRestoreSpecific(successfulBackupNames, successfulRestoreNames)
		})
		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			log.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
		Step("view errors", func() {
			log.Infof("There were %d errors during this test", len(combinedErrors))
			if len(combinedErrors) > 0 {
				err = fmt.Errorf(strings.Join(combinedErrors, "\n"))
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

// TODO: There is no delete org API
/*func DeleteOrganization(orgID string) {
	Step(fmt.Sprintf("Delete organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.Delete{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		_, err := backupDriver.Delete(req)
		Expect(err).NotTo(HaveOccurred())
	})
}*/

// createS3BackupLocation creates backup location
func createGkeBackupLocation(name string, cloudCred string, orgID string) {
	Step(fmt.Sprintf("Create GKE backup location [%s] in org [%s]", name, orgID), func() {
		// TODO(stgleb): Implement this
	})
}

// CreateProviderClusterObject creates cluster for each cluster per each cloud provider
func CreateProviderClusterObject(provider string, kubeconfigList []string, cloudCred, orgID string) {
	Step(fmt.Sprintf("Create cluster [%s-%s] in org [%s]",
		clusterName, provider, orgID), func() {
		kubeconfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
		log.FailOnError(err, "Fetching kubeconfig path for source cluster")
		CreateCluster(fmt.Sprintf("%s-%s", clusterName, provider),
			kubeconfigPath, orgID, "", "")
	})
}

func getProviders() []string {
	providersStr := os.Getenv("PROVIDERS")
	return strings.Split(providersStr, ",")
}

func getProviderClusterConfigPath(provider string, kubeconfigs []string) (string, error) {
	log.Infof("Get kubeconfigPath from list %v and provider %s",
		kubeconfigs, provider)
	for _, kubeconfigPath := range kubeconfigs {
		if strings.Contains(provider, kubeconfigPath) {
			fullPath := path.Join(KubeconfigDirectory, kubeconfigPath)
			return fullPath, nil
		}
	}

	return "nil", fmt.Errorf("kubeconfigPath not found for provider %s in kubeconfigPath list %v",
		provider, kubeconfigs)
}

// CreateBackup creates backup
func CreateBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, pre_rule_name string,
	pre_rule_uid string, post_rule_name string, post_rule_uid string) {

	var bkp *api.BackupObject
	var bkp_uid string
	backupDriver := Inst().Backup
	bkpCreateRequest := &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: pre_rule_name,
			Uid:  pre_rule_uid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: post_rule_name,
			Uid:  post_rule_uid,
		},
	}
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err,  "Fetching px-central-admin ctx")

	_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
	log.FailOnError(err, "Taking backup of applications")

	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkp_uid = bkp.Uid
			break
		}
	}
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkp_uid,
		OrgId: orgID,
	}
	time.Sleep(time.Minute * 2)
	resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
	log.FailOnError(err, "Inspecting the backup taken")
	dash.VerifyFatal(resp.GetBackup().GetStatus().Status, api.BackupInfo_StatusInfo_Success, "Inspecting the backup taken")
}

func GetNodesForBackup(backupName string, bkpNamespace string,
	orgID string, clusterName string, triggerOpts *driver_api.TriggerOptions) []node.Node {

	var nodes []node.Node
	backupDriver := Inst().Backup

	backupUID := getBackupUID(orgID, backupName)
	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	err := Inst().Backup.WaitForBackupRunning(context.Background(), backupInspectReq, defaultTimeout, defaultRetryInterval)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to wait for backup [%s] to start. Error: [%v]",
			backupName, err))

	clusterInspectReq := &api.ClusterInspectRequest{
		OrgId:          orgID,
		Name:           clusterName,
		IncludeSecrets: true,
	}
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	clusterInspectRes, err := backupDriver.InspectCluster(ctx, clusterInspectReq)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to inspect cluster [%s] in org [%s]. Error: [%v]",
			clusterName, orgID, err))
	Expect(clusterInspectRes).NotTo(BeNil(),
		"Got an empty response while inspecting cluster [%s] in org [%s]", clusterName, orgID)

	cluster := clusterInspectRes.GetCluster()
	volumeBackupIDs, err := backupDriver.GetVolumeBackupIDs(context.Background(),
		backupName, bkpNamespace, cluster, orgID)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get volume backup IDs for backup [%s] in org [%s]. Error: [%v]",
			backupName, orgID, err))
	Expect(len(volumeBackupIDs)).NotTo(Equal(0),
		"Got empty list of volumeBackup IDs from backup driver")

	for _, backupID := range volumeBackupIDs {
		n, err := Inst().V.GetNodeForBackup(backupID)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get node on which backup [%s] in running. Error: [%v]",
				backupName, err))

		log.Debugf("Volume backup [%s] is running on node [%s], node id: [%s]\n",
			backupID, n.GetHostname(), n.GetId())
		nodes = append(nodes, n)
	}
	return nodes
}

// CreateRestore creates restore
func CreateRestore(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string) {

	var bkp *api.BackupObject
	var bkp_uid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	curBackups, _ := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkp_uid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:           backupName,
		Cluster:          clusterName,
		NamespaceMapping: namespaceMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkp_uid,
		},
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
	log.FailOnError(err, "Creating restore")
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}
	time.Sleep(time.Minute * 3)
	resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
	log.FailOnError(err, "Verifying restore")
	dash.VerifyFatal(resp.GetRestore().GetStatus().Status, api.RestoreInfo_StatusInfo_PartialSuccess, "Verifying restore")
}

// TearDownBackupRestoreSpecific deletes backups and restores specified by name as well as backup location
func TearDownBackupRestoreSpecific(backups []string, restores []string) {
	for _, backupName := range backups {
		backupUID := getBackupUID(OrgID, backupName)
		DeleteBackup(backupName, backupUID, OrgID)
	}
	for _, restoreName := range restores {
		DeleteRestore(restoreName, OrgID)
	}
	provider := GetProvider()
	DeleteCluster(destinationClusterName, OrgID)
	DeleteCluster(sourceClusterName, OrgID)
	DeleteBackupLocation(backupLocationName, OrgID)
	DeleteCloudCredential(CredName, OrgID, CloudCredUID)
	DeleteBucket(provider, BucketName)
}

// CreateRestoreGetErr creates restore
func CreateRestoreGetErr(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string) (err error) {

	Step(fmt.Sprintf("Create restore [%s] in org [%s] on cluster [%s]",
		restoreName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		createRestoreReq := &api.RestoreCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  restoreName,
				OrgId: orgID,
			},
			Backup:           backupName,
			Cluster:          clusterName,
			NamespaceMapping: namespaceMapping,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
		if err != nil {
			log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
				restoreName, orgID, clusterName, err)
		}

		// TODO: validate createClusterResponse also
	})
	return err
}

func getBackupUID(orgID, backupName string) string {
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	backupUID, err := backupDriver.GetBackupUID(ctx, orgID, backupName)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get backup uid for org %s backup %s ctx: [%v]",
			orgID, backupName, err))
	return backupUID
}
