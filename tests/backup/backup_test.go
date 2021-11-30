package tests

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	clusterName            = "tp-cluster"
	restoreNamePrefix      = "tp-restore"
	configMapName          = "kubeconfigs"
	defaultTimeout         = 5 * time.Minute
	defaultRetryInterval   = 10 * time.Second
	sourceClusterName      = "source-cluster"
	destinationClusterName = "destination-cluster"
	backupLocationName     = "tp-blocation"

	storkDeploymentName      = "stork"
	storkDeploymentNamespace = "kube-system"

	appReadinessTimeout = 10 * time.Minute
)

var (
	orgID      string
	bucketName string
)

var _ = BeforeSuite(func() {
	logrus.Infof("Init instance")
	InitInstance()
})

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Backup", specReporters)
}

func TearDownBackupRestore(bkpNamespaces []string, restoreNamespaces []string) {
	for _, bkpNamespace := range bkpNamespaces {
		BackupName := fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespace)
		DeleteBackup(BackupName, OrgID)
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

var _ = AfterSuite(func() {
	//PerformSystemCheck()
	//ValidateCleanup()
	//	BackupCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}

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
				logrus.Infof("Task name %s\n", taskName)
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

		logrus.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 5)

		// TODO(stgleb): Add multi-namespace backup when ready in px-backup
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				sourceClusterName, namespace, backupName), func() {
				CreateBackup(backupName,
					sourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectores, orgID)
			})
		}

		Step("Kill stork during backup", func() {
			// setup task to delete stork pods as soon as it starts doing backup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				req := &api.BackupInspectRequest{
					Name:  backupName,
					OrgId: orgID,
				}

				logrus.Infof("backup %s wait for running", backupName)
				err := Inst().Backup.WaitForBackupRunning(context.Background(),
					req, BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)

				if err != nil {
					logrus.Warnf("backup %s wait for running err %v",
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
			logrus.Infof("Execute task for killing stork")
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
				logrus.Infof("Run Setup backup with object store provider: %s", provider)
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
					logrus.Infof("Set context to %s", providerClusterConfigPath)
					SetClusterContext(providerClusterConfigPath)

					providerContexts := make([]*scheduler.Context, 0)
					providerNamespaces := make([]string, 0)

					// Rescan specs for each provider to reload provider specific specs
					logrus.Infof("Rescan specs for provider %s", provider)
					err = Inst().S.RescanSpecs(Inst().SpecDir, provider)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to rescan specs from %s for storage provider %s. Error: [%v]",
							Inst().SpecDir, provider, err))

					logrus.Infof("Start deploy applications for provider %s", provider)
					for i := 0; i < Inst().GlobalScaleFactor; i++ {
						taskName := fmt.Sprintf("%s-%s-%d", taskNamePrefix, provider, i)
						logrus.Infof("Task name %s\n", taskName)
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

					logrus.Infof("validate applications for provider %s", provider)
					ValidateApplications(contexts[provider])
				}
			})

			logrus.Info("Wait for IO to proceed\n")
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

					logrus.Infof("Kill stork during backup for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						req := &api.BackupInspectRequest{
							Name:  backupName,
							OrgId: orgID,
						}

						logrus.Infof("backup %s wait for running", backupName)
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
					logrus.Infof("Set config to %s", providerClusterConfigPath)
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

					logrus.Infof("Kill stork during restore for provider %s", provider)
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

						logrus.Infof("restore %s wait for running", restoreName)
						err := Inst().Backup.WaitForRestoreRunning(context.Background(),
							req, BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					logrus.Infof("Kill stork task")
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
					logrus.Infof("teardown backup and restore objects for provider %s", provider)
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
								bkpDeleteRequest := &api.BackupDeleteRequest{
									Name:  backupName,
									OrgId: orgID,
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

								logrus.Infof("Wait for restore %s is deleted", restoreName)
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

				logrus.Infof("Run Setup backup with object store provider: %s", provider)
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
	logrus.Infof("Execute task for killing stork")
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
					[]string{bkpNamespace}, labelSelectors, OrgID)
			})

			triggerFn := func() (bool, error) {
				backupInspectReq := &api.BackupInspectRequest{
					Name:  BackupName,
					OrgId: OrgID,
				}
				//ctx, err := backup.GetPxCentralAdminCtx()
				ctx, err := backup.GetAdminCtxFromSecret()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
						err))
				err = Inst().Backup.WaitForBackupRunning(ctx, backupInspectReq, defaultTimeout, defaultRetryInterval)
				if err != nil {
					logrus.Warnf("[TriggerCheck]: Got error while checking if backup [%s] has started.\n Error : [%v]\n",
						BackupName, err)
					return false, err
				}
				logrus.Infof("[TriggerCheck]: backup [%s] has started.\n",
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
				logrus.Infof("Task name %s\n", taskName)
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
				logrus.Warningf("Skipping waiting for backup %s because %s", backupName, error)
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
							logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
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
				logrus.Infof("Skipping create restore %s because %s", restoreName, error)
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
				logrus.Infof("Skipping waiting for restore %s because %s", restoreName, error)
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
							logrus.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
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
			logrus.Infof("%d/%d backups succeeded.", successfulBackups, len(bkpNamespaces))
			logrus.Infof("%d/%d restores succeeded.", successfulRestores, successfulBackups)
		})

		Step("view errors", func() {
			logrus.Infof("There were %d errors during this test", len(bkpNamespaceErrors))

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
	It("has to connect and check the backup setup", func() {
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)

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
				logrus.Infof("Task name %s\n", taskName)
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
		logrus.Info("Wait for IO to proceed\n")
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
				logrus.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
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
						logrus.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
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
						logrus.Infof("Backup [%s] completed successfully", backupName)
						successfulBackups++
					} else {
						logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
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
						logrus.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
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
						logrus.Infof("Restore [%s] completed successfully", restoreName)
						successfulRestores++
					} else {
						logrus.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
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
					logrus.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
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
			logrus.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			logrus.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
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
				logrus.Infof("Task name %s\n", taskName)
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
		logrus.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 2)

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(5) * time.Minute)
		counter := 0
		for time.Now().Before(end) {
			counter++
			bkpNamespaceErrors := make(map[string]error)
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				logrus.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
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
							logrus.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
						}
					})
				}(namespace)
			}
			var wg sync.WaitGroup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					logrus.Warningf("Skipping waiting for backup %s because %s", backupName, error)
					continue
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, backupName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
						//ctx, err := backup.GetPxCentralAdminCtx()
						ctx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForBackupCompletion(
								ctx,
								backupName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								logrus.Infof("Backup [%s] completed successfully", backupName)
								atomic.AddInt32(&successfulBackups, 1)
							} else {
								logrus.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
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
					logrus.Infof("Skipping create restore %s because %s", restoreName, error)
					continue
				}
				go func(namespace string) {
					atomic.AddInt32(&numRestores, 1)
					Step(fmt.Sprintf("Create restore full name %s:%s:%s",
						destinationClusterName, namespace, restoreName), func() {
						err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
							destinationClusterName, OrgID)
						if err != nil {
							logrus.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
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
					logrus.Infof("Skipping waiting for restore %s because %s", restoreName, error)
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
							logrus.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								logrus.Infof("Restore [%s] completed successfully", restoreName)
								atomic.AddInt32(&successfulRestores, 1)
							} else {
								logrus.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
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
				logrus.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
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
			logrus.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			logrus.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
		Step("view errors", func() {
			logrus.Infof("There were %d errors during this test", len(combinedErrors))
			if len(combinedErrors) > 0 {
				err = fmt.Errorf(strings.Join(combinedErrors, "\n"))
				Expect(err).NotTo(HaveOccurred())
			}
		})
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
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))
		CreateCluster(fmt.Sprintf("%s-%s", clusterName, provider), cloudCred,
			kubeconfigPath, orgID)
	})
}

func getProviders() []string {
	providersStr := os.Getenv("PROVIDERS")
	return strings.Split(providersStr, ",")
}

func getProviderClusterConfigPath(provider string, kubeconfigs []string) (string, error) {
	logrus.Infof("Get kubeconfigPath from list %v and provider %s",
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
	namespaces []string, labelSelectors map[string]string, orgID string) {

	Step(fmt.Sprintf("Create backup [%s] in org [%s] from cluster [%s]",
		backupName, orgID, clusterName), func() {

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
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create backup [%s] in org [%s]. Error: [%v]",
				backupName, orgID, err))
	})
}

func GetNodesForBackup(backupName string, bkpNamespace string,
	orgID string, clusterName string, triggerOpts *driver_api.TriggerOptions) []node.Node {

	var nodes []node.Node
	backupDriver := Inst().Backup

	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
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

		logrus.Debugf("Volume backup [%s] is running on node [%s], node id: [%s]\n",
			backupID, n.GetHostname(), n.GetId())
		nodes = append(nodes, n)
	}
	return nodes
}

// CreateRestore creates restore
func CreateRestore(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string) {

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
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
				restoreName, orgID, clusterName, err))
		// TODO: validate createClusterResponse also
	})
}

//TearDownBackupRestoreSpecific deletes backups and restores specified by name as well as backup location
func TearDownBackupRestoreSpecific(backups []string, restores []string) {
	for _, backupName := range backups {
		DeleteBackup(backupName, OrgID)
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
			logrus.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
				restoreName, orgID, clusterName, err)
		}

		// TODO: validate createClusterResponse also
	})
	return err
}
