package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
)

const (
	backupLocationName     = "tp-blocation"
	clusterName            = "tp-cluster"
	credName               = "tp-backup-cred"
	backupNamePrefix       = "tp-backup"
	restoreNamePrefix      = "tp-restore"
	bucketNamePrefix       = "tp-backup-bucket"
	sourceClusterName      = "source-cluster"
	destinationClusterName = "destination-cluster"

	backupRestoreCompletionTimeoutMin = 20
	retrySeconds                      = 30

	storkDeploymentName      = "stork"
	storkDeploymentNamespace = "kube-system"

	defaultTimeout       = 5 * time.Minute
	defaultRetryInterval = 5 * time.Second

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
var _ = Describe("{BackupKillStork}", func() {
	var (
		contexts         []*scheduler.Context
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backupcreaterestore"
	)

	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	provider, err := getProvider()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get provider. Error: [%v]", err))

	It("has to kill stork while backup is in progress", func() {
	Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			err := setClusterContext("")
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
			err = setupBackup(provider, taskNamePrefix, orgID, credName,
				backupLocationName, bucketName)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed setup backup. Error: [%v]", err))
		})

		sourceClusterConfigPath, err := getSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		err = setClusterContext(sourceClusterConfigPath)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().ScaleFactor; i++ {
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
			backupName := fmt.Sprintf("%s-%s", backupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				sourceClusterName, namespace, backupName), func() {
				err = createBackup(backupName,
					sourceClusterName, backupLocationName,
					[]string{namespace}, labelSelectores, orgID)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed create backup %s Error: [%v]", backupName, err))
			})
		}

		Step("Kill stork during backup", func() {
			// setup task to delete stork pods as soon as it starts doing backup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", backupNamePrefix, namespace)
				req := &api.BackupInspectRequest{
					Name:  backupName,
					OrgId: orgID,
				}

				logrus.Infof("backup %s wait for running", backupName)
				err := Inst().Backup.WaitForBackupRunning(context.Background(),
					req, backupRestoreCompletionTimeoutMin*time.Minute,
					retrySeconds*time.Second)

				if err != nil {
					logrus.Warnf("backup %s wait for running err %v",
						backupName, err)
					continue
				} else {
					break
				}
			}
			err = killStork(storkDeploymentName, storkDeploymentName)
			Expect(err).NotTo(HaveOccurred())
		})

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", backupNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
				err := Inst().Backup.WaitForBackupCompletion(
					context.Background(),
					backupName, orgID,
					backupRestoreCompletionTimeoutMin*time.Minute,
					retrySeconds*time.Second)
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

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", backupNamePrefix, namespace)
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Create restore %s:%s:%s from backup %s:%s:%s",
				destinationClusterName, namespace, restoreName,
				sourceClusterName, namespace, backupName), func() {
				err = createRestore(restoreName, backupName, namespaceMapping,
					destinationClusterName, orgID)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to create restore [%s]. Error: [%v]",
						restoreName, err))
			})
		}

		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for restore %s:%s to complete",
				namespace, restoreName), func() {

				err := Inst().Backup.WaitForRestoreCompletion(context.Background(), restoreName, orgID,
					backupRestoreCompletionTimeoutMin*time.Minute,
					retrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
						restoreName, err))
			})
		}

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step("Validate Restored applications", func() {
			destClusterConfigPath, err := getDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			err = setClusterContext(destClusterConfigPath)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed switch cluster context Error: [%v]", err))

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
			err = tearDownBackupRestore(contexts, provider, taskNamePrefix)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to teardown backup and restore. Error: [%v]", err))
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
	providers := getProviders()
	pathMap := getProviderClusterConfigPath(providers, kubeconfigList)

	It("has to connect and check the backup setup", func() {
		Step("Setup backup", func() {
			kubeconfigs = os.Getenv("KUBECONFIGS")
			Expect(kubeconfigs).NotTo(BeEmpty(),
				fmt.Sprintf("KUBECONFIGS %s must not be empty", kubeconfigs))
			kubeconfigList = strings.Split(kubeconfigs, ",")
			// Validate user has provided at least 1 kubeconfig for cluster
			Expect(kubeconfigList).NotTo(BeEmpty(),
				fmt.Sprintf("kubeconfigList %v must have at least one", kubeconfigList))
			// Set cluster context to cluster where torpedo is running
			err := setClusterContext("")
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
			err = dumpKubeconfigList(kubeconfigList)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to dump configs. Error: [%v]", err))
			for _, provider := range providers {
				logrus.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
					provider, Inst().InstanceID)
				bucketName := fmt.Sprintf("%s-%s-%s", bucketNamePrefix, provider, Inst().InstanceID)
				credName := fmt.Sprintf("%s-%s", credName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				err := setupBackup(provider, taskNamePrefix, orgID, credName, backupLocation, bucketName)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to setup backup. Error: [%v]", err))
			}
		})
		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(Inst().MinRunTimeMins) * time.Minute)

		for time.Now().Before(end) {
			Step("Deploy applications", func() {
				for _, provider := range providers {
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))

					providerContexts := make([]*scheduler.Context, 0)
					providerNamespaces := make([]string, 0)

					// Rescan specs for each provider to reload provider specific specs
					logrus.Infof("Rescan specs for provider %s", provider)
					err = Inst().S.RescanSpecs(Inst().SpecDir, provider)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to rescan specs from %s for storage provider %s. Error: [%v]",
							Inst().SpecDir, provider, err))

					logrus.Infof("Start deploy applications for provider %s", provider)
					for i := 0; i < Inst().ScaleFactor; i++ {
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
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
					// In case of non-portworx volume provider skip volume validation until
					// other volume providers are implemented.
					for _, ctx := range contexts[provider] {
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = backupRestoreCompletionTimeoutMin * time.Minute
					}
					ValidateApplications(contexts[provider])
				}
			})

			logrus.Info("Wait for IO to proceed\n")
			time.Sleep(time.Minute * 5)

			// Perform all backup operations concurrently
			// TODO(stgleb): Add multi-namespace backup when ready in px-backup
			for _, provider := range providers {
				providerClusterConfigPath := pathMap[provider]
				err := setClusterContext(providerClusterConfigPath)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
				ctx, _ := context.WithTimeout(context.Background(),
					backupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", backupNamePrefix, provider,
							namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						Step(fmt.Sprintf("Create backup full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							err := createBackup(backupName, clusterName, backupLocation,
								[]string{namespace}, labelSelectores, orgID)
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
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
					logrus.Infof("Kill stork during backup for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						backupName := fmt.Sprintf("%s-%s-%s", backupNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						req := &api.BackupInspectRequest{
							Name:  backupName,
							OrgId: orgID,
						}

						logrus.Infof("backup %s wait for running", backupName)
						err := Inst().Backup.WaitForBackupRunning(context.Background(),
							req, backupRestoreCompletionTimeoutMin*time.Minute,
							retrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					err = killStork(storkDeploymentName, storkDeploymentNamespace)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to kill stork for provider %s . Error: [%v]",
							provider, err))
				}
			})

			// wait until all backups are completed, there is no need to parallel here
			for provider, namespaces := range bkpNamespaces {
				providerClusterConfigPath := pathMap[provider]
				err := setClusterContext(providerClusterConfigPath)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
				for _, namespace := range namespaces {
					backupName := fmt.Sprintf("%s-%s-%s", backupNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for backup %s to complete in organization %s",
						backupName, orgID), func() {
						err := Inst().Backup.WaitForBackupCompletion(
							context.Background(),
							backupName, orgID,
							backupRestoreCompletionTimeoutMin*time.Minute,
							retrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
								backupName, err))
					})
				}
			}

			Step("teardown all applications on source cluster before switching context to destination cluster", func() {
				for _, provider := range providers {
					providerClusterConfigPath := pathMap[provider]
					logrus.Infof("Set config to %s", providerClusterConfigPath)
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
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
				providerClusterConfigPath := pathMap[provider]
				err := setClusterContext(providerClusterConfigPath)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
				ctx, _ := context.WithTimeout(context.Background(),
					backupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", backupNamePrefix, provider, namespace)
						restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						Step(fmt.Sprintf("Create restore full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							err := createRestore(restoreName, backupName, namespaceMapping, clusterName, orgID)
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
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
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
							req, backupRestoreCompletionTimeoutMin*time.Minute,
							retrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred())
					}
					err = killStork(storkDeploymentName, storkDeploymentNamespace)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to kill stork for provider %s . Error: [%v]",
							provider, err))
				}
			})

			for provider, providerNamespaces := range bkpNamespaces {
				providerClusterConfigPath := pathMap[provider]
				err := setClusterContext(providerClusterConfigPath)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
				for _, namespace := range providerNamespaces {
					restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						err := Inst().Backup.WaitForRestoreCompletion(context.Background(),
							restoreName, orgID,
							backupRestoreCompletionTimeoutMin*time.Minute,
							retrySeconds*time.Second)
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
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
					for _, ctx := range contexts[provider] {
						ctx.SkipClusterScopedObject = true
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = backupRestoreCompletionTimeoutMin * time.Minute

						err := Inst().S.WaitForRunning(ctx, defaultTimeout, defaultRetryInterval)
						Expect(err).NotTo(HaveOccurred())
					}
					ValidateApplications(contexts[provider])
				}
			})
			Step("teardown all restored apps", func() {
				for _, provider := range providers {
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
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
					providerClusterConfigPath := pathMap[provider]
					err := setClusterContext(providerClusterConfigPath)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
					ctx, _ := context.WithTimeout(context.Background(),
						backupRestoreCompletionTimeoutMin*time.Minute)
					errChan := make(chan error)

					for _, namespace := range providerNamespaces {
						go func(provider, namespace string) {
							clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
							backupName := fmt.Sprintf("%s-%s-%s", backupNamePrefix, provider, namespace)
							orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
								provider, Inst().InstanceID)
							Step(fmt.Sprintf("Delete backup full name %s:%s:%s",
								clusterName, namespace, backupName), func() {
								err := deleteBackup(backupName, orgID)
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
								err := deleteRestore(restoreName, orgID)
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
				providerClusterConfigPath := pathMap[provider]
				err := setClusterContext(providerClusterConfigPath)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
				logrus.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix), provider, Inst().InstanceID)
				bucketName := fmt.Sprintf("%s-%s-%s", bucketNamePrefix, provider, Inst().InstanceID)
				credName := fmt.Sprintf("%s-%s", credName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
				err = deleteCluster(clusterName, orgID)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to delete cluster %s provider %s . Error: [%v]",
						clusterName, provider, err))
				err = deleteBackupLocation(backupLocation, orgID)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to backup location %s provider %s . Error: [%v]",
						backupLocation, provider, err))
				err = deleteCloudCredential(credName, orgID)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to delete cloud credential %s provider %s . Error: [%v]",
						credName, provider, err))
				err = deleteBucket(provider, bucketName)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to delete bucket %s provider %s . Error: [%v]",
						bucketName, provider, err))
			}
		})
	})
})

// This test crashes volume driver (PX) while backup is in progress
var _ = Describe("{BackupCrashVolDriver}", func() {
	var contexts []*scheduler.Context
	var namespaceMapping map[string]string
	taskNamePrefix := "backupcrashvoldriver"
	labelSelectores := make(map[string]string)
	provider, err := getProvider()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failedto get provider. Error: [%v]", err))
	volumeParams := make(map[string]map[string]string)

	It("has to complete backup and restore", func() {
		// Set cluster context to cluster where torpedo is running
		err = setClusterContext("")
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
		err = setupBackup(provider, taskNamePrefix, orgID, credName, backupLocationName, bucketName)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to setup backup. Error: [%v]", err))
		sourceClusterConfigPath, err := getSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))
		err = setClusterContext(sourceClusterConfigPath)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
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

		for _, ctx := range contexts {
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				bkpNamespace := GetAppNamespace(ctx, taskName)
				BackupName := fmt.Sprintf("%s-%s", backupNamePrefix, bkpNamespace)

				Step(fmt.Sprintf("Create Backup [%s]", BackupName), func() {
					err = createBackup(BackupName, sourceClusterName, backupLocationName,
						[]string{bkpNamespace}, labelSelectores, orgID)
					Expect(err).NotTo(HaveOccurred())
				})

				triggerFn := func() (bool, error) {
					backupInspectReq := &api.BackupInspectRequest{
						Name:  BackupName,
						OrgId: orgID,
					}
					err := Inst().Backup.WaitForBackupRunning(context.Background(), backupInspectReq, defaultTimeout, defaultRetryInterval)
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

				bkpNode, err := getNodesForBackup(BackupName, bkpNamespace,
					orgID, sourceClusterName, triggerOpts)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(bkpNode)).NotTo(Equal(0),
					fmt.Sprintf("Did not found any node on which backup [%v] is running.",
						BackupName))

				Step(fmt.Sprintf("Kill volume driver %s on node [%v] after backup [%s] starts",
					Inst().V.String(), bkpNode[0].Name, BackupName), func() {
					// Just kill storage driver on one of the node where volume backup is in progress
					Inst().V.StopDriver(bkpNode[0:1], true, triggerOpts)
				})

				Step(fmt.Sprintf("Wait for Backup [%s] to complete", BackupName), func() {
					err := Inst().Backup.WaitForBackupCompletion(context.Background(), BackupName, orgID,
						backupRestoreCompletionTimeoutMin*time.Minute,
						retrySeconds*time.Second)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
							BackupName, err))
				})
			}
		}

		for _, ctx := range contexts {
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				bkpNamespace := GetAppNamespace(ctx, taskName)
				BackupName := fmt.Sprintf("%s-%s", backupNamePrefix, bkpNamespace)
				RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, bkpNamespace)
				Step(fmt.Sprintf("Create Restore [%s]", RestoreName), func() {
					err = createRestore(RestoreName, BackupName,
						namespaceMapping, destinationClusterName, orgID)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to create restore [%s] to complete. Error: [%v]",
							RestoreName, err))
				})

				Step(fmt.Sprintf("Wait for Restore [%s] to complete", RestoreName), func() {
					err := Inst().Backup.WaitForRestoreCompletion(context.Background(), RestoreName, orgID,
						backupRestoreCompletionTimeoutMin*time.Minute,
						retrySeconds*time.Second)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
							RestoreName, err))
				})
			}
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
			destClusterConfigPath, err := getDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			err = setClusterContext(destClusterConfigPath)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed switch cluster context Error: [%v]", err))
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
			err = tearDownBackupRestore(contexts, provider, taskNamePrefix)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to teardown provider [%s] to complete. Error: [%v]",
					provider, err))
		})
	})
})
