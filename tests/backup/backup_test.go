package tests

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	backupLocationName     = "tp-blocation"
	clusterName            = "tp-cluster"
	credName               = "tp-backup-cred"
	backupNamePrefix       = "tp-backup"
	restoreNamePrefix      = "tp-restore"
	bucketNamePrefix       = "tp-backup-bucket"
	configMapName          = "kubeconfigs"
	kubeconfigDirectory    = "/tmp"
	sourceClusterName      = "source-cluster"
	sestinationClusterName = "destination-cluster"

	backupRestoreCompletionTimeoutMin = 20
	retrySeconds                      = 10

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

// getProvider validates and return object store provider
func getProvider() string {
	provider, ok := os.LookupEnv("OBJECT_STORE_PROVIDER")
	Expect(ok).To(BeTrue(), fmt.Sprintf("No environment variable 'PROVIDER' supplied. Valid values are: %s, %s, %s",
		drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke))
	switch provider {
	case drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke:
	default:
		Fail(fmt.Sprintf("Valid values for 'PROVIDER' environment variables are: %s, %s, %s",
			drivers.ProviderAws, drivers.ProviderAzure, drivers.ProviderGke))
	}
	return provider
}

func SetupBackup(testName string) {
	logrus.Infof("Backup driver: %v", Inst().Backup)
	provider := getProvider()
	logrus.Infof("Run Setup backup with object store provider: %s", provider)
	orgID = fmt.Sprintf("%s-%s", strings.ToLower(testName), Inst().InstanceID)
	bucketName = fmt.Sprintf("%s-%s", bucketNamePrefix, Inst().InstanceID)

	CreateBucket(provider, bucketName)
	CreateOrganization(orgID)
	CreateCloudCredential(provider, credName, orgID)
	CreateBackupLocation(provider, backupLocationName, credName, bucketName, orgID)
	CreateSourceAndDestClusters(credName, orgID)
}

func TearDownBackupRestore(contexts []*scheduler.Context, taskNamePrefix string) {
	for _, ctx := range contexts {
		for i := 0; i < Inst().ScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			bkpNamespace := GetAppNamespace(ctx, taskName)
			BackupName := fmt.Sprintf("%s-%s", backupNamePrefix, bkpNamespace)
			RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, bkpNamespace)
			DeleteBackup(BackupName, orgID)
			DeleteRestore(RestoreName, orgID)
		}
	}
	provider := getProvider()
	DeleteCluster(sestinationClusterName, orgID)
	DeleteCluster(sourceClusterName, orgID)
	DeleteBackupLocation(backupLocationName, orgID)
	DeleteCloudCredential(credName, orgID)
	DeleteBucket(provider, bucketName)
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

func SetClusterContext(clusterConfigPath string) {
	err := Inst().S.SetConfig(clusterConfigPath)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to switch to context. Error: [%v]", err))

	err = Inst().S.RefreshNodeRegistry()
	Expect(err).NotTo(HaveOccurred())

	err = Inst().V.RefreshDriverEndpoints()
	Expect(err).NotTo(HaveOccurred())
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

		sourceClusterConfigPath, err := getSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

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
				CreateBackup(backupName,
					sourceClusterName, backupLocationName,
					[]string{namespace}, labelSelectores, orgID)
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
				sestinationClusterName, namespace, restoreName,
				sourceClusterName, namespace, backupName), func() {
				CreateRestore(restoreName, backupName, namespaceMapping,
					sestinationClusterName, orgID)
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
			TearDownBackupRestore(contexts, taskNamePrefix)
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
				bucketName = fmt.Sprintf("%s-%s-%s", bucketNamePrefix, provider, Inst().InstanceID)
				credName := fmt.Sprintf("%s-%s", credName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)

				CreateBucket(provider, bucketName)
				CreateOrganization(orgID)
				CreateCloudCredential(provider, credName, orgID)
				CreateBackupLocation(provider, backupLocation, credName, bucketName, orgID)
				CreateProviderClusterObject(provider, kubeconfigList, credName, orgID)
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
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					// In case of non-portworx volume provider skip volume validation until
					// other volume providers are implemented.
					for _, ctx := range contexts[provider] {
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = backupRestoreCompletionTimeoutMin * time.Minute
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
							_, err = backupDriver.CreateBackup(bkpCreateRequest)
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
							_, err := backupDriver.CreateRestore(createRestoreReq)

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
							req, backupRestoreCompletionTimeoutMin*time.Minute,
							retrySeconds*time.Second)

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
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

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
								backupDriver := Inst().Backup
								bkpDeleteRequest := &api.BackupDeleteRequest{
									Name:  backupName,
									OrgId: orgID,
								}
								_, err := backupDriver.DeleteBackup(bkpDeleteRequest)

								ctx, _ := context.WithTimeout(context.Background(),
									backupRestoreCompletionTimeoutMin*time.Minute)

								if err := backupDriver.WaitForBackupDeletion(ctx, backupName, orgID,
									backupRestoreCompletionTimeoutMin*time.Minute,
									retrySeconds*time.Second); err != nil {
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
								_, err := backupDriver.DeleteRestore(deleteRestoreReq)

								ctx, _ := context.WithTimeout(context.Background(),
									backupRestoreCompletionTimeoutMin*time.Minute)

								logrus.Infof("Wait for restore %s is deleted", restoreName)
								if err := backupDriver.WaitForRestoreDeletion(ctx, restoreName, orgID,
									backupRestoreCompletionTimeoutMin*time.Minute,
									retrySeconds*time.Second); err != nil {
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
				bucketName := fmt.Sprintf("%s-%s-%s", bucketNamePrefix, provider, Inst().InstanceID)
				credName := fmt.Sprintf("%s-%s", credName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				clusterName := fmt.Sprintf("%s-%s", clusterName, provider)

				DeleteCluster(clusterName, orgID)
				DeleteBackupLocation(backupLocation, orgID)
				DeleteCloudCredential(credName, orgID)
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
	labelSelectores := make(map[string]string)
	volumeParams := make(map[string]map[string]string)

	It("has to complete backup and restore", func() {
		// Set cluster context to cluster where torpedo is running
		SetClusterContext("")
		SetupBackup(taskNamePrefix)

		sourceClusterConfigPath, err := getSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

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
					CreateBackup(BackupName, sourceClusterName, backupLocationName,
						[]string{bkpNamespace}, labelSelectores, orgID)
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

				bkpNode := GetNodesForBackup(BackupName, bkpNamespace,
					orgID, sourceClusterName, triggerOpts)
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
					CreateRestore(RestoreName, BackupName,
						namespaceMapping, sestinationClusterName, orgID)
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
			TearDownBackupRestore(contexts, taskNamePrefix)
		})
	})
})

// CreateOrganization creates org on px-backup
func CreateOrganization(orgID string) {
	Step(fmt.Sprintf("Create organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.OrganizationCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		_, err := backupDriver.CreateOrganization(req)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create organization [%s]. Error: [%v]",
				orgID, err))
	})
}

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

// DeleteCloudCredential deletes cloud credentials
func DeleteCloudCredential(name string, orgID string) {
	Step(fmt.Sprintf("Delete cloud credential [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup

		credDeleteRequest := &api.CloudCredentialDeleteRequest{
			Name:  name,
			OrgId: orgID,
		}
		backupDriver.DeleteCloudCredential(credDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		// Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
	})

}

func CreateBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Create bucket [%s]", bucketName), func() {
		switch provider {
		case drivers.ProviderAws:
			CreateS3Bucket(bucketName)
		case drivers.ProviderAzure:
			CreateAzureBucket(bucketName)
		}
	})
}

func CreateS3Bucket(bucketName string) {
	id, secret, endpoint, s3Region, disableSSLBool := getAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get S3 session to create bucket. Error: [%v]", err))

	S3Client := s3.New(sess)

	_, err = S3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create bucket [%v]. Error: [%v]", bucketName, err))

	err = S3Client.WaitUntilBucketExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to wait for bucket [%v] to get created. Error: [%v]", bucketName, err))
}

func CreateAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := getAzureCredsFromEnv()

	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Create container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create shared key credential [%v]", err))

	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context.Background() // This example uses a never-expiring context

	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)

	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create container. Error: [%v]", err))
}

func DeleteBucket(provider string, bucketName string) {
	Step(fmt.Sprintf("Delete bucket [%s]", bucketName), func() {
		switch provider {
		// TODO(stgleb): PTX-2359 Add DeleteAzureBucket
		case drivers.ProviderAws:
			DeleteS3Bucket(bucketName)
		case drivers.ProviderAzure:
			DeleteAzureBucket(bucketName)
		}
	})
}

func DeleteS3Bucket(bucketName string) {
	id, secret, endpoint, s3Region, disableSSLBool := getAWSDetailsFromEnv()
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(id, secret, ""),
		Region:           aws.String(s3Region),
		DisableSSL:       aws.Bool(disableSSLBool),
		S3ForcePathStyle: aws.Bool(true),
	},
	)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get S3 session to create bucket. Error: [%v]", err))

	S3Client := s3.New(sess)

	iter := s3manager.NewDeleteListIterator(S3Client, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	err = s3manager.NewBatchDeleteWithClient(S3Client).Delete(aws.BackgroundContext(), iter)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Unable to delete objects from bucket %q, %v", bucketName, err))

	_, err = S3Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to delete bucket [%v]. Error: [%v]", bucketName, err))
}

func DeleteAzureBucket(bucketName string) {
	// From the Azure portal, get your Storage account blob service URL endpoint.
	_, _, _, _, accountName, accountKey := getAzureCredsFromEnv()

	urlStr := fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, bucketName)
	logrus.Infof("Delete container url %s", urlStr)
	// Create a ContainerURL object that wraps a soon-to-be-created container's URL and a default pipeline.
	u, _ := url.Parse(urlStr)
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create shared key credential [%v]", err))

	containerURL := azblob.NewContainerURL(*u, azblob.NewPipeline(credential, azblob.PipelineOptions{}))
	ctx := context.Background() // This example uses a never-expiring context

	_, err = containerURL.Delete(ctx, azblob.ContainerAccessConditions{})

	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to delete container. Error: [%v]", err))
}

// CreateCloudCredential creates cloud credetials
func CreateCloudCredential(provider, name string, orgID string) {
	Step(fmt.Sprintf("Create cloud credential [%s] in org [%s]", name, orgID), func() {
		logrus.Printf("Create credential name %s for org %s provider %s", name, orgID, provider)
		backupDriver := Inst().Backup
		switch provider {
		case drivers.ProviderAws:
			log.Printf("Create creds for aws")
			id := os.Getenv("AWS_ACCESS_KEY_ID")
			Expect(id).NotTo(Equal(""),
				"AWS_ACCESS_KEY_ID Environment variable should not be empty")

			secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
			Expect(secret).NotTo(Equal(""),
				"AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

			credCreateRequest := &api.CloudCredentialCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  name,
					OrgId: orgID,
				},
				CloudCredential: &api.CloudCredentialInfo{
					Type: api.CloudCredentialInfo_AWS,
					Config: &api.CloudCredentialInfo_AwsConfig{
						AwsConfig: &api.AWSConfig{
							AccessKey: id,
							SecretKey: secret,
						},
					},
				},
			}
			_, err := backupDriver.CreateCloudCredential(credCreateRequest)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
		case drivers.ProviderAzure:
			logrus.Infof("Create creds for azure")
			tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey := getAzureCredsFromEnv()
			credCreateRequest := &api.CloudCredentialCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  name,
					OrgId: orgID,
				},
				CloudCredential: &api.CloudCredentialInfo{
					Type: api.CloudCredentialInfo_Azure,
					Config: &api.CloudCredentialInfo_AzureConfig{
						AzureConfig: &api.AzureConfig{
							TenantId:       tenantID,
							ClientId:       clientID,
							ClientSecret:   clientSecret,
							AccountName:    accountName,
							AccountKey:     accountKey,
							SubscriptionId: subscriptionID,
						},
					},
				},
			}
			_, err := backupDriver.CreateCloudCredential(credCreateRequest)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
			// TODO: validate CreateCloudCredentialResponse also
		}
	})
}

func getAWSDetailsFromEnv() (id string, secret string, endpoint string,
	s3Region string, disableSSLBool bool) {

	// TODO: add separate function to return cred object based on type
	id = os.Getenv("AWS_ACCESS_KEY_ID")
	Expect(id).NotTo(Equal(""),
		"AWS_ACCESS_KEY_ID Environment variable should not be empty")

	secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	Expect(secret).NotTo(Equal(""),
		"AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

	endpoint = os.Getenv("S3_ENDPOINT")
	Expect(endpoint).NotTo(Equal(""),
		"S3_ENDPOINT Environment variable should not be empty")

	s3Region = os.Getenv("S3_REGION")
	Expect(s3Region).NotTo(Equal(""),
		"S3_REGION Environment variable should not be empty")

	disableSSL := os.Getenv("S3_DISABLE_SSL")
	Expect(disableSSL).NotTo(Equal(""),
		"S3_DISABLE_SSL Environment variable should not be empty")

	disableSSLBool, err := strconv.ParseBool(disableSSL)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("S3_DISABLE_SSL=%s is not a valid boolean value", disableSSL))

	return id, secret, endpoint, s3Region, disableSSLBool
}

func getAzureCredsFromEnv() (tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey string) {
	accountName = os.Getenv("AZURE_ACCOUNT_NAME")
	Expect(accountName).NotTo(Equal(""),
		"AZURE_ACCOUNT_NAME Environment variable should not be empty")

	accountKey = os.Getenv("AZURE_ACCOUNT_KEY")
	Expect(accountKey).NotTo(Equal(""),
		"AZURE_ACCOUNT_KEY Environment variable should not be empty")

	log.Printf("Create creds for azure")
	tenantID = os.Getenv("AZURE_TENANT_ID")
	Expect(tenantID).NotTo(Equal(""),
		"AZURE_TENANT_ID Environment variable should not be empty")

	clientID = os.Getenv("AZURE_CLIENT_ID")
	Expect(clientID).NotTo(Equal(""),
		"AZURE_CLIENT_ID Environment variable should not be empty")

	clientSecret = os.Getenv("AZURE_CLIENT_SECRET")
	Expect(clientSecret).NotTo(Equal(""),
		"AZURE_CLIENT_SECRET Environment variable should not be empty")

	subscriptionID = os.Getenv("AZURE_SUBSCRIPTION_ID")
	Expect(clientSecret).NotTo(Equal(""),
		"AZURE_SUBSCRIPTION_ID Environment variable should not be empty")

	return tenantID, clientID, clientSecret, subscriptionID, accountName, accountKey
}

func CreateBackupLocation(provider, name, credName, bucketName, orgID string) {
	switch provider {
	case drivers.ProviderAws:
		createS3BackupLocation(name, credName, bucketName, orgID)
	case drivers.ProviderAzure:
		createAzureBackupLocation(name, credName, bucketName, orgID)
	}
}

// createS3BackupLocation creates backup location
func createS3BackupLocation(name string, cloudCred string, bucketName string, orgID string) {
	Step(fmt.Sprintf("Create S3 backup location [%s] in org [%s]", name, orgID), func() {
		CreateS3BackupLocation(name, cloudCred, bucketName, orgID)
	})
}

// createS3BackupLocation creates backup location
func createAzureBackupLocation(name, cloudCred, bucketName, orgID string) {
	Step(fmt.Sprintf("Create Azure backup location [%s] in org [%s]", name, orgID), func() {
		CreateAzureBackupLocation(name, cloudCred, bucketName, orgID)
	})
}

// createS3BackupLocation creates backup location
func createGkeBackupLocation(name string, cloudCred string, orgID string) {
	Step(fmt.Sprintf("Create GKE backup location [%s] in org [%s]", name, orgID), func() {
		// TODO(stgleb): Implement this
	})
}

// CreateS3BackupLocation creates backuplocation for S3
func CreateS3BackupLocation(name string, cloudCred string, bucketName string, orgID string) {
	backupDriver := Inst().Backup
	_, _, endpoint, region, disableSSLBool := getAWSDetailsFromEnv()
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:            bucketName,
			EncryptionKey:   encryptionKey,
			CloudCredential: cloudCred,
			Type:            api.BackupLocationInfo_S3,
			Config: &api.BackupLocationInfo_S3Config{
				S3Config: &api.S3Config{
					Endpoint:   endpoint,
					Region:     region,
					DisableSsl: disableSSLBool,
				},
			},
		},
	}
	_, err := backupDriver.CreateBackupLocation(bLocationCreateReq)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create backuplocation [%s] in org [%s]", name, orgID))
}

// CreateAzureBackupLocation creates backuplocation for Azure
func CreateAzureBackupLocation(name string, cloudCred string, bucketName string, orgID string) {
	backupDriver := Inst().Backup
	encryptionKey := "torpedo"
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		},
		BackupLocation: &api.BackupLocationInfo{
			Path:            bucketName,
			EncryptionKey:   encryptionKey,
			CloudCredential: cloudCred,
			Type:            api.BackupLocationInfo_Azure,
		},
	}
	_, err := backupDriver.CreateBackupLocation(bLocationCreateReq)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create backuplocation [%s] in org [%s]", name, orgID))
}

// DeleteBackupLocation deletes backuplocation
func DeleteBackupLocation(name string, orgID string) {
	Step(fmt.Sprintf("Delete backup location [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		bLocationDeleteReq := &api.BackupLocationDeleteRequest{
			Name:  name,
			OrgId: orgID,
		}
		backupDriver.DeleteBackupLocation(bLocationDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete backup location [%s] in org [%s]", name, orgID))
		// TODO: validate createBackupLocationResponse also
	})
}

// CreateSourceAndDestClusters creates source and destination cluster
// 1st cluster in KUBECONFIGS ENV var is source cluster while
// 2nd cluster is destination cluster
func CreateSourceAndDestClusters(cloudCred, orgID string) {
	// TODO: Add support for adding multiple clusters from
	// comma separated list of kubeconfig files
	kubeconfigs := os.Getenv("KUBECONFIGS")
	Expect(kubeconfigs).NotTo(Equal(""),
		"KUBECONFIGS Environment variable should not be empty")

	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 2 kubeconfigs for source and destination cluster
	Expect(len(kubeconfigList)).Should(BeNumerically(">=", 2), "At least minimum two kubeconfigs required")

	err := dumpKubeConfigs(configMapName, kubeconfigList)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName))

	// Register source cluster with backup driver
	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", sourceClusterName, orgID), func() {
		srcClusterConfigPath, err := getSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		logrus.Debugf("Save cluster %s kubeconfig to %s", sourceClusterName, srcClusterConfigPath)
		CreateCluster(sourceClusterName, cloudCred, srcClusterConfigPath, orgID)
	})

	// Register destination cluster with backup driver
	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", sestinationClusterName, orgID), func() {
		dstClusterConfigPath, err := getDestinationClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))
		logrus.Debugf("Save cluster %s kubeconfig to %s", sestinationClusterName, dstClusterConfigPath)
		CreateCluster(sestinationClusterName, cloudCred, dstClusterConfigPath, orgID)
	})
}

func getSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	Expect(len(kubeconfigList)).Should(BeNumerically(">=", 2),
		"At least minimum two kubeconfigs required")

	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[0]), nil
}

func getDestinationClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	Expect(len(kubeconfigList)).Should(BeNumerically(">=", 2),
		"At least minimum two kubeconfigs required")

	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[1]), nil
}

func dumpKubeConfigs(configObject string, kubeconfigList []string) error {
	logrus.Infof("dump kubeconfigs to file system")
	cm, err := core.Instance().GetConfigMap(configObject, "default")
	if err != nil {
		logrus.Errorf("Error reading config map: %v", err)
		return err
	}
	logrus.Infof("Get over kubeconfig list %v", kubeconfigList)
	for _, kubeconfig := range kubeconfigList {
		config := cm.Data[kubeconfig]
		if len(config) == 0 {
			configErr := fmt.Sprintf("Error reading kubeconfig: found empty %s in config map %s",
				kubeconfig, configObject)
			return fmt.Errorf(configErr)
		}
		filePath := fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfig)
		logrus.Infof("Save kubeconfig to %s", filePath)
		err := ioutil.WriteFile(filePath, []byte(config), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func DumpKubeconfigs(kubeconfigList []string) {
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get kubeconfigs [%v] from configmap [%s]", kubeconfigList, configMapName))
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
			fullPath := path.Join(kubeconfigDirectory, kubeconfigPath)
			return fullPath, nil
		}
	}

	return "nil", fmt.Errorf("kubeconfigPath not found for provider %s in kubeconfigPath list %v",
		provider, kubeconfigs)
}

// DeleteCluster deletes/de-registers cluster from px-backup
func DeleteCluster(name string, orgID string) {

	Step(fmt.Sprintf("Delete cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		clusterDeleteReq := &api.ClusterDeleteRequest{
			OrgId: orgID,
			Name:  name,
		}
		backupDriver.DeleteCluster(clusterDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete cluster [%s] in org [%s]", name, orgID))
	})
}

// CreateCluster creates/registers cluster with px-backup
func CreateCluster(name string, cloudCred string, kubeconfigPath string, orgID string) {

	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		kubeconfigRaw, err := ioutil.ReadFile(kubeconfigPath)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to read kubeconfig file from location [%s]. Error:[%v]",
				kubeconfigPath, err))

		clusterCreateReq := &api.ClusterCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  name,
				OrgId: orgID,
			},
			Kubeconfig:      base64.StdEncoding.EncodeToString(kubeconfigRaw),
			CloudCredential: cloudCred,
		}

		_, err = backupDriver.CreateCluster(clusterCreateReq)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create cluster [%s] in org [%s]. Error : [%v]",
				name, orgID, err))
	})
}

// CreateBackup creates backup
func CreateBackup(backupName string, clusterName string, bLocation string,
	namespaces []string, labelSelectors map[string]string, orgID string) {

	Step(fmt.Sprintf("Create backup [%s] in org [%s] from cluster [%s]",
		backupName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		bkpCreateRequest := &api.BackupCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  backupName,
				OrgId: orgID,
			},
			BackupLocation: bLocation,
			Cluster:        clusterName,
			Namespaces:     namespaces,
			LabelSelectors: labelSelectors,
		}
		_, err := backupDriver.CreateBackup(bkpCreateRequest)
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
		OrgId: orgID,
		Name:  clusterName,
		IncludeSecrets: true,
	}
	clusterInspectRes, err := backupDriver.InspectCluster(clusterInspectReq)
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
		_, err := backupDriver.CreateRestore(createRestoreReq)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
				restoreName, orgID, clusterName, err))
		// TODO: validate createClusterResponse also
	})
}

// DeleteBackup deletes backup
func DeleteBackup(backupName string, orgID string) {

	Step(fmt.Sprintf("Delete backup [%s] in org [%s]",
		backupName, orgID), func() {

		backupDriver := Inst().Backup
		bkpDeleteRequest := &api.BackupDeleteRequest{
			Name:  backupName,
			OrgId: orgID,
		}
		backupDriver.DeleteBackup(bkpDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete backup [%s] in org [%s]", backupName, orgID))
		// TODO: validate createClusterResponse also
	})
}

// DeleteRestore creates restore
func DeleteRestore(restoreName string, orgID string) {

	Step(fmt.Sprintf("Delete restore [%s] in org [%s]",
		restoreName, orgID), func() {

		backupDriver := Inst().Backup
		Expect(backupDriver).NotTo(BeNil(),
			"Backup driver is not initialized")

		deleteRestoreReq := &api.RestoreDeleteRequest{
			OrgId: orgID,
			Name:  restoreName,
		}
		_, err := backupDriver.DeleteRestore(deleteRestoreReq)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to delete restore [%s] in org [%s]. Error: [%v]",
				restoreName, orgID, err))
		// TODO: validate createClusterResponse also
	})
}
