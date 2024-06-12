package tests

import (
	//"context"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/storkctlcli"
	//"github.com/portworx/torpedo/driver	"github.com/portworx/torpedo/drivers/scheduler"
	//"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	migrationRetryTimeout     = 10 * time.Minute
	migrationRetryInterval    = 10 * time.Second
	domainCheckRetryTimeout   = 1 * time.Minute
	defaultClusterPairDir     = "cluster-pair"
	defaultClusterPairDirNew  = "cluster-pair-new"
	defaultClusterPairName    = "remoteclusterpair"
	defaultClusterPairNameNew = "remoteclusterpairnew"
	defaultBackupLocation     = "s3"
	defaultSecret             = "s3secret"
	defaultMigSchedName       = "automation-migration-schedule-"
	migrationKey              = "async-dr-"
	migrationSchedKey         = "mig-sched-"
	metromigrationKey         = "metro-dr-"
	clusterwideNs             = "openshift-operators"
)

var (
	kubeConfigWritten bool
)

type failoverFailbackParam struct {
	action                    string
	failoverOrFailbackNs      string
	migrationSchedName        string
	configPath                string
	single                    bool
	skipSourceOp              bool
	includeNs                 bool
	excludeNs                 bool
	extraArgsFailoverFailback map[string]string
	contexts                  []*scheduler.Context
}

// This test performs basic test of starting an application, creating cluster pair,
// and migrating application to the destination clsuter
var _ = Describe("{MigrateDeployment}", func() {
	testrailID = 50803
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50803
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("MigrateDeployment", "Migration of application to destination cluster", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var (
		contexts              []*scheduler.Context
		migrationNamespaces   []string
		taskNamePrefix        = "async-dr-mig"
		allMigrations         []*storkapi.Migration
		includeResourcesFlag  = true
		startApplicationsFlag = false
	)

	It("has to deploy app, create cluster pair, migrate app", func() {
		Step("Deploy applications", func() {

			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			// Schedule applications
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				ValidateApplications(contexts)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					migrationNamespaces = append(migrationNamespaces, namespace)
				}
				Step("Create cluster pair between source and destination clusters", func() {
					// Set cluster context to cluster where torpedo is running
					ScheduleValidateClusterPair(appContexts[0], false, true, defaultClusterPairDir, false)
				})
			}

			log.Infof("Migration Namespaces: %v", migrationNamespaces)

		})

		time.Sleep(5 * time.Minute)
		log.Info("Start migration")

		for i, currMigNamespace := range migrationNamespaces {
			migrationName := migrationKey + fmt.Sprintf("%d", i)
			currMig, err := CreateMigration(migrationName, currMigNamespace, defaultClusterPairName, currMigNamespace, &includeResourcesFlag, &startApplicationsFlag)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to create migration: %s in namespace %s. Error: [%v]",
					migrationKey, currMigNamespace, err))
			allMigrations = append(allMigrations, currMig)
		}

		for _, mig := range allMigrations {
			err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to validate migration: %s in namespace %s. Error: [%v]",
					mig.Name, mig.Namespace, err))
		}

		log.InfoD("Start volume only migration")
		includeResourcesFlag = false
		for i, currMigNamespace := range migrationNamespaces {
			migrationName := migrationKey + "volumeonly-" + fmt.Sprintf("%d", i)
			currMig, createMigErr := CreateMigration(migrationName, currMigNamespace, defaultClusterPairName, currMigNamespace, &includeResourcesFlag, &startApplicationsFlag)
			allMigrations = append(allMigrations, currMig)
			log.FailOnError(createMigErr, "Failed to create %s migration in %s namespace", migrationName, currMigNamespace)
			err := storkops.Instance().ValidateMigration(currMig.Name, currMig.Namespace, migrationRetryTimeout, migrationRetryInterval)
			dash.VerifyFatal(err, nil, "Migration successful?")
			resp, getMigErr := storkops.Instance().GetMigration(currMig.Name, currMig.Namespace)
			dash.VerifyFatal(getMigErr, nil, "Received migration response?")
			dash.VerifyFatal(resp.Status.Summary.NumberOfMigratedResources == 0, true, "Validate no resources migrated")
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

		Step("teardown migrations", func() {
			for _, mig := range allMigrations {
				err := DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("failed to delete migration: %s in namespace %s. Error: [%v]",
						mig.Name, mig.Namespace, err))
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{MigrateDeploymentMetroAsync}", func() {
	testrailID = 297595
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/297595
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		skipFlag := getClusterDomainsInfo()
		if skipFlag {
			Skip("Skip test because cluster domains are not set")
		}
		StartTorpedoTest("MigrateDeploymentMetroAsync", "Migration of application using metro+async combination", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var (
		contexts                []*scheduler.Context
		migrationNamespaces     []string
		taskNamePrefix          = "metro-async-dr-mig"
		allMigrationsMetro      []*storkapi.Migration
		allMigrationsAsync      []*storkapi.Migration
		includeResourcesFlag    = true
		includeVolumesFlagMetro = false
		includeVolumesFlagAsync = true
		startApplicationsFlag   = false
	)

	It("has to deploy app, create cluster pair, migrate app", func() {
		Step("Deploy applications", func() {
			err = SetCustomKubeConfig(asyncdr.FirstCluster)
			log.FailOnError(err, "Switching context to first cluster failed")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				ValidateApplications(contexts)
				for _, ctx := range contexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					migrationNamespaces = append(migrationNamespaces, namespace)
					log.Infof("Creating clusterpair between first and second cluster")
					err = ScheduleBidirectionalClusterPair(defaultClusterPairName, namespace, "", "", "", "sync-dr", asyncdr.FirstCluster, asyncdr.SecondCluster)
					log.FailOnError(err, "Failed creating bidirectional cluster pair")
				}
			}
			log.Infof("Migration Namespaces: %v", migrationNamespaces)
		})

		log.Infof("Start migration Metro")

		for i, currMigNamespace := range migrationNamespaces {
			migrationName := metromigrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
			currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, defaultClusterPairName, currMigNamespace, &includeVolumesFlagMetro, &includeResourcesFlag, &startApplicationsFlag, nil)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to create migration: %s in namespace %s. Error: [%v]",
					migrationKey, currMigNamespace, err))
			allMigrationsMetro = append(allMigrationsMetro, currMig)
		}

		// Validate all migrations
		for _, mig := range allMigrationsMetro {
			err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to validate migration: %s in namespace %s. Error: [%v]",
					mig.Name, mig.Namespace, err))
		}

		err = SetCustomKubeConfig(asyncdr.SecondCluster)
		log.FailOnError(err, "Switching context to second cluster failed")

		log.Infof("Start Async migration from Second DR to Third DR")

		for i, currMigNamespace := range migrationNamespaces {
			log.Infof("Creating clusterpair between second and third cluster")
			ScheduleBidirectionalClusterPair(defaultClusterPairNameNew, currMigNamespace, "", storkapi.BackupLocationType(defaultBackupLocation), defaultSecret, "async-dr", asyncdr.SecondCluster, asyncdr.ThirdCluster)
			migrationName := migrationKey + fmt.Sprintf("%d", i) + time.Now().Format("15h03m05s")
			currMig, err := asyncdr.CreateMigration(migrationName, currMigNamespace, defaultClusterPairNameNew, currMigNamespace, &includeVolumesFlagAsync, &includeResourcesFlag, &startApplicationsFlag, nil)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to create migration: %s in namespace %s. Error: [%v]",
					migrationKey, currMigNamespace, err))
			allMigrationsAsync = append(allMigrationsAsync, currMig)
		}

		for _, mig := range allMigrationsAsync {
			err := storkops.Instance().ValidateMigration(mig.Name, mig.Namespace, migrationRetryTimeout, migrationRetryInterval)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("failed to validate migration: %s in namespace %s. Error: [%v]",
					mig.Name, mig.Namespace, err))
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultAsyncSingle}", func() {
	testrailID = 296255
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296255
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultAsyncSingle", "Failover and Failback using storkctl on async cluster for single NS", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("asyncdr", "asyncdr-failover-failback", true, false, false, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultAsyncSkipSourceOperations}", func() {
	testrailID = 296256
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296256
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultAsyncSkipSourceOperations", "Failover and Failback using storkctl on async cluster with skip source operations", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("asyncdr", "asyncdr-failover-failback", false, true, false, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultAsyncIncludeNs}", func() {
	testrailID = 296368
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296368
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultAsyncIncludeNs", "Failover and Failback using storkctl on async cluster with Include Ns", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("asyncdr", "asyncdr-failover-failback", false, false, true, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultAsyncExcludeNs}", func() {
	testrailID = 296367
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296367
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultAsyncExcludeNs", "Failover and Failback using storkctl on async cluster with Exclude Ns", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("asyncdr", "asyncdr-failover-failback", false, false, false, true)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultAsyncMultiple}", func() {
	testrailID = 296255
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296255
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultAsyncMultiple", "Failover and Failback using storkctl on async cluster for multiple NS", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("asyncdr", "asyncdr-failover-failback", false, false, false, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultMetroSingle}", func() {
	testrailID = 296291
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296291
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		skipFlag := getClusterDomainsInfo()
		if skipFlag {
			Skip("Skip test because cluster domains are not set")
		}
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultMetroSingle", "Failover and Failback using storkctl on metro cluster for single NS", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("metrodr", "metrodr-failover-failback", true, false, false, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackDefaultMetroMultiple}", func() {
	testrailID = 296291
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296291
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		skipFlag := getClusterDomainsInfo()
		if skipFlag {
			Skip("Skip test because cluster domains are not set")
		}
		StartTorpedoTest("StorkctlPerformFailoverFailbackDefaultMetroMultiple", "Failover and Failback using storkctl on metro cluster for multiple NS", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			validateFailoverFailback("metrodr", "metrodr-failover-failback", false, false, false, false)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackPostgresql}", func() {
	testrailID = 296287
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296287
	BeforeEach(func() {
		if !kubeConfigWritten {
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})

	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackPostgresql", "Failover and Failback using storkctl for postgresql namespaced operator", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	
	var (
		appPath = "/torpedo/deployments/customconfigs/pgo.yaml"
		opPath = "/torpedo/deployments/customconfigs/pgo-operator.yaml"
		opName = "pgo"
		crName = "postgrescluster"
		ns = "post"
	)

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			
			podList, err := createOperatorBasedApp(appPath, opPath, ns, false)
			log.FailOnError(err, "Failed to create operator based app")
			log.Infof("PodList is: %v", podList)
			podCount := len(podList.Items)
			validateOperatorMigFailover(ns, "asyncdr", opName, crName, podCount, false)
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackElasticSearch}", func() {
	testrailID = 296285
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296285
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})
	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackElasticSearch", "Failover and Failback using storkctl for elasticsearch namespaced operator", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var (
		appPath = "/torpedo/deployments/customconfigs/elasticcr.yaml"
		opPath = "/torpedo/deployments/customconfigs/elastic-op.yaml"
		opName = "elastic-operator"
		crName = "elasticsearch"
		ns = "esop"
	)

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			podList, err := createOperatorBasedApp(appPath, opPath, ns, false)
			log.FailOnError(err, "Failed to create operator based app")
			log.Infof("PodList is: %v", podList)
			podCount := len(podList.Items)
			validateOperatorMigFailover(ns, "asyncdr", opName, crName, podCount, false)
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackPostgresqlClusterwide}", func() {
	testrailID = 297919
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/297919
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})

	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackPostgresqlClusterwide", "Failover and Failback using storkctl for postgresql clusterwide operator", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var (
		appPath = "/torpedo/deployments/customconfigs/pgo.yaml"
		opPath = "/torpedo/deployments/customconfigs/pgo-operator-clusterwide.yaml"
		opName = "pgo"
		crName = "postgrescluster"
		ns = "pgcw-" + time.Now().Format("15h03m05s")
	)

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			podList, err := createOperatorBasedApp(appPath, opPath, ns, true)
			log.FailOnError(err, "Failed to create operator based app")
			log.Infof("PodList is: %v", podList)
			podCount := len(podList.Items)
			validateOperatorMigFailover(ns, "asyncdr", opName, crName, podCount, true)
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorkctlPerformFailoverFailbackeckEsClusterwide}", func() {
	testrailID = 297921
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/297921
	BeforeEach(func() {
		if !kubeConfigWritten {
			// Write kubeconfig files after reading from the config maps created by torpedo deploy script
			WriteKubeconfigToFiles()
			kubeConfigWritten = true
		}
		wantAllAfterSuiteActions = false
	})

	JustBeforeEach(func() {
		StartTorpedoTest("StorkctlPerformFailoverFailbackeckEsClusterwide", "Failover and Failback using storkctl for elasticsearch clusterwide operator", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var (
		appPath = "/torpedo/deployments/customconfigs/elasticcr.yaml"
		opPath = "/torpedo/deployments/customconfigs/elastic-op-cw.yaml"
		opName = "elastic-operator"
		crName = "elasticsearch"
		ns = "escw-" + time.Now().Format("15h03m05s")
	)

	It("has to deploy app, create cluster pair, migrate app and do failover/failback", func() {
		Step("Deploy app, Create cluster pair, Migrate app and Do failover/failback", func() {
			podList, err := createOperatorBasedApp(appPath, opPath, ns, true)
			log.FailOnError(err, "Failed to create operator based app")
			log.Infof("PodList is: %v", podList)
			podCount := len(podList.Items)
			validateOperatorMigFailover(ns, "asyncdr", opName, crName, podCount, true)
		})
	})
	
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func validateFailoverFailback(clusterType, taskNamePrefix string, single, skipSourceOp, includeNs, excludeNs bool) {
	defaultNs := "kube-system"
	migrationNamespaces, contexts := initialSetupApps(taskNamePrefix, single)
	migNamespaces := strings.Join(migrationNamespaces, ",")
	kubeConfigPathSrc, err := GetCustomClusterConfigPath(asyncdr.FirstCluster)
	log.FailOnError(err, "Failed to get source configPath: %v", err)
	kubeConfigPathDest, err := GetCustomClusterConfigPath(asyncdr.SecondCluster)
	log.FailOnError(err, "Failed to get destination configPath: %v", err)
	if single {
		defaultNs = migrationNamespaces[0]
		migNamespaces = defaultNs
	}
	extraArgs := map[string]string{
		"namespaces": migNamespaces,
		"kubeconfig": kubeConfigPathSrc,
	}
	log.Infof("Creating clusterpair between first and second cluster")
	cpName := defaultClusterPairName + time.Now().Format("15h03m05s")
	if clusterType == "asyncdr" {
		err = ScheduleBidirectionalClusterPair(cpName, defaultNs, "", storkapi.BackupLocationType(defaultBackupLocation), defaultSecret, "async-dr", asyncdr.FirstCluster, asyncdr.SecondCluster)
	} else {
		err = ScheduleBidirectionalClusterPair(cpName, defaultNs, "", "", "", "sync-dr", asyncdr.FirstCluster, asyncdr.SecondCluster)
	}
	log.FailOnError(err, "Failed creating bidirectional cluster pair")
	log.Infof("Start migration schedule and perform failover")
	migrationSchedName := migrationSchedKey + time.Now().Format("15h03m05s")
	createMigSchdAndValidateMigration(migrationSchedName, cpName, defaultNs, kubeConfigPathSrc, extraArgs)
	err = SetCustomKubeConfig(asyncdr.SecondCluster)
	log.FailOnError(err, "Switching context to second cluster failed")
	extraArgsFailoverFailback := map[string]string{
		"kubeconfig": kubeConfigPathDest,
	}
	if includeNs {
		extraArgsFailoverFailback["include-namespaces"] = migrationNamespaces[0]
	}
	if excludeNs {
		extraArgsFailoverFailback["exclude-namespaces"] = migrationNamespaces[0]
	}
	failoverParam := failoverFailbackParam{
		action:                    "failover",
		failoverOrFailbackNs:      defaultNs,
		migrationSchedName:        migrationSchedName,
		configPath:                kubeConfigPathDest,
		single:                    single,
		skipSourceOp:              skipSourceOp,
		includeNs:                 includeNs,
		excludeNs:                 excludeNs,
		extraArgsFailoverFailback: extraArgsFailoverFailback,
		contexts:                  contexts,
	}
	performFailoverFailback(failoverParam)
	if skipSourceOp {
		err = hardSetConfig(kubeConfigPathSrc)
		log.FailOnError(err, "Error setting source config: %v", err)
		for _, ctx := range contexts {
			waitForPodsToBeRunning(ctx, false)
		}
	} else {
		err = hardSetConfig(kubeConfigPathDest)
		log.FailOnError(err, "Error setting destination config: %v", err)
		extraArgs["kubeconfig"] = kubeConfigPathDest
		newMigSched := migrationSchedName + "-rev"
		if includeNs {
			extraArgs["namespaces"] = migrationNamespaces[0]
		}
		if excludeNs {
			extraArgs["namespaces"] = strings.Join(migrationNamespaces[1:], ",")
			extraArgsFailoverFailback["exclude-namespaces"] = migrationNamespaces[1]
		}
		createMigSchdAndValidateMigration(newMigSched, cpName, defaultNs, kubeConfigPathDest, extraArgs)
		failoverback := failoverFailbackParam{
			action:                    "failback",
			failoverOrFailbackNs:      defaultNs,
			migrationSchedName:        newMigSched,
			configPath:                kubeConfigPathDest,
			single:                    single,
			skipSourceOp:              false,
			includeNs:                 includeNs,
			excludeNs:                 excludeNs,
			extraArgsFailoverFailback: extraArgsFailoverFailback,
			contexts:                  contexts,
		}
		performFailoverFailback(failoverback)
	}
	err = asyncdr.WaitForNamespaceDeletion(migrationNamespaces)
	if err != nil {
		log.Infof("Failed to delete namespaces: %v", err)
	}
	err = hardSetConfig(kubeConfigPathDest)
	if err != nil {
		log.Infof("Failed to se dest kubeconfig for NS deletion on dest: %v", err)
	}
	err = asyncdr.WaitForNamespaceDeletion(migrationNamespaces)
	if err != nil {
		log.Infof("Failed to delete namespaces: %v", err)
	}
}

func getClusterDomainsInfo() bool {
	skipFlag := false
	listCdsTask := func() (interface{}, bool, error) {
		// Fetch the cluster domains
		cdses, err := storkops.Instance().ListClusterDomainStatuses()
		if err != nil || len(cdses.Items) == 0 {
			log.Infof("Failed to list cluster domains statuses. Error: %v. List of cluster domains: %v", err, len(cdses.Items))
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		cds := cdses.Items[0]
		if len(cds.Status.ClusterDomainInfos) == 0 {
			log.Infof("Found 0 cluster domain info objects in cluster domain status.")
			return "", true, fmt.Errorf("failed to list cluster domains statuses")
		}
		return "", false, nil
	}
	_, err := task.DoRetryWithTimeout(listCdsTask, domainCheckRetryTimeout, migrationRetryInterval)
	if err != nil {
		skipFlag = true
	}
	return skipFlag
}

func WriteKubeconfigToFiles() {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	Expect(kubeconfigs).NotTo(Equal(""),
		"KUBECONFIGS Environment variable should not be empty")

	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 1 kubeconfig for cluster
	Expect(len(kubeconfigList)).Should(BeNumerically(">=", 2), "At least minimum two kubeconfigs required")

	DumpKubeconfigs(kubeconfigList)

}

func CreateMigration(
	name string,
	namespace string,
	clusterPair string,
	migrationNamespace string,
	includeResources *bool,
	startApplications *bool,
) (*storkapi.Migration, error) {

	migration := &storkapi.Migration{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: storkapi.MigrationSpec{
			ClusterPair:       clusterPair,
			IncludeResources:  includeResources,
			StartApplications: startApplications,
			Namespaces:        []string{migrationNamespace},
		},
	}
	// TODO figure out a way to check if it's an auth-enabled and add security annotations
	//if authTokenConfigMap != "" {
	//	err := addSecurityAnnotation(migration)
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	mig, err := storkops.Instance().CreateMigration(migration)
	return mig, err
}

func deleteMigrations(migrations []*storkapi.Migration) error {
	for _, mig := range migrations {
		err := storkops.Instance().DeleteMigration(mig.Name, mig.Namespace)
		if err != nil {
			return fmt.Errorf("Failed to delete migration %s in namespace %s. Error: %v", mig.Name, mig.Namespace, err)
		}
	}
	return nil
}

func WaitForMigration(migrationList []*storkapi.Migration) error {
	checkMigrations := func() (interface{}, bool, error) {
		isComplete := true
		for _, m := range migrationList {
			mig, err := storkops.Instance().GetMigration(m.Name, m.Namespace)
			if err != nil {
				return "", false, err
			}
			if mig.Status.Status != storkapi.MigrationStatusSuccessful && mig.Status.Status != storkapi.MigrationStatusPartialSuccess {
				log.Infof("Migration %s in namespace %s is pending", m.Name, m.Namespace)
				isComplete = false
			}
		}
		if isComplete {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some migrations are still pending")
	}
	_, err := task.DoRetryWithTimeout(checkMigrations, migrationRetryTimeout, migrationRetryInterval)
	return err
}

func DeleteAndWaitForMigrationDeletion(name, namespace string) error {
	log.Infof("Deleting migration: %s in namespace: %s", name, namespace)
	err := storkops.Instance().DeleteMigration(name, namespace)
	if err != nil {
		return fmt.Errorf("Failed to delete migration: %s in namespace: %s", name, namespace)
	}
	getMigration := func() (interface{}, bool, error) {
		migration, err := storkops.Instance().GetMigration(name, namespace)
		if err == nil {
			return "", true, fmt.Errorf("Migration %s in %s has not completed yet.Status: %s. Retrying ", name, namespace, migration.Status.Status)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(getMigration, migrationRetryTimeout, migrationRetryInterval)
	return err
}

func initialSetupApps(taskNamePrefix string, single bool) ([]string, []*scheduler.Context) {
	var contexts []*scheduler.Context
	var migrationNamespaces []string

	err = SetCustomKubeConfig(asyncdr.FirstCluster)
	log.FailOnError(err, "Switching context to first cluster failed")
	if single {
		taskName := fmt.Sprintf("%s", taskNamePrefix)
		log.Infof("Task name %s\n", taskName)
		contexts = append(contexts, ScheduleApplications(taskName)...)
	} else {
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Infof("Task name %s\n", taskName)
			contexts = append(contexts, ScheduleApplications(taskName)...)
		}
	}
	for _, ctx := range contexts {
		// Override default App readiness time out of 5 mins with 10 mins
		ctx.ReadinessTimeout = appReadinessTimeout
		namespace := GetAppNamespace(ctx, "")
		migrationNamespaces = append(migrationNamespaces, namespace)
	}
	log.Infof("Migration Namespaces are : [%v]", migrationNamespaces)
	ValidateApplications(contexts)
	return migrationNamespaces, contexts
}

func createMigSchdAndValidateMigration(migSchedName, cpName, migNs, resetConfigPath string, extraArgs map[string]string) {
	var migration *storkapi.Migration
	err = storkctlcli.ScheduleStorkctlMigrationSched(migSchedName, cpName, migNs, extraArgs)
	log.FailOnError(err, "Error creating migrationschedule: %v", err)
	err = hardSetConfig(resetConfigPath)
	log.FailOnError(err, "Error setting destination config: %v", err)
	time.Sleep(time.Second * 30)
	migSchedule, err := storkops.Instance().GetMigrationSchedule(migSchedName, migNs)
	log.FailOnError(err, "failed to get migrationschedule %v, err: %v", migSchedName, err)
	migrations := migSchedule.Status.Items["Interval"]
	for _, mig := range migrations {
		migration, err = storkops.Instance().GetMigration(mig.Name, migNs)
		log.FailOnError(err, "failed to get migration for migrationschedule %v, err: %v", migSchedName, err)
		err = WaitForMigration([]*storkapi.Migration{migration})
		log.FailOnError(err, "Migration failed with error: %v", err)
	}
}

func performFailoverFailback(foFbParams failoverFailbackParam) {
	err, output := storkctlcli.PerformFailoverOrFailback(foFbParams.action, foFbParams.failoverOrFailbackNs, foFbParams.migrationSchedName, foFbParams.skipSourceOp, foFbParams.extraArgsFailoverFailback)
	log.FailOnError(err, "Error running perform %v: %v", foFbParams.action, err)
	splitOutput := strings.Split(output, "\n")
	prefix := fmt.Sprintf("To check %s status use the command : `", foFbParams.action)
	getStatusCommand := strings.TrimSpace(strings.TrimPrefix(splitOutput[1], prefix))
	getStatusCommand = strings.TrimSuffix(getStatusCommand, "`")
	getStatusCmdArgs := strings.Split(getStatusCommand, " ")
	// Extract the action Name from the command args
	actionName := getStatusCmdArgs[3]
	err = storkctlcli.WaitForActionSuccessful(actionName, foFbParams.failoverOrFailbackNs, Inst().GlobalScaleFactor)
	log.FailOnError(err, "Error in performing %v: %v", foFbParams.action, err)
	validatePodsRunning(foFbParams.action, foFbParams.single, foFbParams.includeNs, foFbParams.excludeNs, foFbParams.contexts)
}

func validatePodsRunning(action string, single, includeNs, excludeNs bool, contexts []*scheduler.Context) {
	switch action {
	case "failover":
		if includeNs {
			waitForPodsToBeRunning(contexts[0], false)
			for i := 1; i < len(contexts); i++ {
				ctx := contexts[i]
				waitForPodsToBeRunning(ctx, true)
			}
		} else if excludeNs {
			waitForPodsToBeRunning(contexts[0], true)
			for i := 1; i < len(contexts); i++ {
				ctx := contexts[i]
				waitForPodsToBeRunning(ctx, false)
			}
		} else if single {
			waitForPodsToBeRunning(contexts[0], false)
		} else {
			for _, ctx := range contexts {
				waitForPodsToBeRunning(ctx, false)
			}
		}
	case "failback":
		kubeConfigPathSrc, err := GetCustomClusterConfigPath(asyncdr.FirstCluster)
		log.FailOnError(err, "Failed to get source configPath: %v", err)
		err = hardSetConfig(kubeConfigPathSrc)
		log.FailOnError(err, "Error setting source config")
		if includeNs {
			for _, ctx := range contexts {
				waitForPodsToBeRunning(ctx, false)
			}
		} else if excludeNs {
			for i := 1; i < len(contexts); i++ {
				ctx := contexts[i]
				if i == 1 {
					waitForPodsToBeRunning(ctx, true)
				} else {
					waitForPodsToBeRunning(ctx, false)
				}
			}
		} else if single {
			waitForPodsToBeRunning(contexts[0], false)
		} else {
			for _, ctx := range contexts {
				waitForPodsToBeRunning(ctx, false)
			}
		}
	}
}

func hardSetConfig(configPath string) error {
	var config *rest.Config
	config, err = clientcmd.BuildConfigFromFlags("", configPath)
	if err != nil {
		return err
	}
	core.Instance().SetConfig(config)
	apps.Instance().SetConfig(config)
	stork.Instance().SetConfig(config)
	return nil
}

func waitForPodsToBeRunning(context *scheduler.Context, expectedFail bool) {
	log.Infof("Verifying Context [%v]", context.App.Key)
	err := Inst().S.WaitForRunning(context, 5*time.Minute, 10*time.Second)
	if expectedFail {
		log.FailOnNoError(err, "Pods are up on destination, they shouldn't be up")
	} else {
		log.FailOnError(err, "Error waiting for pods to be up")
	}
}

func validateOperatorMigFailover(namespace, clusterType, opName, crName string, podCount int, clusterwide bool) {
	cpName := defaultClusterPairName + time.Now().Format("15h03m05s")
	kubeConfigPathSrc, err := GetCustomClusterConfigPath(asyncdr.FirstCluster)
	log.FailOnError(err, "Failed to get source configPath: %v", err)
	kubeConfigPathDest, err := GetCustomClusterConfigPath(asyncdr.SecondCluster)
	log.FailOnError(err, "Failed to get destination configPath: %v", err)
	if clusterType == "asyncdr" {
		err = ScheduleBidirectionalClusterPair(cpName, namespace, "", storkapi.BackupLocationType(defaultBackupLocation), defaultSecret, "async-dr", asyncdr.FirstCluster, asyncdr.SecondCluster)
	} else {
		err = ScheduleBidirectionalClusterPair(cpName, namespace, "", "", "", "sync-dr", asyncdr.FirstCluster, asyncdr.SecondCluster)
	}
	log.FailOnError(err, "Failed creating bidirectional cluster pair")
	log.Infof("Start migration schedule and perform failover")
	migrationSchedName := migrationSchedKey + time.Now().Format("15h03m05s")
	extraArgs := map[string]string{
		"namespaces":             namespace,
		"kubeconfig":             kubeConfigPathSrc,
		"exclude-resource-types": "ClusterServiceVersion,operatorconditions,OperatorGroup,InstallPlan,Subscription",
		"exclude-selectors":      "olm.managed=true",
	}
	extraArgsFailoverFailback := map[string]string{
		"kubeconfig": kubeConfigPathDest,
	}
	err = patchStashStrategy(crName)
	log.FailOnError(err, "Failed to patch stash strategy")
	createMigSchdAndValidateMigration(migrationSchedName, cpName, namespace, kubeConfigPathSrc, extraArgs)
	err = hardSetConfig(kubeConfigPathSrc)
	log.FailOnError(err, "Switching context to source cluster failed")
	scaleCrApp(namespace, opName, true, clusterwide)
	err = SetCustomKubeConfig(asyncdr.SecondCluster)
	log.FailOnError(err, "Switching context to second cluster failed")
	err, output := storkctlcli.PerformFailoverOrFailback("failover", namespace, migrationSchedName, false, extraArgsFailoverFailback)
	log.FailOnError(err, "Error running perform %v: %v", "failover", err)
	actionName := extractActionName(output, "failover")
	err = storkctlcli.WaitForActionSuccessful(actionName, namespace, Inst().GlobalScaleFactor)
	log.FailOnError(err, "Error in performing %v: %v", "failover", err)
	time.Sleep(5 * time.Minute)
	podListDest, err := core.Instance().GetPods(namespace, nil)
	if err != nil {
		log.Errorf("Error getting podlist: %v", err)
	}
	err = asyncdr.WaitForPodToBeRunning(podListDest)
	if err != nil {
		log.Errorf("Error waiting for pod to be running: %v", err)
	}
	podCountDest := len(podListDest.Items)
	dash.VerifyFatal(podCountDest == podCount, true, "Pod count is not equal to expected")
	err = patchStashStrategy(crName)
	log.FailOnError(err, "Failed to patch stash strategy")
	err = hardSetConfig(kubeConfigPathDest)
	log.FailOnError(err, "Switching context to dest cluster failed")
	migrationSchedNameRev := migrationSchedKey + time.Now().Format("15h03m05s") + "-rev"
	extraArgs["kubeconfig"] = kubeConfigPathDest
	createMigSchdAndValidateMigration(migrationSchedNameRev, cpName, namespace, kubeConfigPathDest, extraArgs)
	err = hardSetConfig(kubeConfigPathDest)	
	log.FailOnError(err, "Failed to set destination kubeconfig")
	scaleCrApp(namespace, opName, true, clusterwide)
	err = SetDestinationKubeConfig()
	log.FailOnError(err, "Failed to set destination kubeconfig")
	err, output = storkctlcli.PerformFailoverOrFailback("failback", namespace, migrationSchedNameRev, false, extraArgsFailoverFailback)
	log.FailOnError(err, "Error running perform %v: %v", "failback", err)
	actionName = extractActionName(output, "failback")
	err = storkctlcli.WaitForActionSuccessful(actionName, namespace, Inst().GlobalScaleFactor)
	log.FailOnError(err, "Error in performing %v: %v", "failback", err)
	err = SetSourceKubeConfig()
	log.FailOnError(err, "Failed to set source kubeconfig")
	scaleCrApp(namespace, opName, false, clusterwide)
	time.Sleep(5 * time.Minute)
	err = hardSetConfig(kubeConfigPathSrc)
	log.FailOnError(err, "Failed to set source kubeconfig")
	podListFailback, err := core.Instance().GetPods(namespace, nil)
	if err != nil {
		log.Errorf("Error getting podlist: %v", err)
	}
	err = asyncdr.WaitForPodToBeRunning(podListFailback)
	if err != nil {
		log.Errorf("Error waiting for pod to be running: %v", err)
	}
	podCountFailback := len(podListFailback.Items)
	dash.VerifyFatal(podCountFailback == podCount, true, "Pod count is not equal to expected")
}

func scaleCrApp(namespace, opName string, down, cw bool) {
	opNs := namespace
	if cw {
		opNs = clusterwideNs
	}
	desRepl := int32(1)
	if down {
		desRepl = int32(0)
	}
	deplop, err := apps.Instance().GetDeployment(opName, opNs)
	log.FailOnError(err, "Failed to get deployment")
	deplop.Spec.Replicas = &desRepl
	_, err = apps.Instance().UpdateDeployment(deplop)
	log.FailOnError(err, "Failed to scale deployment")

	if down {
		depl, err := apps.Instance().ListDeployments(namespace, meta_v1.ListOptions{})
		log.FailOnError(err, "Failed to list deployments")
		for _, d := range depl.Items {
			if d.Name != opName {
				d.Spec.Replicas = &desRepl
				_, err = apps.Instance().UpdateDeployment(&d)
				log.FailOnError(err, "Failed to scale down deployment")
			}
		}
		sts, err := apps.Instance().ListStatefulSets(namespace, meta_v1.ListOptions{})
		log.FailOnError(err, "Failed to list deployments")
		for _, s := range sts.Items {
			s.Spec.Replicas = &desRepl
			_, err = apps.Instance().UpdateStatefulSet(&s)
			log.FailOnError(err, "Failed to scale down statefulset")
		}
	}
}

func createOperatorBasedApp(appPath, opPath, ns string, clusterwide bool) (*v1.PodList, error) {
	nsSpec := &v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: ns,
		},
	}
	_, err := core.Instance().CreateNamespace(nsSpec)
	if err != nil {
		return nil, err
	}
	if clusterwide {
		deployAndWaitForRunning(opPath, clusterwideNs, "src")
	} else {
		deployAndWaitForRunning(opPath, ns, "src")
	}
	deployAndWaitForRunning(appPath, ns, "src")
	podList, err := core.Instance().GetPods(ns, nil)
	if err != nil {
		return nil, err
	}
	err = SetDestinationKubeConfig()
	if err != nil {
		return nil, err
	}
	_, err = core.Instance().CreateNamespace(nsSpec)
	if err != nil {
		return nil, err
	}
	// Deploy operator
	if clusterwide {
		deployAndWaitForRunning(opPath, clusterwideNs, "dest")
	} else {
		deployAndWaitForRunning(opPath, ns, "dest")
	}
	return podList, nil
}

func deployAndWaitForRunning(path, namespace, cluster string) error {
	cmd := fmt.Sprintf("kubectl apply -f %v -n %v", path, namespace)
	if cluster == "dest" {
		kubeConfigPathDest, err := GetCustomClusterConfigPath(asyncdr.SecondCluster)
		if err != nil {
			return err
		}
		cmd = fmt.Sprintf("kubectl apply -f %v -n %v --kubeconfig %v", path, namespace, kubeConfigPathDest)
		err = hardSetConfig(kubeConfigPathDest)
		if err != nil {
			return err
		}
	}	
	log.InfoD("Running command: %v", cmd)
	_, _, err = osutils.ExecShell(cmd)
	if err != nil {
		return err
	}
	// Sleeping here, as apps deploys one by one, which takes time to collect all pods
	time.Sleep(1 * time.Minute)
	podList, err := core.Instance().GetPods(namespace, nil)
	if err != nil {
		return err
	}
	err = asyncdr.WaitForPodToBeRunning(podList)
	if err != nil {
		return err
	}
	err = SetSourceKubeConfig()
	if err != nil {
		return err
	}
	return nil
}

type PatchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

type ApplicationRegistration struct {
	Resources []struct {
	} `json:"resources"`
}

func getNumResources(crName string) (int, error) {
	cmd := exec.Command("kubectl", "get", "applicationregistration", crName, "-o", "json")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Infof("Error getting applicationregistration: %v", err)
		return 0, err
	}
	var appReg ApplicationRegistration
	err = json.Unmarshal(out.Bytes(), &appReg)
	if err != nil {
		log.Infof("Error unmarshalling JSON: %v", err)
		return 0, err
	}
	return len(appReg.Resources), nil
}

func patchStashStrategy(crName string) error {
	var patches []PatchOperation
	numResources, err := getNumResources(crName)
	if err != nil {
		return err
	}
	for i := 0; i < numResources; i++ {
		patches = append(patches, PatchOperation{
			Op:    "replace",
			Path:  fmt.Sprintf("/resources/%d/stashStrategy/stashCR", i),
			Value: true,
		})
	}
	patchBytes, err := json.Marshal(patches)
	if err != nil {
		log.Infof("Error marshalling patches: %v", err)
		return err
	}
	cmd := fmt.Sprintf(`kubectl patch applicationregistration %v --type='json' -p='%s'`, crName, string(patchBytes))
	log.Infof("Running command: %v", cmd)
	_, err = exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		log.Infof("Error running command: %v and err is: %v", cmd, err)
		return err
	}
	return nil
}

func extractActionName(output, action string) string {
	splitOutput := strings.Split(output, "\n")
	prefix := fmt.Sprintf("To check %s status use the command : `", action)
	getStatusCommand := strings.TrimSpace(strings.TrimPrefix(splitOutput[1], prefix))
	getStatusCommand = strings.TrimSuffix(getStatusCommand, "`")
	getStatusCmdArgs := strings.Split(getStatusCommand, " ")
	return getStatusCmdArgs[3]
}
