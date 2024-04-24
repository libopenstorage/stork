package tests

import (
	//"context"
	"fmt"
	"os"
	"strings"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"

	//"github.com/portworx/torpedo/driver	"github.com/portworx/torpedo/drivers/scheduler"
	//"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	migrationRetryTimeout  = 10 * time.Minute
	migrationRetryInterval = 10 * time.Second
	domainCheckRetryTimeout  = 1 * time.Minute
	defaultClusterPairDir  = "cluster-pair"
	defaultClusterPairDirNew = "cluster-pair-new"
	defaultClusterPairName = "remoteclusterpair"
	defaultClusterPairNameNew = "remoteclusterpairnew"
	defaultBackupLocation = "s3"
	defaultSecret = "s3secret"

	migrationKey = "async-dr-"
	metromigrationKey = "metro-dr-"
)

var (
	kubeConfigWritten bool
)


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
		contexts              []*scheduler.Context
		migrationNamespaces   []string
		taskNamePrefix        = "metro-async-dr-mig"
		allMigrationsMetro         []*storkapi.Migration
		allMigrationsAsync         []*storkapi.Migration
		includeResourcesFlag  = true
		includeVolumesFlagMetro = false
		includeVolumesFlagAsync = true
		startApplicationsFlag = false
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
			if mig.Status.Status != storkapi.MigrationStatusSuccessful {
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
