package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

func TestUpgradeCluster(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_UpgradeCluster.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Upgrade Cluster", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{UpgradeCluster}", func() {
	var contexts []*scheduler.Context

	It("upgrade scheduler and ensure everything is running fine", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradecluster-%d", i))...)
		}

		ValidateApplications(contexts)

		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		Expect(versions).NotTo(BeEmpty())

		for _, version := range versions {
			Step("start scheduler upgrade", func() {
				err := Inst().S.UpgradeScheduler(version)
				Expect(err).NotTo(HaveOccurred())
			})

			Step("validate storage components", func() {
				err := Inst().V.ValidateStorageCluster(Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				Expect(err).NotTo(HaveOccurred())
			})

			Step("validate all apps after upgrade", func() {
				ValidateApplications(contexts)
			})
		}

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
