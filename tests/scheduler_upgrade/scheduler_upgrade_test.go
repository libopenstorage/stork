package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"

	// https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

var (
	upgradeTimeoutMins = 90 * time.Minute
)

func TestSchedulerUpgrade(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_Upgrade.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : SchedulerUpgrade", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{UpgradeScheduler}", func() {
	var contexts []*scheduler.Context

	It("upgrade scheduler and ensure everything is running fine", func() {
		contexts = make([]*scheduler.Context, 0)

		intitialNodeCount, err := Inst().N.GetASGClusterSize()
		Expect(err).NotTo(HaveOccurred())

		logrus.Infof("Validating cluster size before upgrade. Initial Node Count: [%v]", intitialNodeCount)
		ValidateClusterSize(intitialNodeCount)

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradescheduler-%d", i))...)
		}

		ValidateApplications(contexts)

		upgradeHops := strings.Split(Inst().SchedUpgradeHops, ",")
		Expect(len(upgradeHops)).NotTo(Equal(0), "No upgrade hops provided for UpgradeScheduler test")

		for _, schedVersion := range upgradeHops {
			schedVersion = strings.TrimSpace(schedVersion)
			Step(fmt.Sprintf("start the upgrade of scheduler to version [%v]", schedVersion), func() {
				err := Inst().N.SetClusterVersion(schedVersion, upgradeTimeoutMins)
				Expect(err).NotTo(HaveOccurred())
			})

			Step(fmt.Sprintf("wait for %s minutes for auto recovery of storage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String()), func() {
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})

			err = Inst().S.RefreshNodeRegistry()
			Expect(err).NotTo(HaveOccurred())

			err = Inst().V.RefreshDriverEndpoints()
			Expect(err).NotTo(HaveOccurred())

			Step(fmt.Sprintf("validate number of storage nodes after scheduler upgrade to [%s]",
				schedVersion), func() {
				ValidateClusterSize(intitialNodeCount)
			})

			Step("validate all apps after upgrade", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})
			PerformSystemCheck()
		}
	})
	Step("teardown all apps", func() {
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
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
