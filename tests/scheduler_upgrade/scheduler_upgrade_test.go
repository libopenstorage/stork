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
	"github.com/portworx/torpedo/drivers/node"
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

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradescheduler-%d", i))...)
		}

		ValidateApplications(contexts)

		upgradeHops := strings.Split(Inst().SchedUpgradeHops, ",")
		Expect(len(upgradeHops)).NotTo(Equal(0), "No upgrade hops provided for UpgradeScheduler test")

		intitialNodeCount, err := Inst().N.GetASGClusterSize()
		Expect(err).NotTo(HaveOccurred())

		for _, schedVersion := range upgradeHops {
			Step("start the upgrade of scheduler", func() {
				err := Inst().N.SetClusterVersion(schedVersion, upgradeTimeoutMins)
				Expect(err).NotTo(HaveOccurred())
			})

			Step(fmt.Sprintf("wait for %s minutes for auto recovery of storeage nodes",
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
		}
	})
})

func ValidateClusterSize(count int64) {
	// In multi-zone ASG cluster, node count is per zone
	perZoneCount := count / 3

	// Validate total node count
	currentNodeCount, err := Inst().N.GetASGClusterSize()
	Expect(err).NotTo(HaveOccurred())
	Expect(perZoneCount*3).Should(Equal(currentNodeCount),
		"ASG cluster size is not as expected."+
			" Current size is [%d]. Expected ASG size is [%d]",
		currentNodeCount, perZoneCount*3)

	// Validate storage node count
	var expectedStorageNodesPerZone int
	if Inst().MaxStorageNodesPerAZ <= int(perZoneCount) {
		expectedStorageNodesPerZone = Inst().MaxStorageNodesPerAZ
	} else {
		expectedStorageNodesPerZone = int(perZoneCount)
	}
	storageNodes, err := getStorageNodes()
	Expect(err).NotTo(HaveOccurred())
	Expect(len(storageNodes)).Should(Equal(expectedStorageNodesPerZone*3),
		"Current number of storeage nodes [%d] does not match expected number of storage nodes [%d]."+
			"List of storage nodes:[%v]",
		len(storageNodes), expectedStorageNodesPerZone*3, storageNodes)

	logrus.Infof("Validated successfully that [%d] storage nodes are present", len(storageNodes))
}

func getStorageNodes() ([]node.Node, error) {

	storageNodes := []node.Node{}
	nodes := node.GetStorageDriverNodes()

	for _, node := range nodes {
		devices, err := Inst().V.GetStorageDevices(node)
		if err != nil {
			return nil, err
		}
		if len(devices) > 0 {
			storageNodes = append(storageNodes, node)
		}
	}
	return storageNodes, nil
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
