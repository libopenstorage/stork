package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

const (
	defaultTimeout       = 1 * time.Minute
	driveFailTimeout     = 2 * time.Minute
	defaultRetryInterval = 5 * time.Second
)

func TestDriveFailure(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_DriveFailure.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : DriveFailure", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{DriveFailure}", func() {
	var contexts []*scheduler.Context

	testName := "drivefailure"
	It("has to schedule apps and induce a drive failure on one of the nodes", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		Step("get nodes for all apps in test and induce drive failure on one of the nodes", func() {
			for _, ctx := range contexts {
				var (
					drives        []string
					appNodes      []node.Node
					nodeWithDrive node.Node
				)

				Step(fmt.Sprintf("get nodes where %s app is running", ctx.App.Key), func() {
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appNodes).NotTo(BeEmpty())
					nodeWithDrive = appNodes[0]
				})

				Step(fmt.Sprintf("get drive from node %v", nodeWithDrive), func() {
					drives, err = Inst().V.GetStorageDevices(nodeWithDrive)
					Expect(err).NotTo(HaveOccurred())
					Expect(drives).NotTo(BeEmpty())
				})

				busInfoMap := make(map[string]string)
				Step(fmt.Sprintf("induce a failure on all drives on the node %v", nodeWithDrive), func() {
					for _, driveToFail := range drives {
						busID, err := Inst().N.YankDrive(nodeWithDrive, driveToFail, node.ConnectionOpts{
							Timeout:         defaultTimeout,
							TimeBeforeRetry: defaultRetryInterval,
						})
						busInfoMap[driveToFail] = busID
						Expect(err).NotTo(HaveOccurred())
					}
					Step("wait for the drives to fail", func() {
						time.Sleep(30 * time.Second)
					})

					Step(fmt.Sprintf("check if apps are running"), func() {
						ValidateContext(ctx)
					})

				})

				Step(fmt.Sprintf("recover all drives and the storage driver"), func() {
					for _, driveToFail := range drives {
						err = Inst().N.RecoverDrive(nodeWithDrive, driveToFail, busInfoMap[driveToFail], node.ConnectionOpts{
							Timeout:         driveFailTimeout,
							TimeBeforeRetry: defaultRetryInterval,
						})
						Expect(err).NotTo(HaveOccurred())
					}
					Step("wait for the drives to recover", func() {
						time.Sleep(30 * time.Second)
					})

					err = Inst().V.RecoverDriver(nodeWithDrive)
					Expect(err).NotTo(HaveOccurred())
				})

				Step(fmt.Sprintf("check if volume driver is up"), func() {
					err = Inst().V.WaitDriverUpOnNode(nodeWithDrive, Inst().DriverStartTimeout)
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})

		ValidateAndDestroy(contexts, nil)
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
