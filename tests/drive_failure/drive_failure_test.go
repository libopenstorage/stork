package tests

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	. "github.com/portworx/torpedo/tests"
)

func TestDriveFailure(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo: DriveFailure")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("Induce drive failure on of the nodes", func() {
	testName := "drivefailure"
	It("has to schedule apps and induce a drive failure on one of the nodes", func() {
		var err error
		contexts := ScheduleAndValidate(testName)

		Step("get nodes for all apps in test and induce drive failure on one of the nodes", func() {
			for _, ctx := range contexts {
				var (
					drives               []string
					appNodes             []node.Node
					nodeWithDrive        node.Node
					driveToFail, driveID string
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
					driveToFail = drives[0]
				})

				Step(fmt.Sprintf("induce a drive failure on %v on node %v", driveToFail, nodeWithDrive), func() {
					driveID, err = Inst().N.YankDrive(nodeWithDrive, driveToFail, node.ConnectionOpts{
						Timeout:         1 * time.Minute,
						TimeBeforeRetry: 5 * time.Second,
					})
					Expect(err).NotTo(HaveOccurred())

					Step("wait for the drive to fail", func() {
						time.Sleep(30 * time.Second)
					})

					Step(fmt.Sprintf("check if apps are running"), func() {
						ValidateContext(ctx)
					})

				})

				Step(fmt.Sprintf("recover drive and the storage driver"), func() {
					err = Inst().N.RecoverDrive(nodeWithDrive, driveToFail, driveID, node.ConnectionOpts{
						Timeout:         1 * time.Minute,
						TimeBeforeRetry: 5 * time.Second,
					})
					Expect(err).NotTo(HaveOccurred())

					err = Inst().V.RecoverDriver(nodeWithDrive)
					Expect(err).NotTo(HaveOccurred())
				})

				Step(fmt.Sprintf("check if volume driver is up"), func() {
					err = Inst().V.WaitForNode(nodeWithDrive)
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})

		Step("validate and destroy apps", func() {
			for _, ctx := range contexts {
				ValidateAndDestroy(ctx, nil)
			}
		})

	})
})

var _ = AfterSuite(func() {
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
