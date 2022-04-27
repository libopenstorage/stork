package tests

import (
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/ipv6util"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_ipv6.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Ipv6", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{IPv6PxctlFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context
	var nodes []node.Node
	var numNodes int

	BeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		nodes = node.GetWorkerNodes()
		numNodes = len(nodes)
	})

	Context("{IPv6PxctlCommands}", func() {
		var pxctlCmd string
		var pxctlCmdFull string
		var expectedIPCount int
		var ips []string

		// shared test function for pxctl commands
		testPxctlCmdForIPv6 := func() {
			It("has to run pxctl command and checks for valid ipv6 addresses", func() {
				var output string
				var err error

				Step(fmt.Sprintln("run pxctl command"), func() {
					output, err = Inst().V.GetPxctlCmdOutput(nodes[0], pxctlCmdFull)
					Expect(err).NotTo(HaveOccurred(), "unexpected error getting pxctl command output for running: %v, got output %v", pxctlCmdFull, output)
				})

				Step(fmt.Sprintln("parse address from pxctl command output"), func() {
					ips = ipv6util.ParseIPv6AddressInPxctlCommand(pxctlCmd, output, numNodes)
					Expect(len(ips)).To(Equal(expectedIPCount), "unexpected parsed ip count")
				})

				Step(fmt.Sprintln("validate the address are ipv6"), func() {
					isIpv6 := ipv6util.AreAddressesIPv6(ips)
					Expect(isIpv6).To(BeTrue(), "addresses in pxctl command output are expected to be IPv6, parsed ips: %v", ips)
				})
			})
		}

		// test ip address from pxctl status output
		Context("{PxctlStatusTest}", func() {
			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlStatus
				pxctlCmdFull = ipv6util.PxctlStatus
				// number of ips are the number of nodes + 1 (the node IP where the status command is run on)
				expectedIPCount = numNodes + 1
				testrailID = 9695443
			})
			testPxctlCmdForIPv6()
		})

		// test ip address from pxctl cluster list
		Context("{PxctlClusterList}", func() {
			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlClusterList
				pxctlCmdFull = ipv6util.PxctlClusterList
				expectedIPCount = numNodes
				testrailID = 9695444
			})
			testPxctlCmdForIPv6()
		})

		// test ip address from pxctl cluster inspect
		Context("{PxctlClusterInspect}", func() {
			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlClusterInspect
				pxctlCmdFull = fmt.Sprintf("%s %s", ipv6util.PxctlClusterInspect, nodes[0].Id)
				expectedIPCount = 2
				testrailID = 9695444
			})
			testPxctlCmdForIPv6()
		})

		// test ip address from pxctl cluster inspect
		Context("{PxctlVolumeCommands}", func() {
			var volumeID string
			var err error

			BeforeEach(func() {
				// create and attach volume for testing
				volumeID, err = Inst().V.CreateVolume(ipv6util.Ipv6VolumeName, 1000, 1)
				Expect(err).NotTo(HaveOccurred(), "failed to create volume")
				_, err = Inst().V.AttachVolume(volumeID)
				Expect(err).NotTo(HaveOccurred(), "Failed to attached volume")
			})

			AfterEach(func() {
				// clean up
				err := Inst().V.DetachVolume(volumeID)
				Expect(err).NotTo(HaveOccurred(), "failed to detach volume")
				err = Inst().V.DeleteVolume(volumeID)
				Expect(err).NotTo(HaveOccurred(), "failed to delete volume")
			})

			Context("{PxctlVolumeList", func() {
				JustBeforeEach(func() {
					pxctlCmd = ipv6util.PxctlVolumeList
					pxctlCmdFull = ipv6util.PxctlVolumeList
					expectedIPCount = 1
					testrailID = 9695445
				})
				testPxctlCmdForIPv6()
			})

			Context("{PxctlVolumeInspect", func() {
				JustBeforeEach(func() {
					pxctlCmd = ipv6util.PxctlVolumeInspect
					pxctlCmdFull = fmt.Sprintf("%s %s", ipv6util.PxctlVolumeInspect, volumeID)
					expectedIPCount = 2
					testrailID = 9695445
				})
				testPxctlCmdForIPv6()
			})

		})
	})

	AfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
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
