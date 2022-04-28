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
	var output string
	var err error
	var ips []string

	BeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		nodes = node.GetWorkerNodes()
		numNodes = len(nodes)
	})

	Context("{IPv6PxctlCommands}", func() {
		var pxctlCmd string
		var pxctlCmdFull string
		var expectedIPCount int

		// shared test function for pxctl commands
		testPxctlCmdForIPv6 := func() {
			It("has to run pxctl command and checks for valid ipv6 addresses", func() {
				Step(fmt.Sprintln("run pxctl command"), func() {
					output, err = Inst().V.GetPxctlCmdOutput(nodes[0], pxctlCmdFull)
					Expect(err).NotTo(HaveOccurred(), "unexpected error getting pxctl command output for running: %v, got output %v", pxctlCmdFull, output)
				})

				Step(fmt.Sprintln("parse address from pxctl command output"), func() {
					ips, err = ipv6util.ParseIPv6AddressInPxctlCommand(pxctlCmd, output, numNodes)
					if expectedIPCount >= 0 { // negative 'expectedIPCount' is NA, so skip check
						Expect(len(ips)).To(Equal(expectedIPCount), "unexpected parsed ip count")
					}
					Expect(err).NotTo(HaveOccurred(), "IP parsing failed for %v", ips)
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

		// test ip address from pxctl service kvdb endpoints
		Context("{PxctlServiceKvdbEndpoints}", func() {

			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlServiceKvdbEndpoints
				pxctlCmdFull = ipv6util.PxctlServiceKvdbEndpoints
				expectedIPCount = -1
				testrailID = 9695435
			})
			testPxctlCmdForIPv6()
		})

		// test ip address from pxctl service kvdb members
		Context("{PxctlServiceKvdbMembers}", func() {

			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlServiceKvdbMembers
				pxctlCmdFull = ipv6util.PxctlServiceKvdbMembers
				expectedIPCount = -1
				testrailID = 9695435
			})
			testPxctlCmdForIPv6()
		})

		// test ip address from pxctl service kvdb Alerts
		Context("{IPv6PxctlAlertsDescription}", func() {

			JustBeforeEach(func() {
				pxctlCmd = ipv6util.PxctlAlertsShow
				pxctlCmdFull = fmt.Sprintf("%s -t node -i NodeStateChange -r %s", ipv6util.PxctlAlertsShow, nodes[1].VolDriverNodeID)
				expectedIPCount = -1
				testrailID = 9695446
			})

			It("has to check alerts description for valid IPv6 addresses", func() {
				Step(fmt.Sprintf("Stop storage on node %v\n", nodes[1].Name), func() {
					err := Inst().V.StopDriver([]node.Node{nodes[1]}, false, nil)
					Expect(err).NotTo(HaveOccurred(), "failed to stop node %v", nodes[1].Name)
				})
				Step(fmt.Sprintf("run pxctl alerts command for down volume %v and parse output\n", nodes[1].VolDriverNodeID), func() {
					Eventually(func() (string, error) {
						output, err = Inst().V.GetPxctlCmdOutput(nodes[0], pxctlCmdFull)
						Expect(err).NotTo(HaveOccurred(), "Failed to get alerts for down volume %v, %v", nodes[1].VolDriverNodeID, output)
						return output, err
					}, 45*time.Second, 5*time.Second).Should(ContainSubstring(ipv6util.OperationalStatusDown),
						"failed to get alerts down for resource %v", nodes[1].VolDriverNodeID)
				})

				Step(fmt.Sprintf("parse address from pxctl alerts command output for down volume %v\n", nodes[1].VolDriverNodeID), func() {
					ip, err := ipv6util.ParseIPAddressInPxctlResourceDownAlert(output, nodes[1].VolDriverNodeID)
					Expect(err).NotTo(HaveOccurred(), "failed to parse command output for down volume %v\n", nodes[1].VolDriverNodeID)
					ips = []string{ip}
				})

				Step(fmt.Sprintln("validate the alert resource address is IPv6"), func() {
					isIpv6 := ipv6util.AreAddressesIPv6(ips)
					Expect(isIpv6).To(BeTrue(), "address in pxctl alerts output for resource %v down is expected to be IPv6, parsed ips: %v",
						nodes[1].VolDriverNodeID, ips)
				})

				Step(fmt.Sprintf("start storage on node %v\n", nodes[1].Name), func() {
					err := Inst().V.StartDriver(nodes[1])
					Expect(err).NotTo(HaveOccurred(), "failed to start driver on node %v", nodes[1].Name)
				})

			})
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
