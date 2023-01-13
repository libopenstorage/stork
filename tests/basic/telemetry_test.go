package tests

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	"github.com/libopenstorage/openstorage/pkg/dbg"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

const (
	telemetryCmdRetry      = 5 * time.Second
	telemetryCmdTimeout    = 15 * time.Second
	TelemetryEnabledStatus = "100"
)

var (
	telemetryCmdConnectionOpts = node.ConnectionOpts{
		Timeout:         telemetryCmdTimeout,
		TimeBeforeRetry: telemetryCmdRetry,
		Sudo:            false,
	}
	isTelemetryOperatorEnabled = false
	oneTimeInitDone            = false
)

// Taken from SharedV4 tests...
func telemetryRunCmd(cmd string, n node.Node, cmdConnectionOpts *node.ConnectionOpts) (string, error) {
	if cmdConnectionOpts == nil {
		cmdConnectionOpts = &telemetryCmdConnectionOpts
	}
	output, err := Inst().N.RunCommandWithNoRetry(n, cmd, *cmdConnectionOpts)
	return output, err
}

func runPxctlCommand(pxctlCmd string, n node.Node, cmdConnectionOpts *node.ConnectionOpts) (string, error) {
	if cmdConnectionOpts == nil {
		cmdConnectionOpts = &telemetryCmdConnectionOpts
	}

	output, err := Inst().V.GetPxctlCmdOutputConnectionOpts(n, pxctlCmd, *cmdConnectionOpts, false)
	return output, err
}

func TelemetryEnabled(currNode node.Node) bool {
	// This returns true if telemetry is enabled
	output, err := runPxctlCommand("status | egrep ^Telemetry:", currNode, nil)
	Expect(err).NotTo(HaveOccurred(), "Failed to get status for node %v", currNode.Name)
	log.Infof("node %s: %s", currNode.Name, output)
	status, err := regexp.MatchString(`Telemetry:.*Healthy`, output)
	if err != nil {
		return false
	}
	return status
}

func oneTimeInit() {
	if oneTimeInitDone {
		return
	}
	log.Infof("Checking telemetry status...")
	isOpBased, _ := Inst().V.IsOperatorBasedInstall()
	if isOpBased {
		spec, err := Inst().V.GetDriver()
		Expect(err).ToNot(HaveOccurred())
		if spec.Spec.Monitoring != nil && spec.Spec.Monitoring.Telemetry != nil && spec.Spec.Monitoring.Telemetry.Enabled {
			log.Infof("Telemetry is operator enabled.")
			isTelemetryOperatorEnabled = true
		}
	}
	if !isTelemetryOperatorEnabled {
		log.Infof("Telemetry is not enabled.")
	}
	oneTimeInitDone = true
}

// This test telemetry health via pxctl
var _ = Describe("{DiagsTelemetryPxctlHealthyStatus}", func() {
	var contexts []*scheduler.Context
	var runID int

	testrailID := 54907

	BeforeEach(func() {
		oneTimeInit()
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("DiagsTelemetryPxctlHealthyStatus", "Validate telemetry health", nil, testrailID)
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})

	It("Validate, pxctl displays telemetry status", func() {
		contexts = make([]*scheduler.Context, 0)
		telemetryNodeStatus := make(map[string]bool)

		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("run pxctl status to check telemetry status on node %v", currNode.Name), func() {
				telemetryNodeStatus[currNode.Name] = TelemetryEnabled(currNode)
			})
		}
		for nodeName, isHealthy := range telemetryNodeStatus {
			Expect(isHealthy).To(BeTrue(), "Telemetry not health on node %v", nodeName)
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{DiagsBasic}", func() {
	var contexts []*scheduler.Context

	BeforeEach(func() {
		oneTimeInit()
		StartTorpedoTest("DiagsBasic", "Perform basic test on diags", nil, 0)
	})

	It("has to setup, validate, try to get diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("diagsasyncbasic-%d", i))...)
		}

		ValidateApplications(contexts)
		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s | %s", currNode.Name, currNode.Type), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/%s-diags-%s.tar.gz", currNode.Name, dbg.GetTimeStamp()),
					ContainerName: "",
					OnHost:        true,
					Live:          true,
				}
				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true})
				Expect(err).NotTo(HaveOccurred())
			})
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test performs basic diags collection and validates them on S3 bucket
var _ = Describe("{DiagsCCMOnS3}", func() {
	var testrailIDs = []int{54917, 54912, 54910}
	BeforeEach(func() {

		oneTimeInit()
	})

	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54917
	var runIDs []int
	JustBeforeEach(func() {
		StartTorpedoTest("DiagsCCMOnS3", "Validate telemetry pushed to s3", nil, 0)
		for _, testRailID := range testrailIDs {
			runIDs = append(runIDs, testrailuttils.AddRunsToMilestone(testRailID))
		}
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})
	var contexts []*scheduler.Context
	It("has to setup, validate, try to get diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s | %s", currNode.Name, currNode.Type), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/%s-diags-%s.tar.gz", currNode.Name, dbg.GetTimeStamp()),
					ContainerName: "",
					OnHost:        true,
					Live:          true,
				}
				if !TelemetryEnabled(currNode) {
					log.Debugf("Telemetry not enabled, sleeping for 5 mins")
					time.Sleep(5 * time.Minute)
				}
				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: false})
				Expect(err).NotTo(HaveOccurred(), "Diags collected successfully")
				if TelemetryEnabled(currNode) {
					err = Inst().V.ValidateDiagsOnS3(currNode, path.Base(strings.TrimSpace(config.OutputFile)))
					Expect(err).NotTo(HaveOccurred(), "Diags validated on S3")
				} else {
					log.Debugf("Telemetry not enabled on %s, skipping test", currNode.Name)
				}
			})
		}
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		for i, testRailID := range testrailIDs {
			AfterEachTest(contexts, testRailID, runIDs[i])
		}
	})
})

var _ = Describe("{ProfileOnlyDiags}", func() {
	var testrailID = 54911
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54917
	var runID int

	BeforeEach(func() {
		oneTimeInit()
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("ProfileOnlyDiags", "Validate telemtry for profile only diags", nil, testrailID)
	})
	var contexts []*scheduler.Context
	var diagsFiles []string
	var existingDiags string
	var newDiags string
	var err error
	var pxInstalled bool
	var skipTest = false

	It("has to collect, validate profile diags on S3", func() {
		contexts = make([]*scheduler.Context, 0)
		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			pxInstalled, err = Inst().V.IsDriverInstalled(currNode)
			if err != nil {
				log.Debugf("Could not get PX status on %s", currNode.Name)
			}
			if pxInstalled {
				// Get the most recent profile diags for comparison
				Step(fmt.Sprintf("Check latest profile diags on node %v", currNode.Name), func() {
					log.Infof(" Getting latest profile  diags on %v", currNode.Name)
					existingDiags, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/*-*.{stack,heap}.gz | head -n 2"),
						currNode, nil)
					if err == nil {
						log.Infof("Found latest profiles diags on node %s:\n%s ", currNode.Name, existingDiags)
					} else {
						existingDiags = ""
					}
				})
				// Issue a profile diags to generate new files.
				Step(fmt.Sprintf("collect profile diags on node: %s | %s", currNode.Name, currNode.Type), func() {
					config := &torpedovolume.DiagRequestConfig{
						DockerHost: "unix:///var/run/docker.sock",
						Profile:    true,
					}
					if TelemetryEnabled(currNode) {
						err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true})
						if err != nil {
							log.Errorf("Failed to collect diags on Node: %s Err: %v", currNode.Name, err)
						}
						Expect(err).NotTo(HaveOccurred(), "Profile only diags collected successfully")
					} else {
						log.Infof("Telemetry not enabled on the node %s", currNode.Name)
						skipTest = true
						return
					}
				})
				// This sleep is required because the heap and stack logs might not have been written at same time.
				time.Sleep(10 * time.Second)
				// Get the latest files in the directory.  The newly generated files will not equal the most recent. So you know they are new.
				Step(fmt.Sprintf("Get the new profile diags on node %v", currNode.Name), func() {
					if skipTest {
						log.Infof("Skipping getting new filename for node %s", currNode.Name)
						return
					}
					log.Infof("Getting latest profile diags on %66v", currNode.Name)
					Eventually(func() bool {
						newDiags, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/*-*.{stack,heap}.gz | head -n 2"),
							currNode, nil)
						if err == nil {
							if existingDiags != "" && existingDiags == newDiags {
								log.Infof("No new profile diags found... current latest ones are %v", newDiags)
								newDiags = ""
							}
							if len(newDiags) > 0 {
								log.Infof("Found new profile diags:\n%s", newDiags)
								// Needs to contain both stack/heap
								if strings.Contains(newDiags, ".heap") && strings.Contains(newDiags, ".stack") {
									diagsFiles = strings.Split(newDiags, "\n")
									log.Debugf("Files found %v", diagsFiles)
									return true
								}
							}
						}
						return false
					}, 5*time.Minute, 15*time.Second).Should(BeTrue(), "failed to generate profile diags on node %s", currNode.Name)

				})
				Step(fmt.Sprintf("Validate diags uploaded on S3"), func() {
					for _, file := range diagsFiles {
						fileNameToCheck := path.Base(file)
						log.Debugf("Validating file %s", fileNameToCheck)
						if !skipTest { // This is done in case the system is run without telemetry.
							err := Inst().V.ValidateDiagsOnS3(currNode, fileNameToCheck)
							if err != nil {
								log.Errorf("Failed to validate diags: %v", err)
							}
							Expect(err).NotTo(HaveOccurred(), "Files validated on s3")
						} else {
							log.Debugf("Telemetry not enabled on %s, skipping test", currNode.Name)
						}
					}
				})
			} else {
				log.Debugf("Px not enabled on node %s", currNode.Name)
			}
		}
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Runs cluster wide diags collection and validates on S3
var _ = Describe("{DiagsClusterWide}", func() {
	var testrailID = 54916
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54916
	var runID int
	BeforeEach(func() {
		oneTimeInit()
		StartTorpedoTest("DiagsClusterWide", "Validate cluster wide diags", nil, testrailID)
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})
	var contexts []*scheduler.Context
	var diagFile string
	var err error
	It("has to collect diags on entire cluster, validate diags on S3", func() {
		contexts = make([]*scheduler.Context, 0)
		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("run pxctl sv diags to collect cluster wide diags  %v", currNode.Name), func() {
				_, err := runPxctlCommand("sv diags -a -c", currNode, nil)
				Expect(err).NotTo(HaveOccurred(), "Error running diags on Node: %s", currNode.Name)
			})
			Step(fmt.Sprintf("Get the svc diags collected above %s", currNode.Name), func() {
				log.Infof("Getting latest svc diags on %66v", currNode.Name)
				diagFile, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/%s-*.tar.gz | head -n 1", currNode.Name), currNode, nil)
				if err != nil {
					log.Fatalf("Error in getting cluster wide diags files on: %s, err: %v", currNode.Name, err)
				}
			})
			Step(fmt.Sprintf("Validate diags uploaded on S3"), func() {
				fileNameToCheck := path.Base(strings.TrimSuffix(diagFile, "\n"))
				log.Debugf("Validating file %s", fileNameToCheck)
				if TelemetryEnabled(currNode) {
					err := Inst().V.ValidateDiagsOnS3(currNode, fileNameToCheck)
					Expect(err).NotTo(HaveOccurred(), "Files validated on s3")
				} else {
					log.Debugf("Telemetry not enabled on %s, skipping test", currNode.Name)
				}
			})
			break
		}
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{DiagsAsyncBasic}", func() {
	var contexts []*scheduler.Context

	BeforeEach(func() {
		StartTorpedoTest("DiagsAsyncBasic", "Async diags collection test", nil, 0)
		oneTimeInit()
	})

	It("has to setup, validate, try to get a-sync diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("diagsasyncbasic-%d", i))...)
		}

		ValidateApplications(contexts)

		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s", currNode.Name), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/%s-diags-%s.tar.gz", currNode.Name, dbg.GetTimeStamp()),
					ContainerName: "",
					OnHost:        true,
				}
				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true, Async: true})

				Expect(err).NotTo(HaveOccurred())
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test auto diags on storage crash
var _ = Describe("{DiagsAutoStorage}", func() {
	var contexts []*scheduler.Context
	var existingDiags string
	var pxProcessNm string
	var newDiags string
	var err error

	testProcNmsTestRailIDs := map[string]int{
		"px-storage": 54922,
		"px":         54923,
	}

	runIDs := map[int]int{}

	BeforeEach(func() {
		oneTimeInit()
	})

	JustBeforeEach(func() {
		StartTorpedoTest("DiagsAutoStorage", "Diags auto storage test", nil, 0)
		for _, testRailID := range testProcNmsTestRailIDs {
			runIDs[testRailID] = testrailuttils.AddRunsToMilestone(testRailID)
		}
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})

	It("has to setup, validate, try to collect auto diags on nodes after px-storage/px crash", func() {
		contexts = make([]*scheduler.Context, 0)

		for pxProcessNm = range testProcNmsTestRailIDs {
			Step(fmt.Sprintf("Reset portworx for auto diags collect test after '%s' crash\n", pxProcessNm), func() {
				for _, currNode := range node.GetWorkerNodes() {
					// Restart portworx to reset auto diags interval
					err := Inst().V.StopDriver([]node.Node{currNode}, false, nil)
					Expect(err).NotTo(HaveOccurred(), "'%s' reset: failed to stop node %v", pxProcessNm, currNode.Name)
					err = Inst().V.StartDriver(currNode)
					Expect(err).NotTo(HaveOccurred(), "'%s' reset: failed to stop node %v", pxProcessNm, currNode.Name)
					log.Infof("Wait for driver to start on %v...", currNode.Name)
					err = Inst().V.WaitDriverUpOnNode(currNode, Inst().DriverStartTimeout)
					Expect(err).NotTo(HaveOccurred())
				}
			})
			// One node at a time, collect diags and verify in S3
			for _, currNode := range node.GetWorkerNodes() {
				Step(fmt.Sprintf("'%s': Check latest auto diags on node %v", pxProcessNm, currNode.Name), func() {
					_, err = telemetryRunCmd("ls -d /var/cores/auto", currNode, nil)
					if err == nil {
						log.Infof("'%s': Getting latest auto  diags on %v", pxProcessNm, currNode.Name)
						existingDiags, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/auto/%s*.tar.gz | head -n 1", currNode.Name), currNode, nil)
						if err == nil {
							log.Infof("'%s': Found latest auto diags on node %s: %s ",
								pxProcessNm, currNode.Name, path.Base(existingDiags))
						} else {
							existingDiags = ""
						}
					}
				})
				Step(fmt.Sprintf("'%s': Stop storage on node %v", pxProcessNm, currNode.Name), func() {
					_, err = telemetryRunCmd(fmt.Sprintf("pkill -9 %s", pxProcessNm), currNode, nil) // force stop
					Expect(err).NotTo(HaveOccurred(), "'%s' reset: failed to stop storage on node %v", pxProcessNm, currNode.Name)
					time.Sleep(1 * time.Second)
				})
				Step(fmt.Sprintf("'%s': run pxctl status to check when the server has gone down on %v",
					pxProcessNm, currNode.Name), func() {
					Eventually(func() (string, error) {
						output, err := runPxctlCommand("status | egrep ^PX", currNode, nil)
						return output, err
					}, 45*time.Second, 1*time.Second).Should(ContainSubstring("PX is not running on this host"),
						"'%s': failed to forcefully stop driver on node %s", pxProcessNm, currNode.Name)
				})
				Step(fmt.Sprintf("'%s': Get new auto diags on node %v", pxProcessNm, currNode.Name), func() {
					Eventually(func() bool {
						newDiags, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/auto/%s*.tar.gz | head -n 1", currNode.Name), currNode, nil)
						if err == nil {
							if existingDiags != "" && existingDiags == newDiags {
								log.Infof("'%s': No new auto diags found...", pxProcessNm)
								newDiags = ""
							}
							if len(newDiags) > 0 {
								log.Infof("'%s': Found new auto diags %s", pxProcessNm, newDiags)
								return true
							}
						}
						return false
					}, 5*time.Minute, 15*time.Second).Should(BeTrue(), "'%s': failed to generate auto diags on node %s",
						pxProcessNm, currNode.Name)
				})
				/// Need to validate new auto diags
				err = Inst().V.ValidateDiagsOnS3(currNode, path.Base(strings.TrimSpace(newDiags)))
				Expect(err).NotTo(HaveOccurred())
			}
			driverVersion, err := Inst().V.GetDriverVersion()
			if err != nil {
				driverVersion = "Error in getting driver version"
				log.Errorf(driverVersion)
			}
			testRailID := testProcNmsTestRailIDs[pxProcessNm]
			testrailObject := testrailuttils.Testrail{
				Status:        "Pass",
				TestID:        testRailID,
				RunID:         runIDs[testRailID],
				DriverVersion: driverVersion,
			}
			testrailuttils.AddTestEntry(testrailObject)
		}
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		testRailID := testProcNmsTestRailIDs[pxProcessNm]
		AfterEachTest(contexts, testRailID, runIDs[testRailID])
	})
})

// Stop driver and run diags
var _ = Describe("{DiagsOnStoppedPXnode}", func() {
	var contexts []*scheduler.Context
	var diagsValErr error
	var diagsErr error
	var runID int

	testrailID := 54918

	BeforeEach(func() {
		oneTimeInit()
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("DiagsOnStoppedPXnode", "Diags test on a node where PX is stopped", nil, 0)
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})

	It("Validate, pxctl displays telemetry status", func() {
		contexts = make([]*scheduler.Context, 0)

		Step(fmt.Sprintf("Stop portworx on all nodes..."), func() {
			for _, currNode := range node.GetWorkerNodes() {
				// Stop portworx
				err := Inst().V.StopDriver([]node.Node{currNode}, false, nil)
				Expect(err).NotTo(HaveOccurred(), "failed to stop node %v", currNode.Name)
			}
		})

		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s | %s", currNode.Name, currNode.Type), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/%s-diags-%s.tar.gz", currNode.Name, dbg.GetTimeStamp()),
					ContainerName: "",
					OnHost:        true,
					Live:          true,
				}
				diagsErr = Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: false, PxStopped: true})
				if diagsErr == nil {
					diagsValErr = Inst().V.ValidateDiagsOnS3(currNode, path.Base(strings.TrimSpace(config.OutputFile)))
				}
			})
		}

		Step(fmt.Sprintf("Restart portworx on all the nodes..."), func() {
			for _, currNode := range node.GetWorkerNodes() {
				// Start portworx
				err := Inst().V.StartDriver(currNode)
				Expect(err).NotTo(HaveOccurred(), "failed to stop node %v", currNode.Name)
			}
		})

		// Check errors after restarting PX
		Expect(diagsErr).NotTo(HaveOccurred(), "failed to collect Diags successfully")
		Expect(diagsValErr).NotTo(HaveOccurred(), "diags not validated on S3")

		Step(fmt.Sprintf("Check portworx restart on all the nodes..."), func() {
			for _, currNode := range node.GetWorkerNodes() {
				log.Infof("Wait for driver to start on %v...", currNode.Name)
				err := Inst().V.WaitDriverUpOnNode(currNode, Inst().DriverStartTimeout)
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Runs cluster wide diags collection and validates on S3
var _ = Describe("{DiagsSpecificNode}", func() {
	var testrailID = 54915
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/54916
	var runID int

	BeforeEach(func() {
		oneTimeInit()
	})

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("DiagsSpecificNode", "Diags test on a specific node", nil, testrailID)
		if !isTelemetryOperatorEnabled {
			Skip("Skip test because telemetry is not enabled...")
		}
	})
	var contexts []*scheduler.Context
	var diagFile string
	var existingDiags string
	var err error

	It("has to collect diags on specific node from another node, validate diags on S3", func() {
		contexts = make([]*scheduler.Context, 0)
		nodes := node.GetWorkerNodes()
		currNode := nodes[0]
		diagNode := nodes[1]

		Step(fmt.Sprintf("Check latest diags on node %v", diagNode.Name), func() {
			log.Infof("Getting latest diags on %v", diagNode.Name)
			existingDiags, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/%s*.tar.gz | head -n 1", diagNode.Name), diagNode, nil)
			if err == nil {
				log.Infof("Found latest auto diags on node %s: %s ", diagNode.Name, path.Base(existingDiags))
			} else {
				existingDiags = ""
			}
		})

		Step(fmt.Sprintf("run pxctl sv diags on node %s to collect diags on specific node %v", currNode.Name, diagNode.Name), func() {
			_, err := runPxctlCommand(fmt.Sprintf("sv diags -a -n %s", diagNode.VolDriverNodeID), currNode, nil)
			Expect(err).NotTo(HaveOccurred(), "Error running diags on Node %s to %s", currNode.Name, diagNode.Name)
		})

		Step(fmt.Sprintf("Get new diags on node %v", diagNode.Name), func() {
			diagFile, err = telemetryRunCmd(fmt.Sprintf("ls -t /var/cores/%s-*.tar.gz | head -n 1", diagNode.Name), diagNode, nil)
			Expect(err).NotTo(HaveOccurred(), "Error getting new diags on Node %s", diagNode.Name)
			if existingDiags != diagFile {
				log.Infof("Found new diags %s", diagFile)
				/// Need to validate new diags
				err = Inst().V.ValidateDiagsOnS3(diagNode, path.Base(strings.TrimSpace(diagFile)))
				Expect(err).NotTo(HaveOccurred())
			} else {
				err = fmt.Errorf("Failed to find new diags on Node %s", diagNode.Name)
			}
			Expect(err).NotTo(HaveOccurred())
		})

		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
