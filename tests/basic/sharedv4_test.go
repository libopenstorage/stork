package tests

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/libopenstorage/openstorage/pkg/mount"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	devicePathPrefix = "/dev/pxd/pxd"
)

var _ = Describe("{Sharedv4Functional}", func() {
	var testrailID, runID int
	var contexts, testSharedV4Contexts []*scheduler.Context
	var workers []node.Node
	var numPods int
	var namespacePrefix string
	var volumeMountRW = regexp.MustCompile(`,rw,|,rw|rw,|rw`)
	var volumeMountRO = regexp.MustCompile(`,ro,|,ro|ro,|ro`)

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		StartTorpedoTest("Sharedv4Functional", "Functional Test for Sharedv4", nil, testrailID)
		// Set up all apps
		contexts = nil
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}

		// Skip the test if there are no test-sharedv4 apps
		testSharedV4Contexts = getTestSharedV4Contexts(contexts)
		if len(testSharedV4Contexts) == 0 {
			Skip("No test-sharedv4 apps were found")
		}
		workers = node.GetWorkerNodes()
		numPods = len(workers)

		Step("scale the test-sharedv4 apps so that one pod runs on each worker node", func() {
			scaleApps(testSharedV4Contexts, numPods)
		})
		ValidateApplications(contexts)
	})

	Context("{SharedV4MountRecovery}", func() {
		BeforeEach(func() {
			namespacePrefix = "sharedv4mountrecovery"
		})

		JustBeforeEach(func() {
			for _, ctx := range testSharedV4Contexts {
				k8sApps := apps.Instance()
				// we are removing the anti-affinity rule to force the race condition in testing.
				// without anti-affinity rule, pods creating and terminating can happen at the same time, causing
				// pods can mount on vols that are about to be detached/unmounted. Anti-affinity rule
				// forced creating pods to wait for terminating pods due to one pod per node IP restriction.
				Step(
					fmt.Sprintf("setup app %s to remove anti-affinity rules", ctx.App.Key),
					func() {

						Step(fmt.Sprintf("update %s deployment without anti-affinity", ctx.App.Key), func() {
							deployments, err := k8sApps.ListDeployments(ctx.GetID(), metav1.ListOptions{})
							Expect(err).NotTo(HaveOccurred())
							deployment := deployments.Items[0]
							deployment.Spec.Template.Spec.Affinity = nil

							_, err = k8sApps.UpdateDeployment(&deployment)
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("scale down app: %s to 0 ", ctx.App.Key), func() {
							scaleApp(ctx, 0)
						})

						// wait until all pods are gone
						Step(fmt.Sprintf("wait for app %s to have 0 pods", ctx.App.Key), func() {
							waitForNumPodsToEqual(ctx, 0)
						})

						Step(fmt.Sprintf("scale the app back to numPods for %s", ctx.App.Key), func() {
							scaleApp(ctx, numPods)

							ValidateContext(ctx)
						})

					})
			}

		})

		It("should set device path to RO and validate recovery", func() {
			for _, ctx := range testSharedV4Contexts {
				var err error
				vol, apiVol, attachedNode := getSv4TestAppVol(ctx)
				counterCollectionInterval := 3 * time.Duration(numPods) * time.Second
				devicePath := fmt.Sprintf("%s%s", devicePathPrefix, apiVol.Id)

				// In porx, we check the export path is read-only when it is previously RO and
				// changed to RW by fs due to occuring error. Specifically, when mnt.Opts (default)
				// is RW and mnt.VfsOpts (fs state) is RO.
				// By setting device path to RO, we mock the same behavior. Check state in
				// `/proc/self/mountinfo`
				Step(fmt.Sprintf("mark device path as RO %s", ctx.App.Key), func() {
					setPathToROMode(devicePath, attachedNode)

					// check only mnt.VfsOpts (fs state) is RO
					mntList, err := mount.GetMounts()
					Expect(err).NotTo(HaveOccurred())
					for _, mnt := range mntList {
						if mnt.Mountpoint != devicePath {
							continue
						}
						Expect(volumeMountRW.MatchString(mnt.Opts)).To(BeTrue())
						Expect(volumeMountRO.MatchString(mnt.VfsOpts)).To(BeTrue())
					}
				})

				Step(fmt.Sprintf("validate the counters are inactive %s", ctx.App.Key), func() {
					counters := getAppCounters(apiVol, attachedNode, counterCollectionInterval)
					activePods := getActivePods(counters)
					Expect(len(activePods)).To(Equal(0))
				})

				Step(fmt.Sprintf("restart vol driver %s", ctx.App.Key), func() {
					restartVolumeDriverOnNode(attachedNode)
					ValidateContext(ctx)
					// updates the attachedNode; sometimes the volume is moved when restart to fix the readonly volume
					attachedNode, err = Inst().V.GetNodeForVolume(vol, cmdTimeout, cmdRetry)
					Expect(err).NotTo(HaveOccurred())
				})

				Step(fmt.Sprintf("validate counter are active for %s", ctx.App.Key), func() {
					var counters map[string]appCounter
					// we are seeing a case where the application is validated and running but sharedv4 volume is
					// not mounted/exported yet. adding the eventually here to retry to make sure we capture the
					// counters correctly. Setting timeout of 5 minutes for now as mounting and exporting
					// shouldn't take that long.
					Eventually(func() bool {
						counters = getAppCounters(apiVol, attachedNode, counterCollectionInterval)
						activePods := getActivePods(counters)
						return (len(activePods) == numPods)
					}, 5*time.Minute, 20*time.Second).Should(BeTrue(),
						"number of active keys did not match for volume %v (%v) for app %v. counters map: %v",
						vol.ID, apiVol.Id, ctx.App.Key, counters)
				})

				Step(fmt.Sprintf("validate device path is set as RW for %s", ctx.App.Key), func() {
					mntList, err := mount.GetMounts()
					Expect(err).NotTo(HaveOccurred())

					for _, mnt := range mntList {
						if mnt.Mountpoint != devicePath {
							continue
						}
						Expect(volumeMountRW.MatchString(mnt.Opts)).To(BeTrue())
						Expect(volumeMountRW.MatchString(mnt.VfsOpts)).To(BeTrue())
					}
				})
			}
		})
	})

	// Template for additional tests
	// Context("{}", func() {
	// 	BeforeEach(func() {
	// 		testrailID = 0
	//      namespacePrefix = ""
	// 	})
	//
	// 	JustBeforeEach(func() {
	//		// since the apps are deployed by JustBeforeEach in the outer block,
	// 		// any test-specific changes to the deployed apps should go here.
	// 	})
	//
	// 	It("", func() {
	// 		for _, ctx := range testSharedV4Contexts {
	// 		}
	// 	})

	// 	AfterEach(func() {
	// 	})
	// })

	AfterEach(func() {
		Step("destroy apps", func() {
			if CurrentGinkgoTestDescription().Failed {
				log.Info("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// returns the contexts that are running test-sharedv4* apps
func getTestSharedV4Contexts(contexts []*scheduler.Context) []*scheduler.Context {
	var testSharedV4Contexts []*scheduler.Context
	for _, ctx := range contexts {
		if !strings.HasPrefix(ctx.App.Key, "test-sharedv4") {
			continue
		}
		testSharedV4Contexts = append(testSharedV4Contexts, ctx)
	}
	return testSharedV4Contexts
}

// set the given path to ro
func setPathToROMode(path string, node *node.Node) {
	cmd := fmt.Sprintf("mount -oremount,ro %s", path)
	_, err := runCmd(cmd, *node)

	Expect(err).NotTo(HaveOccurred())
}
