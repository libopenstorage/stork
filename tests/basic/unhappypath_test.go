package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"math"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/kvdbutils"
	"github.com/portworx/torpedo/pkg/snapshotutils"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

var (
	storkInst = stork.Instance()
)

const (
	dropPercentage      = 20
	delayInMilliseconds = 250
	//24 hours
	totalTimeInHours              = 24
	errorPersistTimeInMinutes     = 60 * time.Minute
	snapshotScheduleRetryInterval = 10 * time.Second
	snapshotScheduleRetryTimeout  = 5 * time.Minute
	waitTimeForPXAfterError       = 20 * time.Minute
)

// This test is to verify stability of the system when there is a network error on the system.
var _ = Describe("{NetworkErrorInjection}", func() {
	var testrailID = 3526435
	injectionType := "drop"
	//TODO need to fix this issue later.
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35264
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("NetworkErrorInjection", "Verify stability of the system when there is a network error", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("Inject network error while applications are running", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			log.Infof("Iteration number %d", i)
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("applicationscaleup-%d", i))...)
		}
		currentTime := time.Now()
		timeToExecuteTest := time.Now().Local().Add(time.Hour * time.Duration(totalTimeInHours))

		// Set Autofs trim
		currNode := node.GetWorkerNodes()[0]
		err := Inst().V.SetClusterOpts(currNode, map[string]string{
			"--auto-fstrim": "on",
		})
		if err != nil {
			err = fmt.Errorf("error while enabling auto fstrim, Error:%v", err)
			Expect(err).NotTo(HaveOccurred())
		}

		Step("Verify applications after deployment", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})
		Step("Create snapshot schedule policy", func() {
			createSnapshotSchedule(contexts)
		})

		for int64(timeToExecuteTest.Sub(currentTime).Seconds()) > 0 {
			// TODO core check
			log.Infof("Remaining time to test in minutes : %d ", int64(timeToExecuteTest.Sub(currentTime).Seconds()/60))
			Step("Set packet loss on random nodes ", func() {
				//Get all nodes and set eth0
				nodes := node.GetWorkerNodes()
				numberOfNodes := int(math.Ceil(float64(0.40) * float64(len(nodes))))
				selectedNodes := nodes[:numberOfNodes]
				//nodes []Node, errorInjectionType string, operationType string,
				//dropPercentage int, delayInMilliseconds int
				log.Infof("Set network error injection")
				Inst().N.InjectNetworkError(selectedNodes, injectionType, "add", dropPercentage, delayInMilliseconds)
				log.Infof("Wait %d minutes before checking px status ", errorPersistTimeInMinutes/(time.Minute))
				time.Sleep(errorPersistTimeInMinutes)
				hasPXUp := true
				for _, n := range nodes {
					log.Infof("Check PX status on %v", n.Name)
					err := Inst().V.WaitForPxPodsToBeUp(n)
					if err != nil {
						hasPXUp = false
						log.Errorf("PX failed to be in ready state  %v %s ", n.Name, err)
					}
				}
				if !hasPXUp {
					Expect(fmt.Errorf("PX is not ready on on or more nodes ")).NotTo(HaveOccurred())
				}
				log.Infof("Clear network error injection ")
				Inst().N.InjectNetworkError(selectedNodes, injectionType, "delete", 0, 0)
				//Get kvdb members and
				if injectionType == "drop" {
					injectionType = "delay"
				} else {
					injectionType = "drop"
				}
			})
			log.Infof("Wait %d minutes before checking application status ", waitTimeForPXAfterError/(time.Minute))
			time.Sleep(waitTimeForPXAfterError)
			Step("Verify application after clearing error", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})
			Step("Check KVDB memebers health", func() {
				nodes := node.GetWorkerNodes()
				kvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])
				if err != nil {
					err = fmt.Errorf("Error getting kvdb members using node %v. cause: %v", nodes[0].Name, err)
					Expect(err).NotTo(HaveOccurred())
				}
				err = kvdbutils.ValidateKVDBMembers(kvdbMembers)
				Expect(err).NotTo(HaveOccurred())
			})
			Step("Check Cloudsnap status ", func() {
				verifyCloudSnaps(contexts)
			})
			Step("Check for crash and verify crash was found before ", func() {
				//TODO need to add this method in future.
			})
			log.Infof("Wait  %d minutes before starting next iteration ", errorPersistTimeInMinutes/(time.Minute))
			time.Sleep(errorPersistTimeInMinutes)
			currentTime = time.Now()
		}
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// createSnapshotSchedule creating snapshot schedule
func createSnapshotSchedule(contexts []*scheduler.Context) {
	//Create snapshot schedule
	policyName := "intervalpolicy"
	interval := 30
	for _, ctx := range contexts {
		err := SchedulePolicy(ctx, policyName, interval)
		Expect(err).NotTo(HaveOccurred())
		if strings.Contains(ctx.App.Key, "cloudsnap") {
			appVolumes, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				Expect(err).NotTo(HaveOccurred())
			}
			if len(appVolumes) == 0 {
				err = fmt.Errorf("found no volumes for app %s", ctx.App.Key)
				log.Warnf("No appvolumes found")
				Expect(err).NotTo(HaveOccurred())
			}
		}
	}
}

// verifyCloudSnaps check cloudsnaps are taken on scheduled time.
func verifyCloudSnaps(contexts []*scheduler.Context) {
	for _, ctx := range contexts {
		appVolumes, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			log.Warnf("Error found while getting volumes %s ", err)
		}
		if len(appVolumes) == 0 {
			err = fmt.Errorf("found no volumes for app %s", ctx.App.Key)
			log.Warnf("No appvolumes found")
		}
		//Verify cloudsnap is continuing
		for _, v := range appVolumes {
			if strings.Contains(ctx.App.Key, "cloudsnap") == false {
				log.Warnf("Apps are not cloudsnap supported %s ", v.Name)
				continue
			}
			// Skip cloud snapshot trigger for Pure DA volumes
			isPureVol, err := Inst().V.IsPureVolume(v)
			if err != nil {
				log.Warnf("No pure volumes found in %s ", ctx.App.Key)
			}
			if isPureVol {
				log.Warnf("Cloud snapshot is not supported for Pure DA volumes: [%s]", v.Name)
				continue
			}
			snapshotScheduleName := v.Name + "-interval-schedule"
			log.Infof("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
			appNamespace := ctx.App.Key + "-" + ctx.UID
			log.Infof("Namespace : %v", appNamespace)

			err = snapshotutils.ValidateSnapshotSchedule(snapshotScheduleName, appNamespace)
			Expect(err).NotTo(HaveOccurred())
		}
	}
}

// SchedulePolicy
func SchedulePolicy(ctx *scheduler.Context, policyName string, interval int) error {
	if strings.Contains(ctx.App.Key, "cloudsnap") {
		log.Infof("APP with cloudsnap key available %v ", ctx.App.Key)
		schedPolicy, err := storkInst.GetSchedulePolicy(policyName)
		if err == nil {
			log.Infof("schedPolicy is %v already exists", schedPolicy.Name)
		} else {
			err = snapshotutils.SchedulePolicyInDefaultNamespace(policyName, interval, 2)
			Expect(err).NotTo(HaveOccurred())
		}
		log.Infof("Waiting for 10 mins for Snapshots to be completed")
		time.Sleep(10 * time.Minute)
	}
	return nil
}
