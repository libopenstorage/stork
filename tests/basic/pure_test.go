package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/units"

	"math/rand"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/osutils"
	v1 "k8s.io/api/core/v1"

	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/pureutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
)

const (
	secretNamespace = "kube-system"

	// fbS3CredentialName is the name of the credential object created in pxctl
	// see also formattingPxctlEstablishBackupCredential
	fbS3CredentialName = "fbS3bucket"

	// formattingPxctlEstablishBackupCredential is the command template used to
	// create the S3 credentials object in Portworx
	formattingPxctlEstablishBackupCredential = "pxctl credentials create --provider s3 --s3-access-key %s --s3-secret-key %s --s3-region us-east-1 --s3-endpoint %s --s3-storage-class STANDARD %s"

	// formattingPxctlDeleteFBBackupCredential is the command template used to
	// delete the S3 credentials object in Portworx
	formattingPxctlDeleteFBBackupCredential = "pxctl credentials delete %s"
)

func createCloudsnapCredential() {
	fbConfigs, err := pureutils.GetS3Secret(secretNamespace)
	Expect(err).NotTo(HaveOccurred())
	nodes := node.GetStorageDriverNodes()
	_, err = Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlEstablishBackupCredential, fbConfigs.Blades[0].S3AccessKey, fbConfigs.Blades[0].S3SecretKey, fbConfigs.Blades[0].ObjectStoreEndpoint, fbS3CredentialName), node.ConnectionOpts{
		Timeout:         k8s.DefaultTimeout,
		TimeBeforeRetry: k8s.DefaultRetryInterval,
		Sudo:            true,
	})
	// if the cloudsnap credentials already exist, just leave them there
	if err != nil && strings.Contains(err.Error(), "already exist") {
		err = nil
	}
	Expect(err).NotTo(HaveOccurred(), "unexpected error creating cloudsnap credential")
}

func deleteCloudsnapCredential() {
	nodes := node.GetStorageDriverNodes()
	_, err := Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlDeleteFBBackupCredential, fbS3CredentialName), node.ConnectionOpts{
		Timeout:         k8s.DefaultTimeout,
		TimeBeforeRetry: k8s.DefaultRetryInterval,
		Sudo:            true,
	})
	Expect(err).NotTo(HaveOccurred(), "unexpected error deleting cloudsnap credential")
}

// This test performs basic tests making sure Pure direct access are running as expected
var _ = Describe("{PureVolumeCRUDWithSDK}", func() {
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("PureVolumeCRUDWithSDK", "Test pure volumes on applications, run CRUD", nil, 0)
	})

	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		Step("setup credential necessary for cloudsnap", createCloudsnapCredential)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPureSDK(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
		Step("delete credential used for cloudsnap", deleteCloudsnapCredential)
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test performs basic tests making sure Pure direct access volumes are running as expected
var _ = Describe("{PureVolumeCRUDWithPXCTL}", func() {
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("PureVolumeCRUDWithPXCTL", "Test pure volumes on applications, run CRUD using pxctl", nil, 0)
	})
	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		Step("setup credential necessary for cloudsnap", createCloudsnapCredential)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPurePxctl(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
		Step("delete credential used for cloudsnap", deleteCloudsnapCredential)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test validates that, on an FACD installation, drives are located
// on the correct arrays that match their zone.
var _ = Describe("{PureFACDTopologyValidateDriveLocations}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PureFACDTopologyValidateDriveLocations", "Test that FACD cloud drive volumes are located on proper FlashArrays", nil, 0)
	})
	It("installs with cloud drive volumes on the correct FlashArrays", func() {
		err := ValidatePureCloudDriveTopologies()
		Expect(err).NotTo(HaveOccurred(), "unexpected error validating Pure cloud drive topologies")
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// this tests brings up large number of pods on multiple namespaces and validate if there is not PANIC or nilpointer exceptions
var _ = Describe("{BringUpLargePodsVerifyNoPanic}", func() {
	/*
				https://portworx.atlassian.net/browse/PTX-18792
			    https://portworx.atlassian.net/browse/PTX-17723

				PWX :
				https://portworx.atlassian.net/browse/PWX-32190

				Bug Description :
					PX is hitting `panic: runtime error: invalid memory address or nil pointer dereference`
		when creating 250 FADA volumes

				1. Deploying nginx pods using two FADA volumes in 125 name-space simultaneously
				2. After that verify if any panic in the logs due to nil pointer deference.
	*/
	var testrailID = 0
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("BringUpLargePodsVerifyNoPanic",
			"Validate no panics when creating more number of pods on "+
				"FADA/Generic Volumes while kvdb failover in progress", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Validate no panics when creating more number of pods on FADA/Generic " +
		"Volumes while kvdb failover in progress"
	It(stepLog, func() {
		/*
			NOTE : In order to verify https://portworx.atlassian.net/browse/PWX-32190 , please use nginx-fa-davol
				please use provisioner as portworx.PortworxCsi and storage-device to pure and application as nginx-fa-davol
			e.x : --app-list nginx-fa-davol --provisioner csi --storage-driver pure
		*/

		//https://portworx.atlassian.net/browse/PWX-33551
		err := UpdateDriverVariables(map[string]string{"PURE_REST_TIMEOUT": "60"}, map[string]string{"execution_timeout_sec": "180"})
		log.FailOnError(err, "error update storage cluster spec with env variables")

		var wg sync.WaitGroup
		var terminate bool = false

		log.InfoD("Failover kvdb in parallel while volume creation in progress")
		go func() {
			defer GinkgoRecover()
			for {
				if terminate == true {
					break
				}
				// Wait for KVDB Members to be online
				log.FailOnError(WaitForKVDBMembers(), "failed waiting for KVDB members to be active")

				// Kill KVDB Master Node
				masterNode, err := GetKvdbMasterNode()
				log.FailOnError(err, "failed getting details of KVDB master node")

				log.InfoD("killing kvdb master node with Name [%v]", masterNode.Name)

				// Get KVDB Master PID
				pid, err := GetKvdbMasterPID(*masterNode)
				log.FailOnError(err, "failed getting PID of KVDB master node")

				log.InfoD("KVDB Master is [%v] and PID is [%v]", masterNode.Name, pid)

				// Kill kvdb master PID for regular intervals
				log.FailOnError(KillKvdbMemberUsingPid(*masterNode), "failed to kill KVDB Node")

				// Wait for some time after killing kvdb master Node
				time.Sleep(5 * time.Minute)
			}
		}()

		contexts = make([]*scheduler.Context, 0)

		// Apps list provided by user while triggering the test is considered to run the apps in parallel
		totalAppsRequested := Inst().AppList

		parallelThreads := 5
		scheduleCount := 1
		if len(totalAppsRequested) > 0 {
			for _, eachApp := range totalAppsRequested {
				if eachApp == "nginx-fa-davol" {
					if strings.ToLower(Inst().Provisioner) != fmt.Sprintf("%v", portworx.PortworxCsi) {
						log.FailOnError(fmt.Errorf("need csi provisioner to run the test , "+
							"please pass --provisioner csi "+
							"or -e provisioner=csi in the arguments"), "csi provisioner enabled?")
					}
					parallelThreads = 15
					scheduleCount = 20
				}
			}
		}

		// if app list is more than 5 we run 1 application in one point of time in parallel,
		// intention here is to run 20 applications in parallel, In any point of time max pod count doesn't exceed more than 300
		var appThreads int
		if len(totalAppsRequested) >= 5 {
			appThreads = 1
		} else {
			appThreads = parallelThreads / len(totalAppsRequested)
		}

		wg.Add(appThreads)
		scheduleAppParallel := func() {
			defer wg.Done()
			defer GinkgoRecover()
			id := uuid.New()
			nsName := fmt.Sprintf("%s", id.String()[:4])
			for i := 0; i < scheduleCount; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf(fmt.Sprintf("largenumberpods-%v-%d", nsName, i)))...)
			}
		}

		teardownContext := func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		}

		// Create apps in parallel
		for count := 0; count < appThreads; count++ {
			go scheduleAppParallel()
			time.Sleep(500 * time.Millisecond)
		}
		wg.Wait()

		allVolumes := []*volume.Volume{}
		for _, eachContext := range contexts {
			vols, err := Inst().S.GetVolumes(eachContext)
			if err != nil {
				log.Errorf("Failed to get app %s's volumes", eachContext.App.Key)
			}
			for _, eachVol := range vols {
				allVolumes = append(allVolumes, eachVol)
			}
		}

		// Funciton to validate nil pointer dereference errors
		validateNilPointerErrors := func() {
			terminate = true
			// we validate negative scenario here , function returns true if nil pointer exception is seen.
			errors := []string{}
			for _, eachNode := range node.GetStorageNodes() {
				status, output, Nodeerr := VerifyNilPointerDereferenceError(&eachNode)
				if status == true {
					log.Infof("nil pointer dereference error seen on the Node [%v]", eachNode.Name)
					log.Infof("error log [%v]", output)
					errors = append(errors, fmt.Sprintf("[%v]", eachNode.Name))
				} else if Nodeerr != nil && output == "" {
					// we just print error in case if found one
					log.InfoD(fmt.Sprintf("[%v]", Nodeerr))
				}
			}
			if len(errors) > 0 {
				log.FailOnError(fmt.Errorf("nil pointer dereference panic seen on nodes [%v]", errors),
					"nil pointer de-reference error?")
			}
		}

		// Delete all the applications
		defer teardownContext()

		// Check for nilPointer de-reference error on the nodes.
		defer validateNilPointerErrors()

		// Waiting for all pods to become ready and in running state
		waitForPodsRunning := func() (interface{}, bool, error) {
			for _, eachContext := range contexts {
				log.Infof("Verifying Context [%v]", eachContext.App.Key)
				err := Inst().S.WaitForRunning(eachContext, 5*time.Minute, 2*time.Second)
				if err != nil {
					return nil, true, err
				}
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(waitForPodsRunning, 60*time.Minute, 10*time.Second)
		log.FailOnError(err, "Error checking pool rebalance")

		for _, eachVol := range allVolumes {
			log.InfoD("Validating Volume Status of Volume [%v]", eachVol.ID)
			status, err := IsVolumeStatusUP(eachVol)
			if err != nil {
				log.FailOnError(err, "error validating volume status")
			}
			dash.VerifyFatal(status == true, true, "is volume status up ?")
			terminate = true
		}

		terminate = true
		log.Info("all pods are up and in running state")
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test validates volume token timeout for FADA-volumes
var _ = Describe("{FADAVolTokenTimout}", func() {
	/*
					https://portworx.atlassian.net/browse/PTX-18941

					PWX :
					https://portworx.atlassian.net/browse/PWX-33632

					Bug Description :
						The current token manager timeout is 3 minutes. This is not sufficient for FADA volumes since as part of FADA operations a REST call is made to FA.
		                We have seen slowness in these APIs taking upto 15s to complete. This causes the token timeout to hit and PX to panic.

			1. Deploying nginx pods using two FADA volumes with volume placement strategy, creating volumes on  node-1
		    2. Deploy nginx pods using two FADA volumes creating 40 volumes at same time on the node-1
		    3. After that verify volumes are created successfully
	*/
	JustBeforeEach(func() {

		StartTorpedoTest("FADAVolTokenTimout", "Validate FADA volumes token timeout when multiple requests hit same node at same time", nil, 0)
	})
	stepLog := "Deploy and attach multiple FADA volumes on the same node and validate token request crash"
	It(stepLog, func() {
		log.InfoD(stepLog)
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())
		storageNodes := node.GetStorageNodes()
		selectedNode := storageNodes[rand.Intn(len(storageNodes))]
		var err error
		defer func() {
			Inst().AppList = applist
			err = Inst().S.RemoveLabelOnNode(selectedNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
		}()
		Inst().AppList = []string{"nginx-fada-repl-vps"}
		err = Inst().S.AddLabelOnNode(selectedNode, k8s.NodeType, k8s.ReplVPS)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))

		stepLog = "Schedule apps and attach 200+ volumes"
		i := 0
		Step(stepLog, func() {
			contexts = make([]*scheduler.Context, 0)
			appScale := 200

			for i = 1; i < appScale; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fadavoltkn-%d", i))...)
			}
			ValidateApplications(contexts)
		})

		var wg sync.WaitGroup

		stepLog = "Attaching 40 volumes at same time"
		scheduleCount := 40
		Step(stepLog, func() {
			scheduleAppParallel := func(c int) {
				defer wg.Done()
				defer GinkgoRecover()
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf(fmt.Sprintf("fadavoltkn-%d", c)))...)

			}

			// Create apps in parallel
			for count := 0; count < scheduleCount; count++ {
				wg.Add(1)
				go scheduleAppParallel(i)
				i++
			}
			wg.Wait()
			ValidateApplications(contexts)
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{FADARemoteDetach}", func() {

	/*
								https://portworx.atlassian.net/browse/PTX-20624


								PWX :
								https://portworx.atlassian.net/browse/PWX-33898
								https://portworx.atlassian.net/browse/PWX-34277

								Bug Description :
									pod is in to ContainerCreation state for longer time when tried to move deployment from one node to other after cordoning the node

							1. Deploying nginx pod with RWO FADA volumes on node-1
					        2. Cordon the node-1 and rollout another pod consuming same FADA volume in node-2
				            3. Validate pod is stuck in container creating state.
							4. Stop PX on node-1, pod running on node-1 should go to Terminating state and pod on node-2 should be in running
		                    5. Uncordon node-1 and start PX
							6. pod node-1 should be terminated
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("FADARemoteDetach", "Validate FADA volume remote detach when px is down", nil, 0)
	})

	It("create and attach RWO volume. preform remote detach and attach to new pod", func() {

		applist := Inst().AppList
		var podNode node.Node
		appPodName := "test-mount-error"
		var appPod *v1.Pod
		var newPod *v1.Pod

		var appNamespace string
		contexts = make([]*scheduler.Context, 0)
		defer func() {
			Inst().AppList = applist
			if podNode.Name != "" {
				err = Inst().S.EnableSchedulingOnNode(podNode)
				log.FailOnError(err, "error enabling scheduling on node [%s]", podNode.Name)
				StartVolDriverAndWait([]node.Node{podNode})
			}

		}()
		Inst().AppList = []string{"nginx-fada-deploy"}

		stepLog = "Deploy nginx pod and with RWO FADA Volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			contexts = append(contexts, ScheduleApplications("fadavoldetach")...)

			ValidateApplications(contexts)
		})

		stepLog = "Disable scheduling on the node where pod is running"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nsList, err := core.Instance().ListNamespaces(map[string]string{"creator": "torpedo"})
			log.FailOnError(err, "error getting all namespaces")

			for _, ns := range nsList.Items {
				if strings.Contains(ns.Name, "fadavoldetach") {
					appNamespace = ns.Name
					break
				}
			}
			log.Infof("App deployed in namespace %s", appNamespace)
			appPods, err := core.Instance().GetPods(appNamespace, nil)
			log.FailOnError(err, fmt.Sprintf("error getting pods in namespace %s", appNamespace))
			for _, p := range appPods.Items {
				if strings.Contains(p.Name, appPodName) {
					appPod = &p
					break
				}
			}
			if appPod == nil {
				log.FailOnError(fmt.Errorf("pod with name [%s] not availalbe", appPodName), "error getting app pod")
			}
			podNodeName := appPod.Spec.NodeName
			log.InfoD("pod [%s] is deployed on node %s", appPodName, podNodeName)
			podNode, err = node.GetNodeByName(podNodeName)
			log.FailOnError(err, fmt.Sprintf("error getting node with name %s", podNodeName))
			log.InfoD("Disabling scheduling on node %s", podNodeName)
			err = Inst().S.DisableSchedulingOnNode(podNode)
			log.FailOnError(err, fmt.Sprintf("error cordoning the node %s", podNodeName))
		})

		podVolClaimName := appPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
		stepLog = fmt.Sprintf("Do a rollout restart and create new replacement pod using volume [%s]", podVolClaimName)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			cmd := fmt.Sprintf("kubectl -n %s rollout restart deploy test-mount-error", appNamespace)
			output, _, err := osutils.ExecShell(cmd)
			log.FailOnError(err, "failed to run deployment rollout command")
			if !strings.Contains(output, "restarted") {
				log.FailOnError(fmt.Errorf("deployment restart failed with error : %s", output), "deployment restart failed")
			}
			appPods, err := core.Instance().GetPods(appNamespace, nil)
			log.FailOnError(err, fmt.Sprintf("error getting pods in namespace %s", appNamespace))
			for _, p := range appPods.Items {
				if strings.Contains(p.Name, appPodName) && p.Name != appPod.Name {
					newPod = &p
					break
				}
			}
			if newPod == nil {
				log.FailOnError(fmt.Errorf("new pod with name [%s] is not availalbe", appPodName), "error getting new app pod")
			}

			err = core.Instance().ValidatePod(newPod, 2*time.Minute, 20*time.Second)
			if err != nil {
				currPod, err := core.Instance().GetPodByUID(newPod.UID, newPod.Namespace)
				log.FailOnError(err, fmt.Sprintf("error getting current pod with UID[%s] in namespace [%s]", newPod.UID, newPod.Namespace))
				containerState := currPod.Status.ContainerStatuses[0].State
				if containerState.Waiting != nil {
					dash.VerifyFatal(containerState.Waiting.Reason, "ContainerCreating", "verify new pod container is in ContainerCreating state")
				} else {
					err = fmt.Errorf("current state of pod is %v where as Waiting state is expected", containerState)
					dash.VerifyFatal(err, nil, "validate new pod state")
				}
			}
		})

		stepLog = fmt.Sprintf("Stop Portworx on node [%s] and validate new pod", podNode.Name)
		Step(stepLog, func() {
			StopVolDriverAndWait([]node.Node{podNode})
			err = core.Instance().ValidatePod(newPod, 5*time.Minute, 10*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("verify new pod [%s] is in ready state", newPod.Name))

			// Waiting for original pod to be in terminating state
			waitForPodTerminatingState := func() (interface{}, bool, error) {
				orgPod, err := core.Instance().GetPodByUID(appPod.UID, appPod.Namespace)
				if err != nil {
					return nil, true, err
				}
				containerState := orgPod.Status.ContainerStatuses[0].State
				if containerState.Running != nil {
					return nil, true, fmt.Errorf("container is still in running state")
				}
				log.Infof("current state is %v", containerState)

				return nil, false, nil
			}
			_, err = task.DoRetryWithTimeout(waitForPodTerminatingState, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, fmt.Sprintf("error validating pod with UID[%s] status in namespace [%s]", newPod.UID, newPod.Namespace))
			StartVolDriverAndWait([]node.Node{podNode})
			// Waiting for original pod to be in terminating state
			waitForPodTerminated := func() (interface{}, bool, error) {
				appPods, err := core.Instance().GetPods(appNamespace, nil)
				if err != nil {
					return nil, true, err
				}

				for _, p := range appPods.Items {
					if p.Name == appPod.Name {
						return nil, true, fmt.Errorf("pod [%s] still not terminated. Current state [%v]", appPod.Name, p.Status.ContainerStatuses[0].State)
					}
				}

				return nil, false, nil
			}
			_, err = task.DoRetryWithTimeout(waitForPodTerminated, 5*time.Minute, 10*time.Second)

			dash.VerifyFatal(err, nil, "validate original pod is deleted after px is started.")

		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test Creates multiple FADA volume/app (nginx) - Reboots a Node while volume creation is in progress
/*

https://portworx.testrail.net/index.php?/tests/view/72615025

*/

var _ = Describe("{RebootNodeWhileVolCreate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootNodeWhileVolCreate", "Test creates multiple FADA volume and reboots a node while volume creation is in progress", nil, 72615025)
	})
	It("schedules nginx fada volumes on (n) * (NumberOfDeploymentsPerReboot) different namespaces and reboots a different node after every NumberOfDeploymentsPerReboot have been queued to schedule", func() {
		//Provisioner for pure apps
		var contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup
		//Scheduling app with volume placement strategy
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())
		storageNodes := node.GetStorageNodes()
		selectedNode := storageNodes[rand.Intn(len(storageNodes))]
		var err error
		defer func() {
			Inst().AppList = applist
			err = Inst().S.RemoveLabelOnNode(selectedNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
		}()
		Inst().AppList = []string{"nginx-fada-repl-vps"}
		err = Inst().S.AddLabelOnNode(selectedNode, k8s.NodeType, k8s.ReplVPS)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		Provisioner := fmt.Sprintf("%v", portworx.PortworxCsi)
		n := 3
		NumberOfDeploymentsPerReboot := 8
		//Reboot a random storage node n number of times
		for i := 0; i < n; i++ {
			// Step 1: Schedule applications
			wg.Add(1)
			go func() {
				defer wg.Done()
				Step("Schedule applications", func() {
					log.InfoD("Scheduling applications")
					for j := 0; j < NumberOfDeploymentsPerReboot; j++ {
						taskName := fmt.Sprintf("test-%v", (j+1)+NumberOfDeploymentsPerReboot*i)
						context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
							AppKeys:            Inst().AppList,
							StorageProvisioner: Provisioner,
							PvcSize:            6 * units.GiB,
						})
						log.FailOnError(err, "Failed to schedule application of %v namespace", taskName)
						contexts = append(contexts, context...)
					}
				})
			}()
			// Step 2: Pick a random storage node and reboot
			wg.Add(1)
			go func() {
				defer wg.Done()
				stepLog := "Pick a random storage node and reboot"
				Step(stepLog, func() {

					log.Infof("Stopping node %s", selectedNode.Name)
					err := Inst().N.RebootNode(selectedNode,
						node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultCommandRetry,
							},
						})
					log.FailOnError(err, "Failed to reboot node %v", selectedNode.Name)
				})
			}()

			// Wait for both steps to complete
			wg.Wait()

			log.Infof("wait for node: %s to be back up", selectedNode.Name)
			nodeReadyStatus := func() (interface{}, bool, error) {
				err := Inst().S.IsNodeReady(selectedNode)
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, 10*time.Minute, 35*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", selectedNode.Name))
			err = Inst().V.WaitDriverUpOnNode(selectedNode, Inst().DriverStartTimeout)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", selectedNode.Name))
			log.FailOnError(err, "Failed to reboot node")
			stepLog = "Validate the applications"
			Step(stepLog, func() {
				ValidateApplications(contexts)
			})
		}
		for i := 0; i < n; i++ {
			stepLog = "Reboot a random node,destroy scheduled apps and check if pvc's are deleted gracefully"

			Step(stepLog, func() {
				wg.Add(1)
				// Step 1: Reboot one random storage node
				go func() {
					defer wg.Done()
					stepLog := "Reboot one random storage node"
					Step(stepLog, func() {
						err := Inst().N.RebootNode(selectedNode, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultCommandRetry,
							},
						})
						log.FailOnError(err, "Failed to reboot node")
					})
				}()
				// Step 2: Destroy Application
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Destroy Application"
					//this wait is added because while reboot some of the pods go to error state and takes time to comeback to normal state
					log.InfoD("sleep for 2 and half minutes for pods to comeback to running state")
					time.Sleep((5 / 2) * time.Minute)
					Step(stepLog, func() {
						opts := make(map[string]bool)
						opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
						for j := 0; j < NumberOfDeploymentsPerReboot; j++ {
							TearDownContext(contexts[j+NumberOfDeploymentsPerReboot*i], opts)
						}
					})
				}()
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog = "Wait for node to come up"
					Step(stepLog, func() {
						nodeReadyStatus := func() (interface{}, bool, error) {
							err := Inst().S.IsNodeReady(selectedNode)
							if err != nil {
								return "", true, err
							}
							return "", false, nil
						}
						_, err := DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, 10*time.Minute, 35*time.Second)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", selectedNode.Name))
						err = Inst().V.WaitDriverUpOnNode(selectedNode, Inst().DriverStartTimeout)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", selectedNode.Name))
					})
				}()
				//wait for both the steps to finish
				wg.Wait()
			})
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
