package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/units"
	appsv1 "k8s.io/api/apps/v1"
	"sort"
	"strconv"

	"math/rand"

	"github.com/portworx/torpedo/pkg/osutils"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	contexts := make([]*scheduler.Context, 0)

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

// This test Creates multiple FADA volume/app (nginx) - Restart PX on a Node while volume creation is in progress
/*
https://portworx.testrail.net/index.php?/tests/view/72615026

*/
var _ = Describe("{RestartPXWhileVolCreate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPXWhileVolCreate", "Test creates multiple FADA volume and restarts px on a node while volume creation is in progress", nil, 72615026)
	})
	It("schedules nginx fada volumes on (n) * (NumberOfDeploymentsPerRestart) different namespaces and restarts portworx on a node where volumes are placed after every NumberOfDeploymentsPerRestart have been queued to schedule", func() {
		var contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())

		//select the node to place volumes and PX will be restarted in this node
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

		//Number of times portworx has to be restarted
		n := 3

		//Number of apps to be deployed after which a restart can be triggered
		NumberOfDeploymentsPerRestart := 8

		//Restart portworx n number of times
		stepLog = "start provisioning nginx apps in the created namespaces and for every NumberOfDeploymentsPerRestart restart portworx on the selected node"
		Step(stepLog, func() {
			for i := 0; i < n; i++ {

				// Step 1: Schedule applications
				wg.Add(1)
				go func() {
					defer wg.Done()
					Step("Schedule applications", func() {
						log.InfoD("Scheduling applications")
						for j := 0; j < NumberOfDeploymentsPerRestart; j++ {
							taskName := fmt.Sprintf("test%v", (j)+NumberOfDeploymentsPerRestart*i)
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

				// Step 2: Restart Portworx
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Restart Portworx"
					Step(stepLog, func() {
						log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
						StopVolDriverAndWait([]node.Node{selectedNode})
						log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), selectedNode.Name)
						StartVolDriverAndWait([]node.Node{selectedNode})
					})
				}()
				// Wait for both steps to complete
				wg.Wait()
				stepLog = "Validate the applications after portworx restart"
				Step(stepLog, func() {
					ValidateApplications(contexts)
				})
			}
		})
		for i := 0; i < n; i++ {
			stepLog = "Restart portworx,destroy apps and check if the pvc's are deleted gracefully"
			Step(stepLog, func() {

				// Step 1: Restart Portworx
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Restart Portworx"
					Step(stepLog, func() {
						log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
						StopVolDriverAndWait([]node.Node{selectedNode})
						log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), selectedNode.Name)
						StartVolDriverAndWait([]node.Node{selectedNode})
					})
				}()

				// Step 2: Destroy Application
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Destroy Application"
					Step(stepLog, func() {
						opts := make(map[string]bool)
						opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
						for j := 0; j < NumberOfDeploymentsPerRestart; j++ {
							TearDownContext(contexts[j+NumberOfDeploymentsPerRestart*i], opts)
						}
					})
				}()
				// Wait for both steps to complete
				wg.Wait()
			})
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// This test Creates multiple FADA volume/app (nginx) - Restart PX on a Node while volume creation is in progress
/*
https://portworx.testrail.net/index.php?/tests/view/72615026

*/
var _ = Describe("{RestartPXWhileVolCreate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPXWhileVolCreate", "Test creates multiple FADA volume and restarts px on a node while volume creation is in progress", nil, 72615026)
	})
	It("schedules nginx fada volumes on (n) * (NumberOfDeploymentsPerRestart) different namespaces and restarts portworx on a node where volumes are placed after every NumberOfDeploymentsPerRestart have been queued to schedule", func() {
		var contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())

		//select the node to place volumes and PX will be restarted in this node
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

		//Number of times portworx has to be restarted
		n := 3

		//Number of apps to be deployed after which a restart can be triggered
		NumberOfDeploymentsPerRestart := 8

		//Restart portworx n number of times
		stepLog = "start provisioning nginx apps in the created namespaces and for every NumberOfDeploymentsPerRestart restart portworx on the selected node"
		Step(stepLog, func() {
			for i := 0; i < n; i++ {

				// Step 1: Schedule applications
				wg.Add(1)
				go func() {
					defer wg.Done()
					Step("Schedule applications", func() {
						log.InfoD("Scheduling applications")
						for j := 0; j < NumberOfDeploymentsPerRestart; j++ {
							taskName := fmt.Sprintf("test%v", (j)+NumberOfDeploymentsPerRestart*i)
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

				// Step 2: Restart Portworx
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Restart Portworx"
					Step(stepLog, func() {
						log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
						StopVolDriverAndWait([]node.Node{selectedNode})
						log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), selectedNode.Name)
						StartVolDriverAndWait([]node.Node{selectedNode})
					})
				}()
				// Wait for both steps to complete
				wg.Wait()
				stepLog = "Validate the applications after portworx restart"
				Step(stepLog, func() {
					ValidateApplications(contexts)
				})
			}
		})
		for i := 0; i < n; i++ {
			stepLog = "Restart portworx,destroy apps and check if the pvc's are deleted gracefully"
			Step(stepLog, func() {

				// Step 1: Restart Portworx
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Restart Portworx"
					Step(stepLog, func() {
						log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
						StopVolDriverAndWait([]node.Node{selectedNode})
						log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), selectedNode.Name)
						StartVolDriverAndWait([]node.Node{selectedNode})
					})
				}()

				// Step 2: Destroy Application
				wg.Add(1)
				go func() {
					defer wg.Done()
					stepLog := "Destroy Application"
					Step(stepLog, func() {
						opts := make(map[string]bool)
						opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
						for j := 0; j < NumberOfDeploymentsPerRestart; j++ {
							TearDownContext(contexts[j+NumberOfDeploymentsPerRestart*i], opts)
						}
					})
				}()
				// Wait for both steps to complete
				wg.Wait()
			})
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// This test Creates multiple FADA volume/app (nginx) - Stop PX on a Node, resize and validate pvc's,delete the apps and check if all the pods,pvc's and volumes are being deleted from the backend
/*
https://portworx.testrail.net/index.php?/cases/view/93034
https://portworx.testrail.net/index.php?/cases/view/93035

*/
var _ = Describe("{StopPXAddDiskDeleteApps}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("StopPXAddDiskDeleteApps", "Test creates multiple FADA volume and stops px on a node,resize pvc and checks if all the pods,pvc's are being deleted gracefully", nil, 93034)
	})
	It("schedules multiple nginx fada volumes, stops portworx on a node where volumes are placed,resize pvc's and checks if all the resources created are deleted gracefully", func() {
		var contexts = make([]*scheduler.Context, 0)
		requestedVols := make([]*volume.Volume, 0)
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())

		//select the node to place volumes and PX will be stopped in this node
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

		//Number of apps to be deployed
		NumberOfDeployments := 200

		Step("Schedule applications", func() {
			log.InfoD("Scheduling applications")
			for j := 0; j < NumberOfDeployments; j++ {
				taskName := fmt.Sprintf("test-%v", j)
				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Provisioner,
					PvcSize:            6 * units.GiB,
				})
				log.FailOnError(err, "Failed to schedule application of %v namespace", taskName)
				contexts = append(contexts, context...)
			}
			ValidateApplications(contexts)
		})
		stepLog = fmt.Sprintf("Stop portworx,resize and validate pvc,destroy apps and check if the pvc's are deleted gracefully")
		Step(stepLog, func() {
			stepLog := fmt.Sprintf("Stop Portworx")
			Step(stepLog, func() {
				log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
				StopVolDriverAndWait([]node.Node{selectedNode})
			})
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				var err error
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.Infof("len of app volumes is : %v", len(appVolumes))
					if len(appVolumes) == 0 {
						log.Errorf("found no volumes for app %s", ctx.App.Key)
					}
				})

				stepLog = fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						pvcs, err := GetContextPVCs(ctx)
						log.FailOnError(err, "Failed to get pvc's from context")
						for _, pvc := range pvcs {
							pvcSize := pvc.Spec.Resources.Requests.Storage().String()
							pvcSize = strings.TrimSuffix(pvcSize, "Gi")
							pvcSizeInt, err := strconv.Atoi(pvcSize)
							log.InfoD("increasing pvc [%s/%s]  size to %v %v", pvc.Namespace, pvc.Name, 2*pvcSizeInt, pvc.UID)
							resizedVol, err := Inst().S.ResizePVC(ctx, pvc, uint64(2*pvcSizeInt))
							log.FailOnError(err, "pvc resize failed pvc:%v", pvc.UID)
							log.InfoD("Vol uid %v", resizedVol.ID)
							requestedVols = append(requestedVols, resizedVol)
						}
					})
				stepLog = fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						for _, v := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								log.FailOnError(err, "didn't get auth token")
							}
							err := Inst().V.ValidateUpdateVolume(v, params)
							log.FailOnError(err, "Could not validate volume resize %v", v.Name)
						}
					})
			}
			stepLog = fmt.Sprintf("Destroy Application")
			Step(stepLog, func() {
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				for j := 0; j < NumberOfDeployments; j++ {
					TearDownContext(contexts[j], opts)
				}
			})
			stepLog = fmt.Sprintf("start portworx and wait for it to come up")
			Step(stepLog, func() {
				log.Infof("Start volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
				StartVolDriverAndWait([]node.Node{selectedNode})
			})
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// This test Kills the PX nodes where FADA volumes are attached, Deletes the pods and PVCs.
/*
https://portworx.testrail.net/index.php?/cases/view/92893

*/
var _ = Describe("{AppCleanUpWhenPxKill}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("AppCleanUpWhenPxKill", "Test creates multiple FADA volume and kills px nodes while the pods and pvc's are being deleted", nil, 72760884)
	})
	It("Schedules apps that use FADA volumes, kill the nodes where these volumes are placed while the volumes are being deleted.", func() {
		var contexts = make([]*scheduler.Context, 0)
		var wg sync.WaitGroup
		//Scheduling app with volume placement strategy
		applist := Inst().AppList
		rand.Seed(time.Now().Unix())

		//select the one storage node,one storageless node and one KVDB member node to place volumes and kill the nodes while apps are being destroyed
		storageNodes := node.GetStorageNodes()
		storageLessNodes := node.GetStorageLessNodes()
		kvdbNodes, err := GetAllKvdbNodes()
		log.FailOnError(err, "Failed to get kvdb nodes")
		var selectedNodes []node.Node
		selectedNodes = append(selectedNodes, storageNodes[rand.Intn(len(storageNodes))])
		selectedNodes = append(selectedNodes, storageLessNodes[rand.Intn(len(storageLessNodes))])
		for _, kvdbNode := range kvdbNodes {
			if kvdbNode.ID != selectedNodes[0].Id {
				selectedKvdbNode, err := node.GetNodeDetailsByNodeID(kvdbNode.ID)
				log.FailOnError(err, "Failed to get kvdb node details")
				log.InfoD("Selected kvdb node: %v", selectedKvdbNode.Name)
				selectedNodes = append(selectedNodes, selectedKvdbNode)
				break
			}
		}

		defer func() {
			Inst().AppList = applist
			for _, node := range selectedNodes {
				err = Inst().S.RemoveLabelOnNode(node, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", node.Name)
			}
		}()

		Inst().AppList = []string{"nginx-fada-repl-vps"}
		for _, node := range selectedNodes {
			err = Inst().S.AddLabelOnNode(node, k8s.NodeType, k8s.ReplVPS)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", node.Name))
		}

		Provisioner := fmt.Sprintf("%v", portworx.PortworxCsi)
		//Number of apps to be deployed
		NumberOfAppsToBeDeployed := 300

		stepLog = fmt.Sprintf("schedule application")
		Step(stepLog, func() {
			for j := 0; j < NumberOfAppsToBeDeployed; j++ {
				taskName := fmt.Sprintf("app-cleanup-when-px-kill-%v", j)
				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Provisioner,
					PvcSize:            6 * units.GiB,
				})
				log.FailOnError(err, "Failed to schedule application of %v namespace", taskName)
				contexts = append(contexts, context...)
			}
			ValidateApplications(contexts)
		})
		stepLog = fmt.Sprintf("Kill PX nodes,destroy apps and check if the pvc's are deleted gracefully")
		Step(stepLog, func() {
			// Step 1: Destroy Applications
			wg.Add(1)
			go func() {
				defer wg.Done()
				stepLog := "Destroy Applications"
				Step(stepLog, func() {
					opts := make(map[string]bool)
					opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
					for j := 0; j < NumberOfAppsToBeDeployed; j++ {
						TearDownContext(contexts[j], opts)
					}
				})
			}()

			// Step 2: kill px nodes
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				stepLog := fmt.Sprintf("Kill px nodes")
				Step(stepLog, func() {
					for _, selectedNode := range selectedNodes {
						log.InfoD("Crashing node: %v", selectedNode.Name)
						err := Inst().N.CrashNode(selectedNode, node.CrashNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						log.FailOnError(err, "Failed to crash node:%v", selectedNode.Name)
					}
				})
			}()
			// Wait for both steps to complete
			wg.Wait()
		})
		stepLog = fmt.Sprintf("Wait until all the nodes come up")
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, selectedNode := range selectedNodes {
				err = Inst().N.TestConnection(selectedNode, node.ConnectionOpts{
					Timeout:         defaultTestConnectionTimeout,
					TimeBeforeRetry: defaultWaitRebootRetry,
				})
				log.FailOnError(err, "node:%v Failed to come up?", selectedNode.Name)
				err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
				log.FailOnError(err, "Portworx not coming up on node:%v", selectedNode.Name)

			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{ResizePVCToMaxLimit}", func() {

	/*
		PTX:
			https://portworx.atlassian.net/browse/PTX-20636
			https://portworx.atlassian.net/browse/PTX-20637
		TestRail:
			https://portworx.testrail.net/index.php?/cases/view/87940
			https://portworx.testrail.net/index.php?/tests/view/87941
	*/

	// Backend represents the cloud storage provider for volume provisioning
	type Backend string

	const (
		BackendPure    Backend = "PURE"
		BackendVSphere Backend = "VSPHERE"
		BackendUnknown Backend = "UNKNOWN"
	)

	// VolumeType represents the type of provisioned volume
	type VolumeType string

	const (
		VolumeFADA    VolumeType = "FADA"
		VolumeFBDA    VolumeType = "FBDA"
		VolumeFACD    VolumeType = "FACD"
		VolumeVsCD    VolumeType = "VsCD"
		VolumeUnknown VolumeType = "UNKNOWN"
	)

	var (
		contexts            = make([]*scheduler.Context, 0)
		backend             = BackendUnknown
		volumeMap           = make(map[VolumeType][]*api.Volume)
		volumeCtxMap        = make(map[string]*scheduler.Context)
		steps        uint64 = 5
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePVCToMaxLimit", "Validate PVC resize to max limit", nil, 87940)
	})

	It("Validates PVC resize to max limit", func() {
		// getPureMaxVolSize returns the maximum volume size based on the given volume type on pure backend
		getPureMaxVolSize := func(volType VolumeType) uint64 {
			switch volType {
			case VolumeFADA, VolumeFBDA:
				return 100 * units.TiB
			default:
				return uint64(MaxVolumeSize) * units.TiB
			}
		}
		// getMaxVolSize gets the maximum volume size based on the given backend and volume type
		getMaxVolSize := func(backend Backend, volType VolumeType) uint64 {
			switch backend {
			case BackendPure:
				return getPureMaxVolSize(volType)
			default:
				return 40 * units.TiB
			}
		}
		// getResizeSequence generates a sequence of sizes to resize to, based on the start, max values and number of steps
		getResizeSequence := func(start uint64, max uint64, steps uint64) []uint64 {
			seq := make([]uint64, 0)
			if steps == 0 {
				return []uint64{max}
			}
			if start >= max {
				log.Errorf("start value [%d] should be less than max value [%d]", start, max)
				return nil
			}
			d := (max - start) / steps
			for i := uint64(1); i <= steps; i++ {
				value := start + i*d
				seq = append(seq, value)
			}
			return seq
		}
		// getPureVolumeType determines the type of the volume based on the proxy spec
		getPureVolumeType := func(vol *volume.Volume) (VolumeType, error) {
			proxySpec, err := Inst().V.GetProxySpecForAVolume(vol)
			if err != nil {
				return "", fmt.Errorf("failed to get proxy spec for the volume [%s/%s]. Err: [%v]", vol.Namespace, vol.Name, err)
			}
			if proxySpec != nil {
				switch proxySpec.ProxyProtocol {
				case api.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE:
					return VolumeFBDA, nil
				case api.ProxyProtocol_PROXY_PROTOCOL_PURE_BLOCK:
					return VolumeFADA, nil
				default:
					return VolumeUnknown, nil
				}
			} else {
				return VolumeFACD, nil
			}
		}
		// formatMaxVolSizeReachedErrorMessage formats the error message when the maximum volume size is reached
		formatMaxVolSizeReachedErrorMessage := func(allowedSize uint64, requestedSize uint64) string {
			return "rpc error: code = Internal desc = Failed to update volume: " +
				"rpc error: code = Internal desc = Failed to update volume: " +
				"Feature upgrade needed. Licensed maximum reached for " +
				"'VolumeSize' feature (allowed " + fmt.Sprintf("%d", allowedSize/units.GiB) +
				" GiB, requested " + fmt.Sprintf("%d", requestedSize/units.GiB) + " GiB)\n"
		}
		// getContextAndPVC retrieves the scheduler context and PVC spec associated with a given volume
		getContextAndPVC := func(vol *api.Volume) (*scheduler.Context, *v1.PersistentVolumeClaim, error) {
			pvcName := vol.Spec.VolumeLabels["pvc"]
			namespace := vol.Spec.VolumeLabels["namespace"]
			pvc, err := core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get PVC [%s/%s] spec", pvcName, namespace)
			}
			if ctx, ok := volumeCtxMap[vol.Id]; !ok {
				return nil, nil, fmt.Errorf("context associated with PVC [%s/%s] not found", pvcName, namespace)
			} else {
				return ctx, pvc, nil
			}
		}
		// getPVCSize returns the requested storage size of the given PVC in bytes
		getPVCSize := func(pvc *v1.PersistentVolumeClaim) (uint64, error) {
			storage, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			if !ok {
				return 0, fmt.Errorf("failed to get storage resource request from PVC [%v]", pvc)
			}
			return uint64(storage.Value()), nil
		}
		// resizePVC resizes the given PVC using K8s API
		resizePVC := func(volType VolumeType, vol *api.Volume, newSize uint64) error {
			ctx, pvc, err := getContextAndPVC(vol)
			if err != nil {
				return fmt.Errorf("failed to get pvc from contexts. Err: [%v]", err)
			}
			pvcSize, err := getPVCSize(pvc)
			if err != nil {
				return fmt.Errorf("failed to get pvc [%v] size. Err: [%v]", pvc, err)
			}
			// adjustedSize ensures ResizePVC sets the volume to newSize
			adjustedSize := newSize - pvcSize
			log.Infof("Adjusted size for resizing [%s] volume [%s/%s] is [%d]", volType, vol.Id, vol.Locator.Name, adjustedSize)
			_, err = Inst().S.ResizePVC(ctx, pvc, adjustedSize/units.GiB)
			if err != nil {
				return fmt.Errorf("failed to resize [%s] volume [%s/%s] from [%d] to [%d]. Err: [%v]", volType, vol.Id, vol.Locator.Name, pvcSize, newSize, err)
			}
			return nil
		}
		// resizeVolume attempts to resize the volume to the maximum allowed size
		resizeVolume := func(volType VolumeType, vol *api.Volume) error {
			maxVolSize := getMaxVolSize(backend, volType)
			previousSize := vol.Spec.Size
			resizeSequence := getResizeSequence(vol.Spec.Size, maxVolSize, steps)
			log.Infof("Original size of [%s] volume [%s/%s] is [%d]", volType, vol.Id, vol.Locator.Name, vol.Spec.Size)
			waitForResizeCompletionBasedOnSize := func(newSize uint64) (interface{}, bool, error) {
				vol, err = Inst().V.InspectVolume(vol.Id)
				if err != nil {
					return nil, false, fmt.Errorf("failed to inspect [%s] volume [%s/%s]", volType, vol.Id, vol.Locator.Name)
				}
				if vol.Spec.Size == newSize {
					return nil, false, nil
				}
				return nil, true, fmt.Errorf("volume size mismatch: inspected [%d], estimated [%d]", vol.Spec.Size, newSize)
			}
			for _, newSize := range resizeSequence {
				log.Infof("Resizing [%s] volume [%s/%s] from [%d] to [%d]", volType, vol.Id, vol.Locator.Name, previousSize, newSize)
				switch volType {
				case VolumeFADA:
					err = resizePVC(volType, vol, newSize)
				default:
					err = Inst().V.ResizeVolume(vol.Id, newSize)
				}
				if err != nil {
					return fmt.Errorf("failed to resize [%s] volume [%s/%s] from [%d] to [%d]. Err: [%v]", volType, vol.Id, vol.Locator.Name, previousSize, newSize, err)
				}
				waitForResizeCompletion := func() (interface{}, bool, error) {
					return waitForResizeCompletionBasedOnSize(newSize)
				}
				_, err = task.DoRetryWithTimeout(waitForResizeCompletion, 10*time.Minute, 30*time.Second)
				if err != nil {
					return fmt.Errorf("failed to wait for volume [%s] resize completion. Err: [%v]", vol.Locator.Name, err)
				}
				previousSize = newSize
			}
			newSize := 2 * previousSize
			log.Infof("Resizing [%s] volume [%s/%s] from [%d] to size [%d], which is over the limit [%d]", volType, vol.Id, vol.Locator.Name, previousSize, newSize, maxVolSize)
			switch volType {
			case VolumeFADA:
				err = resizePVC(volType, vol, newSize)
				waitForResizeCompletion := func() (interface{}, bool, error) {
					return waitForResizeCompletionBasedOnSize(newSize)
				}
				_, err = task.DoRetryWithTimeout(waitForResizeCompletion, 10*time.Minute, 30*time.Second)
				if err != nil {
					return fmt.Errorf("failed to wait for volume [%s] resize completion. Err: [%v]", vol.Locator.Name, err)
				}
			default:
				err = Inst().V.ResizeVolume(vol.Id, newSize)
			}
			if err != nil {
				switch err.Error() {
				case formatMaxVolSizeReachedErrorMessage(vol.Spec.Size, newSize):
					log.InfoD("Skipping error [%v] as it falls within expected behavior", err)
					return nil
				default:
					return fmt.Errorf("failed to resize [%s] volume [%s/%s] from [%d] to [%d]. Err: [%v]", volType, vol.Id, vol.Locator.Name, vol.Spec.Size, newSize, err)
				}
			}
			return nil
		}
		Step("Schedule applications", func() {
			log.InfoD("Scheduling applications")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("pure-test-%d", i)
				for _, ctx := range ScheduleApplications(taskName) {
					ctx.ReadinessTimeout = appReadinessTimeout
					contexts = append(contexts, ctx)
				}
			}
		})
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(contexts)
		})
		Step("Identify backend and categorize volumes", func() {
			log.InfoD("Identifying backend")
			volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
			log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
			secretList, err := core.Instance().ListSecret(volDriverNamespace, metav1.ListOptions{})
			log.FailOnError(err, "failed to get secret list from namespace [%s]", volDriverNamespace)
			for _, secret := range secretList.Items {
				switch secret.Name {
				case PX_PURE_SECRET_NAME:
					backend = BackendPure
					break
				case PX_VSPHERE_SCERET_NAME:
					backend = BackendVSphere
					break
				}
			}
			log.InfoD("Backend: %v", backend)
			log.InfoD("Categorizing volumes")
			for _, ctx := range contexts {
				volumes, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "failed to get volumes for app [%s/%s]", ctx.App.NameSpace, ctx.App.Key)
				dash.VerifyFatal(len(volumes) > 0, true, "Verifying if volumes exist for resizing")
				// The CloudStorage.Provider in StorageCluster Spec is not accurate
				for _, vol := range volumes {
					apiVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "failed to inspect volume [%s/%s]", vol.Name, vol.ID)
					switch backend {
					case BackendPure:
						volType, err := getPureVolumeType(vol)
						log.FailOnError(err, "failed to get pure volume type for volume [%+v]", vol)
						volumeMap[volType] = append(volumeMap[volType], apiVol)
					case BackendVSphere:
						volumeMap[VolumeVsCD] = append(volumeMap[VolumeVsCD], apiVol)
					default:
						volumeMap[VolumeUnknown] = append(volumeMap[VolumeUnknown], apiVol)
					}
					volumeCtxMap[apiVol.Id] = ctx
				}
			}
		})
		Step("Resize a random volume of each type to max limit", func() {
			log.InfoD("Resizing a random volume of each type to max limit")
			for volType, vols := range volumeMap {
				if len(vols) > 0 {
					log.Infof("List of all [%d] [%s] volumes [%s]", len(vols), volType, vols)
					vol := vols[rand.Intn(len(vols))]
					log.InfoD("Resizing random [%s] volume [%s/%s] to max limit [%d]", volType, vol.Id, vol.Locator.Name, getMaxVolSize(backend, volType))
					err := resizeVolume(volType, vol)
					log.FailOnError(err, "failed to resize random [%s] volume [%s/%s] to max limit [%d]", volType, vol.Id, vol.Locator.Name, getMaxVolSize(backend, volType))
				}
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Destroying applications")
		DestroyApps(contexts, opts)
	})
})

var _ = Describe("{CreateAndDeleteMultipleVolumesInParallel}", func() {

	/*
		PTX:
			https://portworx.atlassian.net/browse/PTX-20633
		TestRail:
			https://portworx.testrail.net/index.php?/cases/view/92653
			https://portworx.testrail.net/index.php?/cases/view/92654
	*/

	// Backend represents the cloud storage provider for volume provisioning
	type Backend string

	const (
		BackendPure    Backend = "PURE"
		BackendVSphere Backend = "VSPHERE"
		BackendUnknown Backend = "UNKNOWN"
	)

	// VolumeType represents the type of provisioned volume
	type VolumeType string

	const (
		VolumeFADA    VolumeType = "FADA"
		VolumeFBDA    VolumeType = "FBDA"
		VolumeFACD    VolumeType = "FACD"
		VolumeVsCD    VolumeType = "VsCD"
		VolumeUnknown VolumeType = "UNKNOWN"
	)

	var (
		contexts            []*scheduler.Context
		appSpecMap          = make(map[string]*spec.AppSpec)
		volCountFromSpecMap = make(map[string]int)
		approxVolCount      = 500
		exceedVolCount      = true
		backend             = BackendUnknown
		volumeMap           = make(map[VolumeType][]*api.Volume)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndDeleteMultipleVolumesInParallel", "Validate volume creation and deletion in parallel", nil, 92653)
	})

	It("Validates volume creation and deletion in parallel", func() {
		// getAppSpec retrieves the app spec for a given app key
		getAppSpec := func(appKey string) (*spec.AppSpec, error) {
			switch Inst().S.(type) {
			case *k8s.K8s:
				appSpec, err := Inst().S.(*k8s.K8s).SpecFactory.Get(appKey)
				if err != nil {
					return nil, fmt.Errorf("failed to get app [%s] spec", appKey)
				}
				return appSpec, nil
			default:
				return nil, fmt.Errorf("unsupported scheduler [%s] type [%T]", Inst().S.String(), Inst().S)
			}
		}
		// getVolCountFromSpec gets the total number of volumes in the given app spec similar to Inst().S.GetVolumes
		getVolCountFromSpec := func(appSpec *spec.AppSpec) (int, error) {
			volCount := 0
			for _, spec := range appSpec.SpecList {
				switch kObj := spec.(type) {
				case *v1.PersistentVolumeClaim:
					// For PVCs, each is counted once, without replication
					volCount++
				case *appsv1.StatefulSet:
					// For StatefulSets, each VolumeClaimTemplate results in PVCs multiplied by the replica count
					replicas := 1
					if kObj.Spec.Replicas != nil {
						replicas = int(*kObj.Spec.Replicas)
					}
					volCount += len(kObj.Spec.VolumeClaimTemplates) * replicas
				}
			}
			log.InfoD("Expected volume count for app [%s] is [%d]", appSpec.Key, volCount)
			return volCount, nil
		}
		// getPureVolumeType determines the type of the volume based on the proxy spec
		getPureVolumeType := func(vol *volume.Volume) (VolumeType, error) {
			proxySpec, err := Inst().V.GetProxySpecForAVolume(vol)
			if err != nil {
				return "", fmt.Errorf("failed to get proxy spec for the volume [%s/%s]. Err: [%v]", vol.Namespace, vol.Name, err)
			}
			if proxySpec != nil {
				switch proxySpec.ProxyProtocol {
				case api.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE:
					return VolumeFBDA, nil
				case api.ProxyProtocol_PROXY_PROTOCOL_PURE_BLOCK:
					return VolumeFADA, nil
				default:
					return VolumeUnknown, nil
				}
			} else {
				return VolumeFACD, nil
			}
		}
		// scaleDownApp scales an app to zero replicas using the given context and waits for pods to terminate
		scaleDownApp := func(ctx *scheduler.Context) error {
			scaleApp(ctx, 0)
			waitForPodsToTerminate := func() (interface{}, bool, error) {
				vols, err := Inst().S.GetVolumes(ctx)
				if err != nil {
					return nil, false, err
				}
				podCount := 0
				for _, vol := range vols {
					if vol.ID == "" {
						return nil, false, fmt.Errorf("empty vol.ID in volume [%v]", vol)
					}
					pods, err := core.Instance().GetPodsUsingPV(vol.ID)
					if err != nil {
						return nil, false, err
					}
					podCount += len(pods)
				}
				if podCount > 0 {
					return nil, true, fmt.Errorf("expected no pods, but found [%d] remaining", podCount)
				}
				return nil, false, nil
			}
			_, err := task.DoRetryWithTimeout(waitForPodsToTerminate, 3*time.Minute, 10*time.Second)
			if err != nil {
				return fmt.Errorf("failed to scale down app [%s] and ensure all pods are deleted. Err: [%v]", ctx.App.Key, err)
			}
			return nil
		}
		// formatVolNotFoundErrorMessage formats the error message when the volume is not found
		formatVolNotFoundErrorMessage := func(volId string) string {
			return "rpc error: code = NotFound desc = Volume id " + volId + " not found"
		}
		// deletePVC deletes the PVC linked to the given volume
		deletePVC := func(volType VolumeType, vol *api.Volume) error {
			namespace := vol.Spec.VolumeLabels["namespace"]
			pvcName := vol.Spec.VolumeLabels["pvc"]
			log.Infof("Deleting PVC [%s/%s] linked with [%s] volume [%s/%s]", namespace, pvcName, volType, vol.Id, vol.Locator.Name)
			err = core.Instance().DeletePersistentVolumeClaim(pvcName, namespace)
			if err != nil {
				return fmt.Errorf("failed to delete pvc [%s/%s] linked with [%s] volume [%s/%s]", namespace, pvcName, volType, vol.Id, vol.Locator.Name)
			}
			waitForVolumeDeletion := func() (interface{}, bool, error) {
				_, err := Inst().V.InspectVolume(vol.Id)
				if err != nil {
					switch err.Error() {
					case formatVolNotFoundErrorMessage(vol.Id):
						return nil, false, nil
					default:
						return nil, false, err
					}
				}
				return nil, true, fmt.Errorf("[%s] volume [%s/%s] still exists", volType, vol.Id, vol.Locator.Name)
			}
			_, err = task.DoRetryWithTimeout(waitForVolumeDeletion, 5*time.Minute, 30*time.Second)
			if err != nil {
				return fmt.Errorf("failed to wait for [%s] volume [%s/%s] deletion. Err: [%v]", volType, vol.Id, vol.Locator.Name, err)
			}
			return nil
		}
		Step("Extract volume counts from app specs", func() {
			log.InfoD("Extracting volume counts from app specs")
			for _, appKey := range Inst().AppList {
				appSpec, err := getAppSpec(appKey)
				log.FailOnError(err, "failed to get app [%s] spec", appKey)
				appSpecMap[appKey] = appSpec
				volCount, err := getVolCountFromSpec(appSpec)
				log.FailOnError(err, "failed to get volume count from app [%s] spec [%v]", appKey, appSpec)
				volCountFromSpecMap[appKey] = volCount
			}
		})
		Step(fmt.Sprintf("Schedule applications in parallel until [%d] volumes are created", approxVolCount), func() {
			// Counting the number of schedules for each app to reach approximate volume count
			appList := make([]string, len(Inst().AppList))
			copy(appList, Inst().AppList)
			// Sorting in descending order by volume count to prioritize scheduling of larger-volume apps
			sort.SliceStable(appList, func(i, j int) bool {
				return volCountFromSpecMap[appList[i]] > volCountFromSpecMap[appList[j]]
			})
			scheduleCount := make(map[string]int)
			totalVolCount := 0
			for _, appKey := range appList {
				appVolCount := volCountFromSpecMap[appKey]
				requiredInstances := (approxVolCount - totalVolCount) / appVolCount
				scheduleCount[appKey] = requiredInstances
				totalVolCount += requiredInstances * appVolCount
			}
			if exceedVolCount && totalVolCount < approxVolCount {
				for i := len(appList) - 1; i >= 0; i-- {
					appKey := appList[i]
					appVolCount := volCountFromSpecMap[appKey]
					if totalVolCount+appVolCount > approxVolCount {
						scheduleCount[appKey]++
						break
					}
				}
			}
			log.Infof("ScheduleCount: %v", scheduleCount)
			var mu sync.Mutex
			var wg sync.WaitGroup
			log.InfoD("Scheduling applications in parallel until [%d] volumes are created", approxVolCount)
			for appKey, count := range scheduleCount {
				for i := 0; i < count; i++ {
					wg.Add(1)
					go func(appKey string, i int) {
						defer GinkgoRecover()
						defer wg.Done()
						namespace := fmt.Sprintf("%s-deletevol-%d", appKey, i)
						scheduleOptions := CreateScheduleOptions(namespace)
						scheduleOptions.AppKeys = []string{appKey}
						context, err := Inst().S.Schedule(Inst().InstanceID, scheduleOptions)
						log.FailOnError(err, "failed to schedule app [%s-%d] with options [%v]", appKey, i, scheduleOptions)
						mu.Lock()
						contexts = append(contexts, context...)
						mu.Unlock()
					}(appKey, i)
				}
			}
			wg.Wait()
		})
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			for _, ctx := range contexts {
				ValidateContext(ctx)
				vols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "failed to get volumes for app [%s]", ctx.App.Key)
				dash.VerifyFatal(len(vols), volCountFromSpecMap[ctx.App.Key], fmt.Sprintf("Verifying volume count for app [%s]", ctx.App.Key))
			}
		})
		Step("Scale down applications to release volumes", func() {
			log.InfoD("Scaling down applications to release volumes")
			for _, ctx := range contexts {
				log.InfoD("Scaling down app [%s]", ctx.App.Key)
				err := scaleDownApp(ctx)
				log.FailOnError(err, "failed to scale down app [%s]", ctx.App.Key)
			}
		})
		Step("Identify backend and categorize volumes", func() {
			log.InfoD("Identifying backend")
			volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
			log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
			secretList, err := core.Instance().ListSecret(volDriverNamespace, metav1.ListOptions{})
			log.FailOnError(err, "failed to get secret list from namespace [%s]", volDriverNamespace)
			for _, secret := range secretList.Items {
				switch secret.Name {
				case PX_PURE_SECRET_NAME:
					backend = BackendPure
					break
				case PX_VSPHERE_SCERET_NAME:
					backend = BackendVSphere
					break
				}
			}
			log.InfoD("Backend: %v", backend)
			log.InfoD("Categorizing volumes")
			for _, ctx := range contexts {
				volumes, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "failed to get volumes for app [%s/%s]", ctx.App.NameSpace, ctx.App.Key)
				dash.VerifyFatal(len(volumes) > 0, true, "Verifying if volumes exist for deleting")
				// The CloudStorage.Provider in StorageCluster Spec is not accurate
				for _, vol := range volumes {
					apiVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "failed to inspect volume [%s/%s]", vol.Name, vol.ID)
					switch backend {
					case BackendPure:
						volType, err := getPureVolumeType(vol)
						log.FailOnError(err, "failed to get pure volume type for volume [%+v]", vol)
						volumeMap[volType] = append(volumeMap[volType], apiVol)
					case BackendVSphere:
						volumeMap[VolumeVsCD] = append(volumeMap[VolumeVsCD], apiVol)
					default:
						volumeMap[VolumeUnknown] = append(volumeMap[VolumeUnknown], apiVol)
					}
				}
			}
		})
		Step("Delete volumes in parallel", func() {
			log.InfoD("Deleting volumes in parallel")
			for volType, vols := range volumeMap {
				if len(vols) > 0 {
					log.Infof("List of all [%d] [%s] volumes [%s]", len(vols), volType, vols)
					var wg sync.WaitGroup
					for _, vol := range vols {
						wg.Add(1)
						go func(volType VolumeType, vol *api.Volume) {
							defer GinkgoRecover()
							defer wg.Done()
							log.InfoD("Delete [%s] volume [%s/%s]", volType, vol.Id, vol.Locator.Name)
							err = deletePVC(volType, vol)
							log.FailOnError(err, "failed to delete [%s] volume [%s/%s]", volType, vol.Id, vol.Locator.Name)
						}(volType, vol)
					}
					wg.Wait()
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Destroying applications")
		DestroyApps(contexts, opts)
	})
})
