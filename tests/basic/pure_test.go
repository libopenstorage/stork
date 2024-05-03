package tests

import (
	"fmt"

	"github.com/devans10/pugo/flasharray"
	"github.com/portworx/sched-ops/k8s/storage"

	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	storageApi "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/libopenstorage/openstorage/api"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/pureutils"
	"github.com/portworx/torpedo/pkg/units"

	. "github.com/onsi/ginkgo/v2"
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
		Step("setup credential necessary for cloudsnap", createCloudsnapCredential)
	})

	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		contexts = make([]*scheduler.Context, 0)

		err := Inst().V.InitializePureLocalVolumePaths() // Initialize our "baseline" of Pure devices, such as FACD devices or other local FA disks
		Expect(err).NotTo(HaveOccurred(), "unexpected error taking Pure device baseline")

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPureSDK(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})

	JustAfterEach(func() {
		Step("delete credential used for cloudsnap", deleteCloudsnapCredential)

		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// This test performs basic tests making sure Pure direct access volumes are running as expected
var _ = Describe("{PureVolumeCRUDWithPXCTL}", func() {
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("PureVolumeCRUDWithPXCTL", "Test pure volumes on applications, run CRUD using pxctl", nil, 0)
		Step("setup credential necessary for cloudsnap", createCloudsnapCredential)
	})
	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		contexts = make([]*scheduler.Context, 0)

		err := Inst().V.InitializePureLocalVolumePaths() // Initialize our "baseline" of Pure devices, such as FACD devices or other local FA disks
		Expect(err).NotTo(HaveOccurred(), "unexpected error taking Pure device baseline")

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPurePxctl(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
	JustAfterEach(func() {
		Step("delete credential used for cloudsnap", deleteCloudsnapCredential)

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
							7. Repeat same step from 2-5 and schedule pod on node-1
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
		podNodes := node.GetStorageNodes()[:2]
		defer func() {
			Inst().AppList = applist
			if podNode.Name != "" {
				err = Inst().S.EnableSchedulingOnNode(podNode)
				log.FailOnError(err, "error enabling scheduling on node [%s]", podNode.Name)
				StartVolDriverAndWait([]node.Node{podNode})
				for _, pn := range podNodes {
					err = Inst().S.RemoveLabelOnNode(pn, "apptype")
					log.FailOnError(err, fmt.Sprintf("error removing label apptype=fada on node [%s]", pn.Name))
				}
			}

		}()
		Inst().AppList = []string{"nginx-fada-deploy"}

		stepLog = "Deploy nginx pod and with RWO FADA Volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, pn := range podNodes {
				err = Inst().S.AddLabelOnNode(pn, "apptype", "fada")
				log.FailOnError(err, fmt.Sprintf("error applying label apptype=fada on node [%s]", pn.Name))
			}
			contexts = append(contexts, ScheduleApplications("fadavoldetach")...)
			ValidateApplications(contexts)
		})

		//Getting the pod and cordoning the node where pod is deployed
		disableSchedulingOnPodNode := func() {
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
		}

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
			disableSchedulingOnPodNode()
		})

		//Do rollout of deployment so the new pod is created
		performDeploymentRollout := func() {
			cmd := fmt.Sprintf("kubectl -n %s rollout restart deploy test-mount-error", appNamespace)
			output, _, err := osutils.ExecShell(cmd)
			log.FailOnError(err, "failed to run deployment rollout command")
			if !strings.Contains(output, "restarted") {
				log.FailOnError(fmt.Errorf("deployment restart failed with error : %s", output), "deployment restart failed")
			}
		}

		//validate the state of new pod is ContainerCreating after rollout
		validateNewPodState := func() {
			appPods, err := core.Instance().GetPods(appNamespace, nil)
			log.FailOnError(err, fmt.Sprintf("error getting pods in namespace %s", appNamespace))
			for _, p := range appPods.Items {
				if strings.Contains(p.Name, appPodName) && p.Name != appPod.Name {
					newPod = &p
					break
				}
			}
			if newPod == nil {
				log.FailOnError(fmt.Errorf("new pod with name [%s] is not available", appPodName), "error getting new app pod")
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
		}

		podVolClaimName := appPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
		stepLog = fmt.Sprintf("Do a rollout restart and create new replacement pod using volume [%s]", podVolClaimName)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			performDeploymentRollout()
			validateNewPodState()

		})

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

		//Validating  new pod ready  and original pod termination
		validatePodRemoteDetach := func() {
			stepLog = fmt.Sprintf("Stop Portworx on node [%s] and validate new pod", podNode.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				StopVolDriverAndWait([]node.Node{podNode})
				err = core.Instance().ValidatePod(newPod, 5*time.Minute, 10*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify new pod [%s] is in ready state", newPod.Name))

				_, err = task.DoRetryWithTimeout(waitForPodTerminatingState, 5*time.Minute, 10*time.Second)
				log.FailOnError(err, fmt.Sprintf("error validating pod with UID[%s] status in namespace [%s]", newPod.UID, newPod.Namespace))

				StartVolDriverAndWait([]node.Node{podNode})
				_, err = task.DoRetryWithTimeout(waitForPodTerminated, 5*time.Minute, 10*time.Second)
				dash.VerifyFatal(err, nil, "validate original pod is deleted after px is started.")

				err = Inst().S.EnableSchedulingOnNode(podNode)
				log.FailOnError(err, "error enabling scheduling on node [%s]", podNode.Name)
			})
		}

		validatePodRemoteDetach()

		stepLog = fmt.Sprintf("Schedule pod back on node [%s]", podNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			disableSchedulingOnPodNode()
			performDeploymentRollout()
			validateNewPodState()
			validatePodRemoteDetach()

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
		volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
		log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
		pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
		log.FailOnError(err, "failed to get secret [%s]  in namespace [%s]", PureSecretName, volDriverNamespace)
		flashArrays := pxPureSecret.Arrays
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

							gotVol := false
							for _, fa := range flashArrays {
								faVol, err := pureutils.GetPureFAVolumeSize(v.Name, fa.MgmtEndPoint, fa.APIToken)
								log.FailOnError(err, "error getting vol [%s] size", v.Name)
								if faVol != 0 {
									dash.VerifyFatal(faVol, v.Size, fmt.Sprintf("validate volume [%s] resize in FA backend", v.Name))
									gotVol = true
									break
								}
							}
							if !gotVol {
								log.FailOnError(fmt.Errorf("unable to find vol [%s] size", v.Name), "error getting volume size")
							}
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
			log.Infof("waiting for 5 mins allowing voals to delete in backend")
			time.Sleep(5 * time.Minute)

			var faVolsAfterDel []string
			for _, fa := range flashArrays {
				v, err := pureutils.GetPureFAVolumes(fa.MgmtEndPoint, fa.APIToken)
				faVolsAfterDel = append(faVolsAfterDel, v...)
				log.FailOnError(err, "error getting vols using end point [%s],token [%s]", fa.MgmtEndPoint, fa.APIToken)
			}

			var existingVols []string
			for _, cv := range requestedVols {
				if faLUNExists(faVolsAfterDel, cv.Name) {
					existingVols = append(existingVols, cv.Name)
				}
			}

			dash.VerifyFatal(len(existingVols) == 0, true, fmt.Sprintf("validate all volumes are deleted in FA backend. Existing vols: [%v]", existingVols))
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
		requestedVols := make([]string, 0)
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
		if len(storageLessNodes) > 0 {
			selectedNodes = append(selectedNodes, storageLessNodes[rand.Intn(len(storageLessNodes))])
		}
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

			for _, ctx := range contexts {
				pvcs, err := GetContextPVCs(ctx)
				log.FailOnError(err, "Failed to get pvc's from context")
				for _, pvc := range pvcs {
					requestedVols = append(requestedVols, pvc.Spec.VolumeName)
				}
			}
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

		volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
		log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
		pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
		log.FailOnError(err, "failed to get secret [%s]  in namespace [%s]", PureSecretName, volDriverNamespace)
		flashArrays := pxPureSecret.Arrays

		if len(flashArrays) == 0 {
			log.FailOnError(fmt.Errorf("no FlashArrays details found"), fmt.Sprintf("error getting FlashArrays creds from %s [%s]", PureSecretName, pxPureSecret))
		}

		var faVolsAfterDel []string
		for _, fa := range flashArrays {
			v, err := pureutils.GetPureFAVolumes(fa.MgmtEndPoint, fa.APIToken)
			faVolsAfterDel = append(faVolsAfterDel, v...)
			log.FailOnError(err, "error getting vols using end point [%s],token [%s]", fa.MgmtEndPoint, fa.APIToken)
		}

		var existingVols []string
		for _, cv := range requestedVols {
			if faLUNExists(faVolsAfterDel, cv) {
				existingVols = append(existingVols, cv)
			}
		}

		dash.VerifyFatal(len(existingVols) == 0, true, fmt.Sprintf("validate all volumes are deleted in FA backend. Existing vols: [%v]", existingVols))
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
		// resizeVolumeToMaxSize attempts to resize the volume to the maximum allowed size
		resizeVolumeToMaxSize := func(volType VolumeType, vol *api.Volume) error {
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
				case VolumeFADA, VolumeFBDA:
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
				log.Infof("List of all [%d] [%s] volumes [%s]", len(vols), volType, vols)
				randomVol := vols[rand.Intn(len(vols))]
				log.InfoD("Resizing random [%s] volume [%s/%s] to max limit [%d]", volType, randomVol.Id, randomVol.Locator.Name, getMaxVolSize(backend, volType))
				err := resizeVolumeToMaxSize(volType, randomVol)
				log.FailOnError(err, "failed to resize random [%s] volume [%s/%s] to max limit [%d]", volType, randomVol.Id, randomVol.Locator.Name, getMaxVolSize(backend, volType))
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
			https://portworx.atlassian.net/browse/PTX-20619
			https://portworx.atlassian.net/browse/PTX-20631

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
		approxVolCount      = 2
		exceedVolCount      = true
		backend             = BackendUnknown
		volDriverNamespace  string
		clusterUIDPrefix    string
		volumeMap           = make(map[VolumeType][]*api.Volume)
		pureClientMap       = make(map[VolumeType]map[string]*flasharray.Client)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndDeleteMultipleVolumesInParallel", "Validate volume creation and deletion in parallel", nil, 92653)
		volDriverNamespace, err = Inst().V.GetVolumeDriverNamespace()
		log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
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
			_, err := task.DoRetryWithTimeout(waitForPodsToTerminate, 10*time.Minute, 30*time.Second)
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
		// getPureVolName translates the volume name into its equivalent in the pure backend
		getPureVolName := func(vol *api.Volume) string {
			return "px_" + clusterUIDPrefix + "-" + vol.Locator.Name
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
		Step("Identify backend and categorize volumes", func() {
			log.InfoD("Identifying backend")
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
		Step("Validate FADA and FBDA volumes creation in Pure Backend", func() {
			log.InfoD("Validating FADA and FBDA volumes creation in Pure Backend")
			if backend == BackendPure {
				// The check validates the pure backend only for FADA and FBDA volumes, as FACD volumes are not listed there
				if len(volumeMap[VolumeFADA])+len(volumeMap[VolumeFBDA]) > 0 {
					secret, err := pureutils.GetPXPureSecret(volDriverNamespace)
					log.FailOnError(err, "failed to get secret [%s/%s]", PureSecretName, volDriverNamespace)
					for _, volType := range []VolumeType{VolumeFADA, VolumeFBDA} {
						if len(volumeMap[volType]) > 0 {
							pureClientMap[volType] = make(map[string]*flasharray.Client)
							switch volType {
							case VolumeFADA:
								pureClientMap[volType], err = pureutils.GetFAClientMapFromPXPureSecret(secret)
							}
							log.FailOnError(err, "failed to get [%s] client map from secret [%s/%s]", volType, PureSecretName, volDriverNamespace)
						}
					}
				}
				cluster, err := Inst().V.InspectCurrentCluster()
				log.FailOnError(err, "failed to inspect current cluster")
				log.Infof("Current cluster [%s] UID: [%s]", cluster.Cluster.Name, cluster.Cluster.Id)
				clusterUIDPrefix = strings.Split(cluster.Cluster.Id, "-")[0]
				for volType, clientMap := range pureClientMap {
					allPureVolumes := make([]flasharray.Volume, 0)
					for mgmtEndPoint, client := range clientMap {
						pureVolumes, err := client.Volumes.ListVolumes(nil)
						log.FailOnError(err, "failed to list [%s] volumes from endpoint [%s]", volType, mgmtEndPoint)
						allPureVolumes = append(allPureVolumes, pureVolumes...)
					}
					for _, vol := range volumeMap[volType] {
						found := false
						pureVolName := getPureVolName(vol)
						for _, pureVol := range allPureVolumes {
							if pureVol.Name == pureVolName {
								found = true
							}
						}
						dash.VerifyFatal(found, true, fmt.Sprintf("Verify [%s] volume [%s/%s] creation in the pure backend", volType, vol.Id, vol.Locator.Name))
					}
				}
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
		Step("Delete volumes in parallel", func() {
			log.InfoD("Deleting volumes in parallel")
			for volType, vols := range volumeMap {
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
			for volType, clientMap := range pureClientMap {
				allPureVolumes := make([]flasharray.Volume, 0)
				for mgmtEndPoint, client := range clientMap {
					pureVolumes, err := client.Volumes.ListVolumes(nil)
					log.FailOnError(err, "failed to list [%s] volumes from endpoint [%s]", volType, mgmtEndPoint)
					allPureVolumes = append(allPureVolumes, pureVolumes...)
				}
				for _, vol := range volumeMap[volType] {
					found := false
					pureVolName := getPureVolName(vol)
					for _, pureVol := range allPureVolumes {
						if pureVol.Name == pureVolName {
							found = true
						}
					}
					dash.VerifyFatal(found, false, fmt.Sprintf("Verify [%s] volume [%s/%s] deletion in the pure backend", volType, vol.Id, vol.Locator.Name))
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

var _ = Describe("{PVCLUNValidation}", func() {
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("PVCLUNValidation", "Create and destroy large number of PVCs and validate LUN in the FA", nil, 0)
	})
	stepLog = "create large number of PVC and destroy them, restart PX and validate LUN on FA"
	It(stepLog, func() {
		log.InfoD(stepLog)
		volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
		log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())
		pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
		log.FailOnError(err, "failed to get secret [%s]  in namespace [%s]", PureSecretName, volDriverNamespace)
		flashArrays := pxPureSecret.Arrays

		if len(flashArrays) == 0 {
			log.FailOnError(fmt.Errorf("no FlashArrays details found"), fmt.Sprintf("error getting FlashArrays creds from %s [%s]", PureSecretName, pxPureSecret))
		}

		stepLog = "Create PVCs and restart PX"
		scName := "pure-blockfamgmt"
		nsName := "pvc-lun-ns"
		pvcPrefix := "falun-test"
		numPVCs := 501
		var createdPVCS []string
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.InfoD("creating storage class %s", scName)
			createSC := func(scName string) {
				params := make(map[string]string)
				params["repl"] = "1"
				params["priority_io"] = "high"
				params["io_profile"] = "auto"
				params["backend"] = "pure_block"

				v1obj := metav1.ObjectMeta{
					Name: scName,
				}
				reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
				bindMode := storageApi.VolumeBindingImmediate
				scObj := storageApi.StorageClass{
					ObjectMeta:        v1obj,
					Provisioner:       k8s.CsiProvisioner,
					Parameters:        params,
					ReclaimPolicy:     &reclaimPolicyDelete,
					VolumeBindingMode: &bindMode,
				}

				k8sStorage := storage.Instance()
				_, err = k8sStorage.CreateStorageClass(&scObj)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify sc [%s] creation", scName))
			}

			createNs := func(nsName string) {
				ns := &v1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: nsName,
					},
				}
				log.InfoD("Creating namespace %v", nsName)
				_, err = core.Instance().CreateNamespace(ns)

				if err != nil {
					if apierrors.IsAlreadyExists(err) {
						log.Infof("Namespace %s already exists. Skipping creation.", ns.Name)
					} else {
						log.FailOnError(err, fmt.Sprintf("error creating namespace [%s]", nsName))
					}
				}
			}

			createPVC := func(pvcName, scName, appNs string, errCh chan error, wg *sync.WaitGroup) {
				defer wg.Done()
				log.InfoD("creating PVC [%s] in namespace [%s]", pvcName, appNs)

				pvcObj := &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pvcName,
						Namespace: appNs,
					},
					Spec: v1.PersistentVolumeClaimSpec{
						AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
						StorageClassName: &scName,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceStorage: resource.MustParse("5Gi"),
							},
						},
					},
				}
				_, err = core.Instance().CreatePersistentVolumeClaim(pvcObj)
				if err != nil {
					errCh <- err
				}

			}

			createSC(scName)
			createNs(nsName)
			stNodes := node.GetStorageDriverNodes()
			var wg sync.WaitGroup
			errCh := make(chan error, numPVCs+len(stNodes)) // creating a buffered channel with length for worst case scenario failures
			for i := 1; i <= numPVCs; i++ {
				pvcName := fmt.Sprintf("%s-%d", pvcPrefix, i)
				wg.Add(1)
				go createPVC(pvcName, scName, nsName, errCh, &wg)
			}

			//restarting all volume driver nodes sequentially
			wg.Add(1)
			go func(errCh chan error, wg *sync.WaitGroup) {
				defer wg.Done()

				for _, stNode := range stNodes {
					restartNodes := []node.Node{stNode}
					err = Inst().V.StopDriver(restartNodes, false, nil)
					if err != nil {
						errCh <- err
						break
					}
					err = Inst().V.WaitDriverDownOnNode(stNode)
					if err != nil {
						errCh <- err
						break
					}
					err = Inst().V.StartDriver(stNode)
					if err != nil {
						errCh <- err
						break
					}
					err = Inst().V.WaitDriverUpOnNode(stNode, 5*time.Minute)
					if err != nil {
						errCh <- err
						break
					}
				}
			}(errCh, &wg)

			wg.Wait()
			close(errCh)

			if len(errCh) > 0 {
				for err := range errCh {
					log.Errorf("%v", err)
				}
				log.FailOnError(fmt.Errorf("error(s) occured while creating PVC and restarting PX on nodes"), "no errors should occur")
			}

		})

		pvcList, err := core.Instance().GetPersistentVolumeClaims(nsName, nil)
		log.FailOnError(err, fmt.Sprintf("error getting pvcs from namespace [%s]", nsName))
		log.Infof("len of pvc items: %d", len(pvcList.Items))
		for _, p := range pvcList.Items {
			//few PVCs are getting empty volume name, this is workaround for the fix
			pvc, err := core.Instance().GetPersistentVolumeClaim(p.Name, nsName)
			log.FailOnError(err, fmt.Sprintf("error getting pvc [%s] from namespace [%s]", p.Name, nsName))
			if pvc.Spec.VolumeName == "" {
				log.Errorf("volume name empty for [%v]", p)

			} else {
				createdPVCS = append(createdPVCS, pvc.Spec.VolumeName)
			}
			createdPVCS = append(createdPVCS, p.Spec.VolumeName)
		}

		for _, pvc := range pvcList.Items {
			err := Inst().S.WaitForSinglePVCToBound(pvc.Name, nsName)
			log.FailOnError(err, fmt.Sprintf("error validating PVC [%s] status in namespace [%s]", pvc.Name, nsName))
		}

		var faVols []string
		for _, fa := range flashArrays {
			v, err := pureutils.GetPureFAVolumes(fa.MgmtEndPoint, fa.APIToken)
			faVols = append(faVols, v...)
			log.FailOnError(err, "error getting vols using end point [%s],token [%s]", fa.MgmtEndPoint, fa.APIToken)
		}

		var missingVols []string

		for _, cv := range createdPVCS {
			if !faLUNExists(faVols, cv) {
				missingVols = append(missingVols, cv)
			}
		}

		dash.VerifyFatal(len(missingVols) == 0, true, fmt.Sprintf("validate all volumes are created in FA backend. Missing vols: [%v]", missingVols))

		log.InfoD("Destroying Volumes")
		err = core.Instance().DeleteNamespace(nsName)
		log.FailOnError(err, fmt.Sprintf("error deleting namespace [%s]", nsName))

		log.Infof("waiting for 5 mins allowing vols to delete")
		time.Sleep(5 * time.Minute)

		var faVolsAfterDel []string
		for _, fa := range flashArrays {
			v, err := pureutils.GetPureFAVolumes(fa.MgmtEndPoint, fa.APIToken)
			faVolsAfterDel = append(faVolsAfterDel, v...)
			log.FailOnError(err, "error getting vols using end point [%s],token [%s]", fa.MgmtEndPoint, fa.APIToken)
		}

		var existingVols []string
		for _, cv := range createdPVCS {
			if faLUNExists(faVolsAfterDel, cv) {
				existingVols = append(existingVols, cv)
			}
		}

		dash.VerifyFatal(len(existingVols) == 0, true, fmt.Sprintf("validate all volumes are deleted in FA backend. Existing vols: [%v]", existingVols))

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)

	})
})

func faLUNExists(faVolList []string, pvc string) bool {
	for _, v := range faVolList {
		if strings.Contains(v, pvc) {
			return true
		}
	}
	return false
}

var _ = Describe("{FADAVolMigrateValidation}", func() {

	/*
		          1. Attach FADA PVC on Node 1, confirm proper attachment.
			  2. Stop PX on Node 1, ensure volume persistence in multipath -ll.
		          3. Move deployment to Node 2, validate successful pod startup.
			  4. Paths on original node indicate failure. Restart PX on Node 1, confirm old multipath device absence.

	*/
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("FADAVolMigrateValidation", "Migrate pods from node 1 to node and check multipath consistency", nil, 0)
	})

	stepLog = "Schedule apps, migrate apps from node 1 to node 2 and check if new multipath has been updated and old multipath has been erased"
	It(stepLog, func() {
		log.InfoD(stepLog)

		//get device path of the volume
		devicePaths := make([]string, 0)

		stepLog = "Schedule fada deployment apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// select a node for apps to be scheduled
			applist := Inst().AppList
			storageNodes := node.GetStorageNodes()
			selectedNode := storageNodes[0]
			secondNode := storageNodes[1]
			log.Infof("Length of storage nodes: %v", len(storageNodes))
			log.InfoD("Selected Node: %v", selectedNode.Name)
			defer func() {
				Inst().AppList = applist
				err = Inst().S.RemoveLabelOnNode(selectedNode, "apptype")
				log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
			}()
			Inst().AppList = []string{"nginx-fada-deploy"}
			err = Inst().S.AddLabelOnNode(selectedNode, "apptype", k8s.PureDAVolumeLabelValueFA)

			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
			Provisioner := fmt.Sprintf("%v", portworx.PortworxCsi)

			stepLog = fmt.Sprintf("schedule application")
			Step(stepLog, func() {
				for i := 0; i < Inst().GlobalScaleFactor; i++ {
					taskName := fmt.Sprintf("vol-migrate-test-%v", i)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Provisioner,
					})
					log.FailOnError(err, "Failed to schedule application of %v namespace", taskName)
					contexts = append(contexts, context...)
				}
				ValidateApplications(contexts)
			})

			stepLog = fmt.Sprintf("Check where the apps is scheduled and Stop Px on that node")
			Step(stepLog, func() {
				//get the volume name and inspect volume to get device path
				for _, ctx := range contexts {
					volumes, err := Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes for app %v", ctx.App.Key)
					for _, volume := range volumes {
						volInspect, err := Inst().V.InspectVolume(volume.ID)
						log.FailOnError(err, "Failed to inspect volume %v", volume.ID)
						devicePath := volInspect.DevicePath
						// get part of the device path
						log.Infof("device path: %v", devicePath)
						devicePathSplit := strings.Split(devicePath, "/")
						devicePath = devicePathSplit[len(devicePathSplit)-1]
						devicePaths = append(devicePaths, devicePath)
						log.InfoD("Device path of the volume: %v , device path: %v", volumes[0].Name, devicePath)
					}
				}

				StopVolDriverAndWait([]node.Node{selectedNode})
			})
			defer func() {
				err = core.Instance().UnCordonNode(selectedNode.Name, defaultCommandTimeout, defaultCommandRetry)
				log.FailOnError(err, "Failed to uncordon node %v", selectedNode.Name)
				log.Infof("uncordoned node %v", selectedNode.Name)

				err = Inst().S.RemoveLabelOnNode(secondNode, "apptype")
				log.FailOnError(err, "error removing label on node [%s]", secondNode.Name)
			}()
			stepLog = "cordon the node where the app is scheduled and delete the apps"
			Step(stepLog, func() {

				err = core.Instance().CordonNode(selectedNode.Name, defaultCommandTimeout, defaultCommandRetry)
				log.FailOnError(err, "Failed to cordon node %v", selectedNode.Name)
				log.InfoD("cordoned node %v", selectedNode.Name)

				err = Inst().S.AddLabelOnNode(secondNode, "apptype", k8s.PureDAVolumeLabelValueFA)
				log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondNode.Name))

				// delete the pods and wait for it to delete
				var wg sync.WaitGroup
				for _, ctx := range contexts {
					wg.Add(1)
					go func(ctx *scheduler.Context) {
						defer wg.Done()
						defer GinkgoRecover()
						pods, err := core.Instance().GetPods(ctx.App.NameSpace, nil)
						for _, pod := range pods.Items {
							log.InfoD("Delete pod %v", pod.Name)
							err = core.Instance().DeletePod(pod.Name, ctx.App.NameSpace, true)
							log.FailOnError(err, "Failed to delete pod %v", pods.Items[0].Name)
							// wait for the pod to delete
							t := func() (interface{}, bool, error) {
								currentPodList, err := core.Instance().GetPods(ctx.App.NameSpace, nil)
								log.FailOnError(err, "Failed to get pods in namespace %v", ctx.App.NameSpace)
								for _, currentPod := range currentPodList.Items {
									log.InfoD("Delete pod %v", pod.Name)

									if currentPod.Name == pod.Name {
										log.FailOnError(fmt.Errorf("Pod %v is still present", pod.Name), "Pod %v should be deleted", pod.Name)
										return nil, true, nil
									}
								}
								return nil, false, nil
							}
							_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
							log.FailOnError(err, "Failed to wait for pods to delete")
						}
					}(ctx)
				}
				wg.Wait()
			})

			stepLog = "run the multipath -ll command on the node where the pods were scheduled before deleting"
			Step(stepLog, func() {
				// sleep for 60 seconds for all the entries to update
				time.Sleep(30 * time.Second)
				log.InfoD("Sleeping for 30 seconds for all the entries to update")
				cmd := fmt.Sprintf("multipath -ll")
				output, err := runCmd(cmd, selectedNode)
				log.FailOnError(err, "Failed to run multipath -ll command on node %v", selectedNode.Name)
				log.InfoD("Output of multipath on provisioned node -ll command: %v", output)
				//check if the device path is present in multipath
				if !strings.Contains(output, "failed faulty running") {
					log.FailOnError(fmt.Errorf("Multipath device error not detected"), "Multipath device error should be detected")
				}

				stepLog = "Check if pod is scheduled on other node and validate if the volume is attached on the new node"
				Step(stepLog, func() {
					pods, err := core.Instance().GetPods(contexts[0].App.NameSpace, nil)
					log.FailOnError(err, "Failed to get pods in namespace %v", contexts[0].App.NameSpace)
					for _, pod := range pods.Items {
						log.InfoD("Pod name: %v, node name: %v", pod.Name, pod.Spec.NodeName)
						if pod.Spec.NodeName != selectedNode.Name {
							log.InfoD("Pod %v is scheduled on node %v", pod.Name, pod.Spec.NodeName)
							break
						}
					}
				})
			})
			stepLog = "Start portworx on the node where the volume was attached"
			Step(stepLog, func() {
				StartVolDriverAndWait([]node.Node{selectedNode})
			})

			stepLog = "Check if the old multipath device entry is deleted from the node where the volume was attached"
			Step(stepLog, func() {
				//sleep for some time for the entries to update
				time.Sleep(30 * time.Second)

				//run the multipath -ll command on the node where the volume is attached
				cmd := fmt.Sprintf("multipath -ll")
				output, err := runCmd(cmd, selectedNode)
				log.FailOnError(err, "Failed to run multipath -ll command on node %v", selectedNode.Name)
				log.InfoD("Output of multipath -ll command: %v", output)
				//check if the device path is present in multipath
				for _, devicePath := range devicePaths {
					if strings.Contains(output, devicePath) {
						log.FailOnError(fmt.Errorf("Multipath device %v is still present", devicePath), "Multipath device %v should be deleted", devicePath)
					}
				}
				//check if the device path is present in multipath
				if strings.Contains(output, "failed faulty running") {
					log.FailOnError(fmt.Errorf("Multipath device error not detected"), "Multipath device error should be detected")
				}
				log.InfoD("Successfully validated that the old multipath device is deleted")
			})
			stepLog = "Destroy apps"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				DestroyApps(contexts, nil)
			})

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)

	})
})

var _ = Describe("{VolAttachFAPxRestart}", func() {
	/*
				https://purestorage.atlassian.net/browse/PTX-21440
			    1. Create a host in the FA whose secret is not present in pure secret
		        2. Create a volume and attach it to the host created in step 1
		        3. using iscsiadm commands run commands to login to the controllers
		        4. check multipath -ll output
		        5. Restart portworx
		        6. The multipath entry for the volume attached from a different FA shouldn't vanish
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("VolAttachFAPxRestart", "Attach a vol from a FA, restart portworx and check multipath consistency", nil, 0)
	})

	var (
		hostName               = fmt.Sprintf("torpedo-host-%v", time.Now().UnixNano())
		volumeName             = fmt.Sprintf("torpedo-vol-%v", time.Now().UnixNano())
		faSecret               = Inst().FaSecret
		FAclient               *flasharray.Client
		MultipathBeforeRestart string
		faMgmtEndPoint         string
		faAPIToken             string
		host                   *flasharray.Host
		IQNExists              bool
	)

	itLog := "Attach a volume from a different FA, restart portworx and check multipath consistency"
	It(itLog, func() {
		log.InfoD(itLog)
		// select a random node to run the test
		n := node.GetStorageDriverNodes()[0]

		stepLog := "get the secrete of FA which is not present in pure secret"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//get the flash array details
			volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
			log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())

			pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
			log.FailOnError(err, "Failed to get secret %v", pxPureSecret)
			flashArraysInSecret := pxPureSecret.Arrays

			if len(flashArraysInSecret) == 0 {
				log.FailOnError(fmt.Errorf("no FlashArrays details found"), fmt.Sprintf("error getting FlashArrays creds from %s [%s]", PureSecretName, pxPureSecret))
			}

			for _, value := range strings.Split(faSecret, ",") {
				faMgmtEndPoint = strings.Split(value, ":")[0]
				faAPIToken = strings.Split(value, ":")[1]
				if len(faMgmtEndPoint) == 0 || len(faAPIToken) == 0 {
					continue
				}
				log.InfoD("famanagement endpoint: %v, faAPIToken: %v", faMgmtEndPoint, faAPIToken)
				break
			}
			if len(faMgmtEndPoint) == 0 || len(faAPIToken) == 0 {
				log.FailOnError(fmt.Errorf("no FlashArrays details found"), fmt.Sprintf("error getting FlashArrays creds from %s [%s]", PureSecretName, pxPureSecret))
			}

			for _, fa := range flashArraysInSecret {
				if fa.MgmtEndPoint == faMgmtEndPoint {
					log.FailOnError(fmt.Errorf("Flash Array details present in secret"), "Flash Array details should not be present in the secret")
				}
			}
		})

		stepLog = "Create a volume, create a host, attach the volume to the host, update iqn of the host and attach the volume to the host"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			iqn, err := GetIQNOfNode(n)
			log.FailOnError(err, "Failed to get iqn of the node %v", n.Name)
			log.InfoD("Iqn of the node: %v", iqn)

			//create a connections to the FA whose credentials not present in the pure secret
			FAclient, err = pureutils.PureCreateClientAndConnect(faMgmtEndPoint, faAPIToken)
			log.FailOnError(err, "Failed to create client and connect to FA")

			// Check if the IQN of the node is present in the FA if present take the existing host else create one
			IQNExists, err = pureutils.IsIQNExistsOnFA(FAclient, iqn)
			log.FailOnError(err, "Failed to check if iqn exists on FA")

			if !IQNExists {
				//create a host in the FA
				host, err = pureutils.CreateNewHostOnFA(FAclient, hostName)
				log.FailOnError(err, "Failed to create host on FA")
				log.InfoD("Host created on FA: %v", host.Name)

				//Update iqn of the specific host
				_, err = pureutils.UpdateIQNOnSpecificHosts(FAclient, hostName, iqn)
				log.FailOnError(err, "Failed to update iqn on host %v", hostName)
				log.InfoD("Updated iqn on host %v", hostName)

			} else {
				// If iqn already exist in FA find the host which is using it
				host, err = pureutils.GetHostFromIqn(FAclient, iqn)
				log.FailOnError(err, "Failed to get host from FA")
				log.InfoD("Host already exists on FA: %v", host)
			}

			//create a volume on the FA
			volSize := 1048576 * rand.Intn(10)
			volume, err := pureutils.CreateVolumeOnFABackend(FAclient, volumeName, volSize)
			log.FailOnError(err, "Failed to create volume on FA")
			log.InfoD("Volume created on FA: %v", volume.Name)

			//Attach the volume to the host
			connectedVolume, err := pureutils.ConnectVolumeToHost(FAclient, host.Name, volumeName)
			log.FailOnError(err, "Failed to connect volume to host")
			log.InfoD("Volume connected to host: %v", connectedVolume.Name)

		})
		stepLog = "Run iscsiadm commands to login to the controllers"
		Step(stepLog, func() {

			//Run iscsiadm commands to login to the controllers
			networkInterfaces, err := pureutils.GetSpecificInterfaceBasedOnServiceType(FAclient, "iscsi")

			for _, networkInterface := range networkInterfaces {
				err = LoginIntoController(n, networkInterface, *FAclient)
				log.FailOnError(err, "Failed to login into controller")
				log.InfoD("Successfully logged into controller: %v", networkInterface.Address)
			}

			// run multipath after login
			cmd := "multipath -ll"
			MultipathBeforeRestart, err = runCmd(cmd, n)
			log.FailOnError(err, "Failed to run multipath -ll command on node %v", n.Name)
			log.InfoD("Output of multipath -ll command before restart: %v", MultipathBeforeRestart)

		})

		stepLog = "Restart portworx and check multipath consistency"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.StopDriver([]node.Node{n}, false, nil)
			log.FailOnError(err, fmt.Sprintf("Failed to stop portworx on node [%s]", n.Name))
			err = Inst().V.WaitDriverDownOnNode(n)
			log.FailOnError(err, fmt.Sprintf("Driver is up on node [%s]", n.Name))
			err = Inst().V.StartDriver(n)
			log.FailOnError(err, fmt.Sprintf("Failed to start portworx on node [%s]", n.Name))
			err = Inst().V.WaitDriverUpOnNode(n, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node [%s]", n.Name))
			dash.VerifyFatal(err == nil, true,
				fmt.Sprintf("PX is up after restarting on node [%s]", n.Name))

			time.Sleep(10 * time.Second)
			//run multipath after restart
			cmd := "multipath -ll"
			multipathAfterRestart, err := runCmd(cmd, n)
			log.FailOnError(err, "Failed to run multipath -ll command on node %v", n.Name)
			log.InfoD("Output of multipath -ll command after restart: %v", multipathAfterRestart)

			//check if the multipath entries are same before and after restart
			dash.VerifyFatal(MultipathBeforeRestart == multipathAfterRestart, true, "Multipath entries are same before and after restart")

		})

		stepLog = "Delete the volume and host from the FA"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//log out of all the controllers
			networkInterfaces, err := pureutils.GetSpecificInterfaceBasedOnServiceType(FAclient, "iscsi")

			for _, networkInterface := range networkInterfaces {
				err = LogoutFromController(n, networkInterface, *FAclient)
				log.FailOnError(err, "Failed to login into controller")
				log.InfoD("Successfully logged out of controller: %v", networkInterface.Address)
			}

			//disconnect volume from host
			_, err = pureutils.DisConnectVolumeFromHost(FAclient, hostName, volumeName)
			log.FailOnError(err, "Failed to disconnect volume from host")
			log.InfoD("Volume disconnected from host: %v", volumeName)

			//Delete the volume
			_, err = pureutils.DeleteVolumeOnFABackend(FAclient, volumeName)
			log.FailOnError(err, "Failed to delete volume on FA")
			log.InfoD("Volume deleted on FA: %v", volumeName)

			//Delete the host from FAbackend
			if !IQNExists {
				_, err = pureutils.DeleteHostOnFA(FAclient, hostName)
				log.FailOnError(err, "Failed to delete host on FA")
				log.InfoD("Host deleted on FA: %v", hostName)
			}
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

func LoginIntoController(n node.Node, networkInterface flasharray.NetworkInterface, FAclient flasharray.Client) error {
	ipAddress := networkInterface.Address
	iqn, err := GetIQNOfFA(n, FAclient)
	cmd := fmt.Sprintf("iscsiadm -m node -P %s -p %s -l", iqn, ipAddress)
	iscsiAdmOutput, err := runCmd(cmd, n)
	if err != nil {
		return err
	}
	log.InfoD("Output of iscsiadm login command: %v", iscsiAdmOutput)

	return nil
}

func LogoutFromController(n node.Node, networkInterface flasharray.NetworkInterface, FAclient flasharray.Client) error {
	ipAddress := networkInterface.Address
	iqn, err := GetIQNOfFA(n, FAclient)
	cmd := fmt.Sprintf("iscsiadm -m node -P %s -p %s --logout", iqn, ipAddress)
	iscsiAdmOutput, err := runCmd(cmd, n)
	if err != nil {
		return err
	}
	log.InfoD("Output of iscsiadm login command: %v", iscsiAdmOutput)

	return nil
}

var _ = Describe("{VolAttachSameFAPxRestart}", func() {
	/*
				https://purestorage.atlassian.net/browse/PTX-21440
			    1. Create a host in the FA whose secret is in pure secret
		        2. Create a volume and attach it to the host created in step 1
		        3. using iscsiadm commands run commands to login to the controllers
		        4. check multipath -ll output
		        5. Restart portworx
		        6. The multipath entry for the volume attached from a different FA shouldn't vanish and I/O should be consistent
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("VolAttachSameFAPxRestart", "Attach a vol from a FA, restart portworx and check multipath consistency", nil, 0)
	})

	var (
		hostName               = fmt.Sprintf("torpedo-host-%v", time.Now().UnixNano())
		volumeName             = fmt.Sprintf("torpedo-vol-%v", time.Now().UnixNano())
		FAclient               *flasharray.Client
		MultipathBeforeRestart string
		faMgmtEndPoint         string
		faAPIToken             string
		host                   *flasharray.Host
		volSize                int
		wg                     sync.WaitGroup
	)

	itLog := "Attach a volume from a different FA, restart portworx and check multipath consistency and I/O consistency"
	It(itLog, func() {
		log.InfoD(itLog)
		// select a random node to run the test
		n := node.GetStorageDriverNodes()[0]

		stepLog := "get the secrete of FA in pure secret"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//get the flash array details
			volDriverNamespace, err := Inst().V.GetVolumeDriverNamespace()
			log.FailOnError(err, "failed to get volume driver [%s] namespace", Inst().V.String())

			pxPureSecret, err := pureutils.GetPXPureSecret(volDriverNamespace)
			log.FailOnError(err, "Failed to get secret %v", pxPureSecret)
			flashArrays := pxPureSecret.Arrays

			if len(flashArrays) == 0 {
				log.FailOnError(fmt.Errorf("no FlashArrays details found"), fmt.Sprintf("error getting FlashArrays creds from %s [%s]", PureSecretName, pxPureSecret))
			}

			faMgmtEndPoint = flashArrays[0].MgmtEndPoint
			faAPIToken = flashArrays[0].APIToken
		})

		stepLog = "Create a volume, create a host, attach the volume to the host, update iqn of the host and attach the volume to the host"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			iqn, err := GetIQNOfNode(n)
			log.FailOnError(err, "Failed to get iqn of the node %v", n.Name)
			log.InfoD("Iqn of the node: %v", iqn)

			//create a connections to the FA whose credentials not present in the pure secret
			FAclient, err = pureutils.PureCreateClientAndConnect(faMgmtEndPoint, faAPIToken)
			log.FailOnError(err, "Failed to create client and connect to FA")

			// Check if the IQN of the node is present in the FA if present take the existing host else create one
			IQNExists, err := pureutils.IsIQNExistsOnFA(FAclient, iqn)
			log.FailOnError(err, "Failed to check if iqn exists on FA")

			if !IQNExists {
				//create a host in the FA
				host, err = pureutils.CreateNewHostOnFA(FAclient, hostName)
				log.FailOnError(err, "Failed to create host on FA")
				log.InfoD("Host created on FA: %v", host.Name)

				//Update iqn of the specific host
				_, err = pureutils.UpdateIQNOnSpecificHosts(FAclient, hostName, iqn)
				log.FailOnError(err, "Failed to update iqn on host %v", hostName)
				log.InfoD("Updated iqn on host %v", hostName)

			} else {
				// If iqn already exist in FA find the host which is using it
				host, err = pureutils.GetHostFromIqn(FAclient, iqn)
				log.FailOnError(err, "Failed to get host from FA")
				log.InfoD("Host already exists on FA: %v", host)
			}

			//create a volume on the FA
			volSize = 104857600000 * (rand.Intn(10) + 1)
			volume, err := pureutils.CreateVolumeOnFABackend(FAclient, volumeName, volSize)
			log.FailOnError(err, "Failed to create volume on FA")
			log.InfoD("Volume created on FA: %v", volume.Name)

			//Attach the volume to the host
			connectedVolume, err := pureutils.ConnectVolumeToHost(FAclient, host.Name, volumeName)
			log.FailOnError(err, "Failed to connect volume to host")
			log.InfoD("Volume connected to host: %v", connectedVolume.Name)

		})
		stepLog = "Run iscsiadm commands to login to the controllers"
		Step(stepLog, func() {

			//run multipath before refresh
			cmd := "multipath -ll"
			output, err := runCmd(cmd, n)
			log.FailOnError(err, "Failed to run multipath -ll command on node %v", n.Name)
			log.InfoD("Output of multipath -ll command before PX restart : %v", output)

			// Refresh the iscsi session
			err = RefreshIscsiSession(n)
			log.FailOnError(err, "Failed to refresh iscsi session")
			log.InfoD("Successfully refreshed iscsi session")

			//sleep for 10s for the entries to update
			time.Sleep(10 * time.Second)

			// run multipath after login
			cmd = "multipath -ll"
			MultipathBeforeRestart, err = runCmd(cmd, n)
			log.FailOnError(err, "Failed to run multipath -ll command on node %v", n.Name)
			log.InfoD("Output of multipath -ll command before PX restart : %v", MultipathBeforeRestart)

			// multipath before and after shoouldn't be same
			dash.VerifyFatal(MultipathBeforeRestart != output, true, "Multipath entries are different before and after refresh")

		})
		stepLog = "create ext4 file system on top of the volume,mount it to /home/test Start running fio on the volume"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//Get the device path of the volume
			cmd := "multipath -ll | grep dm-  | sort -n | tail -n 1"
			dm, err := runCmd(cmd, n)
			log.FailOnError(err, "Failed to get the device path of the volume")
			log.InfoD("Device path of the volume: %v", dm)
			dmPath := strings.Fields(dm)
			if len(dmPath) > 2 {
				dm = dmPath[1]
			} else {
				log.FailOnError(fmt.Errorf("Failed to get the device path of the volume"), "Failed to get the device path of the volume")
			}
			//create ext4 file system on top of the volume
			cmd = fmt.Sprintf("mkfs.ext4 /dev/%s", dm)
			_, err = runCmd(cmd, n)
			log.FailOnError(err, "Failed to create ext4 file system on the volume")
			log.InfoD("Successfully created ext4 file system on the volume")

			//Mount the volume to /home/test
			cmd = fmt.Sprintf("mkdir -p /home/test && mount /dev/%s /home/test", dm)
			_, err = runCmd(cmd, n)
			log.FailOnError(err, "Failed to mount the volume to /home/test")
			log.InfoD("Successfully mounted the volume to /home/test")

			//pick a random name for a file to write data into
			fileName := fmt.Sprintf("/home/test/fio-%v", time.Now().UnixNano())

			//Create a file with the random name
			cmd = fmt.Sprintf("touch %s", fileName)
			_, err = runCmd(cmd, n)
			log.FailOnError(err, "Failed to create a file with the random name")
			log.InfoD("Successfully created a file with the random name")

			//run fio on the volume

			wg.Add(1)
			go func() {
				defer wg.Done()
				fioCmd := fmt.Sprintf("fio --name=randwrite --ioengine=libaio --iodepth=32 --rw=randwrite --bs=4k --direct=1 --size=%vG --numjobs=1 --runtime=30 --time_based --group_reporting --filename=%s", volSize/2, fileName)
				_, err = runCmd(fioCmd, n)
				log.FailOnError(err, "Failed to run fio on the volume")
				log.InfoD("Successfully ran fio on the volume")
			}()

		})

		stepLog = "Restart portworx and check multipath consistency"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.StopDriver([]node.Node{n}, false, nil)
			log.FailOnError(err, fmt.Sprintf("Failed to stop portworx on node [%s]", n.Name))
			err = Inst().V.WaitDriverDownOnNode(n)
			log.FailOnError(err, fmt.Sprintf("Driver is up on node [%s]", n.Name))
			err = Inst().V.StartDriver(n)
			log.FailOnError(err, fmt.Sprintf("Failed to start portworx on node [%s]", n.Name))
			err = Inst().V.WaitDriverUpOnNode(n, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node [%s]", n.Name))
			dash.VerifyFatal(err == nil, true,
				fmt.Sprintf("PX is up after restarting on node [%s]", n.Name))

			//run multipath after restart
			cmd := "multipath -ll"
			multipathAfterRestart, err := runCmd(cmd, n)
			log.FailOnError(err, "Failed to run multipath -ll command on node %v", n.Name)

			//check if the multipath entries are same before and after restart
			dash.VerifyFatal(MultipathBeforeRestart == multipathAfterRestart, true, "Multipath entries are same before and after restart")

		})
		wg.Wait()

		stepLog = "Delete the volume and host from the FA"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//disconnect volume from host
			_, err = pureutils.DisConnectVolumeFromHost(FAclient, host.Name, volumeName)
			log.FailOnError(err, "Failed to disconnect volume from host")
			log.InfoD("Volume disconnected from host: %v", volumeName)

			//Delete the volume
			_, err = pureutils.DeleteVolumeOnFABackend(FAclient, volumeName)
			log.FailOnError(err, "Failed to delete volume on FA")
			log.InfoD("Volume deleted on FA: %v", volumeName)

			//Refresh the iscsi session
			err = RefreshIscsiSession(n)
			log.FailOnError(err, "Failed to refresh iscsi session")
			log.InfoD("Successfully refreshed iscsi session")

		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

/*
This test deploys app with FBDA volume having storageClass with pure_nfs_endpoint parameter.
It validates that FBDA volume gets consumed over IP mentioned in `pure_nfs_endpoint` parameter of storageClass.
*/
var _ = Describe("{FBDAMultiTenancyBasicTest}", func() {
	var contexts []*scheduler.Context
	var testName string
	var customConfigAppName string

	testName = "fbda-multitenancy"
	JustBeforeEach(func() {
		StartTorpedoTest("FBDAMultiTenancyBasicTest", "Validate FBDA vols get consumed over IP mentioned in `pure_nfs_endpoint` parameter of storageClass", nil, 0)
		Step("setup credential necessary for cloudsnap", createCloudsnapCredential)

		log.Infof("Using CustomAppConfig: %+v", Inst().CustomAppConfig)
		// Skip the test if we don't find any of our apps
		for appNameFromCustomAppConfig := range Inst().CustomAppConfig {
			found := false
			for _, appName := range Inst().AppList {
				if appName == appNameFromCustomAppConfig {
					found = true
					customConfigAppName = appNameFromCustomAppConfig
					break
				}
			}
			if !found {
				log.Warnf("App %v not found in %d contexts, skipping test", appNameFromCustomAppConfig, len(contexts))
				Skip(fmt.Sprintf("app %v not found", appNameFromCustomAppConfig))
			}
		}
		contexts = ScheduleApplications(testName)
		ValidateApplicationsPureSDK(contexts)

	})

	When("pure_nfs_endpoint parameter specified in storageClass", func() {
		It("should create a FBDA volume over pure_nfs_endpoint mentioned in storageClass", func() {
			ctx := findContext(contexts, customConfigAppName)

			vols, err := Inst().S.GetVolumes(ctx)
			dash.VerifyNotNilFatal(err, "Failed to get list of volumes")
			dash.VerifyFatal(len(vols) != 0, true, "Failed to get volumes")

			expectedPureNfsEndpoint := Inst().CustomAppConfig[customConfigAppName].StorageClassPureNfsEndpoint

			for _, vol := range vols {
				apiVol, err := Inst().V.InspectVolume(vol.ID)
				log.FailOnError(err, fmt.Sprintf("Failed to inspect volume [%s]", apiVol.GetId()))
				// Validate the volume is created over the NFS endpoint mentioned in storageClass
				dash.VerifyFatal(apiVol.Spec.ProxySpec.PureFileSpec.NfsEndpoint, expectedPureNfsEndpoint, "FBDA volume is not using NFS endpoint mentioned in storageClass.")
			}
		})

		JustAfterEach(func() {
			defer EndTorpedoTest()
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
			Step("delete credential used for cloudsnap", deleteCloudsnapCredential)
			AfterEachTest(contexts)
		})
	})
})

var _ = Describe("{FADAPodRecoveryDisableDataPortsOnFA}", func() {

	/*
			PTX : https://purestorage.atlassian.net/browse/PTX-23763
		Verify that FA Pods Recovers after bringing back network Interface down on all FA's

	*/
	JustBeforeEach(func() {
		log.Infof("Starting Torpedo tests ")
		StartTorpedoTest("FADAPodRecoveryDisableDataPortsOnFA",
			"Verify Pod Recovers from RO mode after Bounce after blocking network interface from FA end",
			nil, 0)
	})

	itLog := "FADAPodRecoveryDisableDataPortsOnFA"
	It(itLog, func() {

		var contexts []*scheduler.Context
		var k8sCore = core.Instance()
		//podNodes := []*corev1.Pod{}

		// Pick all the Volumes with RWO Status, We check if the Volume is with Access Mode RWO and PureBlock Volume
		vols := make([]*volume.Volume, 0)
		stepLog = "Schedule application"
		Step(stepLog, func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fapodrecovery-%d", i))...)
			}
		})

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		flashArrayGetIscsiPorts := func() map[string][]string {
			flashArrays, err := FlashArrayGetIscsiPorts()
			log.FailOnError(err, "Failed to Get Details on Flasharray iscsi ports that are in Use ")

			return flashArrays
		}

		interfaces := flashArrayGetIscsiPorts()
		log.Infof("Map of List [%v]", interfaces)

		disableInterfaces := func() {
			stepLog = "Disable all Data ports on the FA Controller"
			Step(stepLog, func() {
				for mgmtIp, iFaces := range interfaces {
					for _, eachIface := range iFaces {
						log.Infof("Disabling Network interfaces [%v] on FA Host [%v]", eachIface, mgmtIp)
						log.FailOnError(DisableFlashArrayNetworkInterface(mgmtIp, eachIface), "Disabling network interface failed?")
					}
				}
			})
		}

		enableInterfaces := func() {
			stepLog = "Enable all Data ports on the FA Controller"
			Step(stepLog, func() {
				for mgmtIp, iFaces := range interfaces {
					for _, eachIface := range iFaces {
						log.Infof("Enabling Interface [%v] on FA Host [%v]", eachIface, mgmtIp)
						log.FailOnError(EnableFlashArrayNetworkInterface(mgmtIp, eachIface), "Enabling Network interface failed?")
					}
				}
			})
		}

		defer enableInterfaces()

		stepLog = "Get all Volumes and Validate "
		Step(stepLog, func() {
			for _, ctx := range contexts {
				appVols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, fmt.Sprintf("error getting volumes for app [%s]", ctx.App.Key))
				vols = append(vols, appVols...)
			}
		})

		allStorageNodes := node.GetStorageNodes()

		stepLog = "Disable Data port on all FA's "
		Step(stepLog, func() {
			disableInterfaces()
		})

		// Sleep for sometime for PVC's to go in RO mode while data ingest in progress
		time.Sleep(15 * time.Minute)

		// Verify Px goes down on all the nodes present in the cluster
		for _, eachNodes := range allStorageNodes {
			log.FailOnError(Inst().V.WaitDriverDownOnNode(eachNodes), fmt.Sprintf("Driver on the Node [%v] is not down yet", eachNodes.Name))
		}

		stepLog = "Verify if pods are not in Running state after disabling "
		Step(stepLog, func() {
			for _, eachVol := range vols {
				// Pod details after blocking IP
				podsOnBlock, err := k8sCore.GetPodsUsingPVC(eachVol.Name, eachVol.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				// Verify that Pod Bounces and not in Running state till the time iscsi rules are not reverted
				for _, eachPodAfter := range podsOnBlock {
					if eachPodAfter.Status.Phase == "Running" {
						log.FailOnError(fmt.Errorf("pod is in Running State  [%v]",
							eachPodAfter.Status.HostIP), "Pod is in Running state")
					}
					log.Infof("Pod with Name [%v] placed on Host [%v] and Phase [%v]",
						eachPodAfter.Name, eachPodAfter.Status.HostIP, eachPodAfter.Status.Phase)
				}
			}
		})

		// Enable Back the network interface on all the FA CLuster
		enableInterfaces()

		// Sleep for some time for Px to come up online and working
		time.Sleep(10 * time.Minute)

		stepLog = "Verify that each pods comes back online once network restored"
		Step(stepLog, func() {

		})
		// Verify Px goes down on all the nodes present in the cluster
		for _, eachNodes := range allStorageNodes {
			log.FailOnError(Inst().V.WaitDriverUpOnNode(eachNodes, Inst().DriverStartTimeout),
				fmt.Sprintf("Driver on the Node [%v] is not Up yet", eachNodes.Name))
		}

		stepLog = "Verify Each pod in Running State after bringing back data ports"
		Step(stepLog, func() {
			for _, eachVol := range vols {
				// Pod details after blocking IP
				podsAfterRevert, err := k8sCore.GetPodsUsingPVC(eachVol.Name, eachVol.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				for _, eachPod := range podsAfterRevert {
					if eachPod.Status.Phase != "Running" {
						log.FailOnError(fmt.Errorf("Pod didn't bounce on the node [%v]",
							eachPod.Status.HostIP), "Pod didn't bounce on the node")
					}
				}
			}
		})

	})

	JustAfterEach(func() {
		log.Infof("In Teardown")
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
