package lib

import (
	"errors"
	"fmt"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	dataservices "github.com/portworx/torpedo/drivers/pds/dataservice"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/node"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	_ "github.com/portworx/torpedo/drivers/scheduler/dcos"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/tests"
)

const (
	PdsDeploymentControllerManagerPod   = "pds-deployment-controller-manager"
	PdsAgentPod                         = "pds-agent"
	PdsTeleportPod                      = "pds-teleport"
	PdsBackupControllerPod              = "pds-backup-controller-manager"
	PdsTargetControllerPod              = "pds-operator-target-controller-manager"
	ActiveNodeRebootDuringDeployment    = "active-node-reboot-during-deployment"
	KillDeploymentControllerPod         = "kill-deployment-controller-pod-during-deployment"
	RestartPxDuringDSScaleUp            = "restart-portworx-during-ds-scaleup"
	RebootNodesDuringDeployment         = "reboot-multiple-nodes-during-deployment"
	KillAgentPodDuringDeployment        = "kill-agent-pod-during-deployment"
	RestartAppDuringResourceUpdate      = "restart-app-during-resource-update"
	UpdateTemplate                      = "Medium"
	RebootNodeDuringAppVersionUpdate    = "reboot-node-during-app-version-update"
	KillTeleportPodDuringDeployment     = "kill-teleport-pod-during-deployment"
	RestoreDSDuringPXPoolExpansion      = "restore-ds-during-px-pool-expansion"
	RestoreDSDuringKVDBFailOver         = "restore-ds-during-kvdb-fail-over"
	RestoreDuringAllNodesReboot         = "restore-ds-during-node-reboot"
	StopPXDuringStorageResize           = "stop-px-during-storage-resize"
	RebootNodeDuringAppResourceUpdate   = "reboot-node-during-app-resource-update"
	KillDbMasterNodeDuringStorageResize = "kill-db-master-node-during-storage-resize"
	poolResizeTimeout                   = time.Minute * 120
	retryTimeout                        = time.Minute * 2
)

// PDS vars
var (
	dataservice               *dataservices.DataserviceType
	wg                        sync.WaitGroup
	ResiliencyFlag            = false
	hasResiliencyConditionMet = false
	FailureType               TypeOfFailure
	CapturedErrors            = make(chan error, 10)
	checkTillReplica          int32
	ResiliencyCondition       = make(chan bool)
	restoredDeployment        *pds.ModelsDeployment
	dsEntity                  restoreBkp.DSEntity
	DynamicDeployments        []*pds.ModelsDeployment
	RestoredDeployments       []*pds.ModelsDeployment
)

// Struct Definition for kind of Failure the framework needs to trigger
type TypeOfFailure struct {
	Type   string
	Method func() error
}

// Wrapper to Define failure type from Test Case
func DefineFailureType(failuretype TypeOfFailure) {
	FailureType = failuretype
}

// Executes all methods in parallel
func ExecuteInParallel(functions ...func()) {
	wg.Add(len(functions))
	defer wg.Wait()
	for _, fn := range functions {
		go func(FuncToRun func()) {
			defer wg.Done()
			FuncToRun()
		}(fn)
	}
}

// Function to enable Resiliency Test
func MarkResiliencyTC(resiliency bool) {
	ResiliencyFlag = resiliency
}

// Function to wait for event to induce failure
func InduceFailure(failure string, ns string) {
	isResiliencyConditionset := <-ResiliencyCondition
	if isResiliencyConditionset {
		FailureType.Method()
	} else {
		CapturedErrors <- errors.New("Resiliency Condition did not meet. Failing this test case.")
		return
	}
	return
}

// Close all open Resiliency channels here
func CloseResiliencyChannel() {
	// Close the Channel if it's empty. Otherwise there is no need to close as per Golang official documentation,
	// as far as we are making sure no writes are happening to a closed channel. Make sure to call this method only
	// during Post Test Case execution to avoid any unknown panics
	if len(ResiliencyCondition) == 0 {
		close(ResiliencyCondition)
	}
}

func InduceFailureAfterWaitingForCondition(deployment *pds.ModelsDeployment, namespace string, CheckTillReplica int32) error {
	switch FailureType.Type {
	// Case when we want to reboot a node onto which a deployment pod is coming up
	case ActiveNodeRebootDuringDeployment:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will reboot the node it is hosted upon.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RebootNodeDuringAppVersionUpdate:
		log.InfoD("Entering to check if Data service pods started update " +
			"Once it does, we restart portworx")
		func1 := func() {
			CheckPodIsTerminating(deployment.GetClusterResourceName(), namespace)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RestartPxDuringDSScaleUp:
		log.InfoD("Entering to check if Data service has %v active pods. "+
			"Once it does, we restart portworx", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, CheckTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case KillDeploymentControllerPod:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will kill the deployment Controller Pod.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RestartAppDuringResourceUpdate:
		log.InfoD("Entering to check if Data service has %v active pods. "+
			"Once it does, we restart application pods", checkTillReplica)
		func1 := func() {
			UpdateDeploymentResourceConfig(deployment, namespace, UpdateTemplate)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RebootNodesDuringDeployment:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will start rebooting all worker nodes.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case KillAgentPodDuringDeployment:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will kill the Agent Pod.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case KillTeleportPodDuringDeployment:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will kill the Agent Pod.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RestoreDSDuringPXPoolExpansion:
		log.InfoD("Entering to restore the Data service, while it does increase the PX-Pool size")
		func1 := func() {
			RestoreAndValidateConfiguration(namespace, deployment)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RestoreDSDuringKVDBFailOver:
		log.InfoD("Entering to restore the Data service, while the KVDB pods are down")
		func1 := func() {
			RestoreAndValidateConfiguration(namespace, deployment)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RestoreDuringAllNodesReboot:
		log.InfoD("Entering to restore the Data service, while the nodes are rebooted")
		func1 := func() {
			RestoreAndValidateConfiguration(namespace, deployment)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case StopPXDuringStorageResize:
		log.InfoD("Entering to resize of the Data service Volume, while PX on volume node is stopped")
		func1 := func() {
			ResizeDataserviceStorage(deployment, namespace, UpdateTemplate)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case KillDbMasterNodeDuringStorageResize:
		log.InfoD("Entering to resize of the Data service Volume, while PX on volume node is stopped")
		func1 := func() {
			ResizeDataserviceStorage(deployment, namespace, UpdateTemplate)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	case RebootNodeDuringAppResourceUpdate:
		checkTillReplica = CheckTillReplica
		log.InfoD("Entering to check if Data service has %v active pods. Once it does, we will start rebooting all worker nodes.", checkTillReplica)
		func1 := func() {
			GetPdsSs(deployment.GetClusterResourceName(), namespace, checkTillReplica)
		}
		func2 := func() {
			InduceFailure(FailureType.Type, namespace)
		}
		ExecuteInParallel(func1, func2)
	}

	var aggregatedError error
	for w := 1; w <= len(CapturedErrors); w++ {
		if err := <-CapturedErrors; err != nil {
			aggregatedError = fmt.Errorf("%v : %v", aggregatedError, err)
		}
	}
	if aggregatedError != nil {
		return aggregatedError
	}
	//validate method needs to be called from the testcode
	err := dataservice.ValidateDataServiceDeployment(deployment, namespace)
	return err
}

func RestartPXDuringDSScaleUp(ns string, deployment *pds.ModelsDeployment) error {
	// Get StatefulSet Object
	var ss *v1.StatefulSet
	var testError error

	//Waiting till pod have a node assigned
	var pods []corev1.Pod
	var nodeToRestartPX node.Node
	var nodeName string
	var podName string
	err = wait.Poll(resiliencyInterval, timeOut, func() (bool, error) {
		ss, testError = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), ns)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Get Pods of this StatefulSet
		pods, testError = k8sApps.GetStatefulSetPods(ss)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Check if the new Pod have a node assigned or it's in a window where it's just coming up
		podCount := 0
		for _, pod := range pods {
			log.Infof("Nodename of pod %v is :%v:", pod.Name, pod.Spec.NodeName)
			if pod.Spec.NodeName == "" || pod.Spec.NodeName == " " {
				log.Infof("Pod %v still does not have a node assigned. Retrying in 5 seconds", pod.Name)
				return false, nil
			} else {
				podCount += 1
				log.Debugf("No of pods that has node assigned: %d", podCount)
			}
			if int32(podCount) == *ss.Spec.Replicas {
				log.Debugf("Expected pod %v has node %v assigned", pod.Name, pod.Spec.NodeName)
				nodeName = pod.Spec.NodeName
				podName = pod.Name
				return true, nil
			}
		}
		return true, nil
	})
	nodeToRestartPX, testError = node.GetNodeByName(nodeName)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}

	log.InfoD("Going ahead and restarting PX the node %v as there is an "+
		"application pod %v that's coming up on this node", nodeName, podName)
	testError = tests.Inst().V.RestartDriver(nodeToRestartPX, nil)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}

	log.InfoD("PX restarted successfully on node %v", podName)
	return testError
}

// RestoreAndValidateConfiguration triggers restore of DS and validates configuration post restore
func RestoreAndValidateConfiguration(ns string, deployment *pds.ModelsDeployment) (bool, error) {
	//Get Cluster context and create restoreClient obj
	RestoredDeployments = []*pds.ModelsDeployment{}
	ctx, err := tests.GetSourceClusterConfigPath()
	log.FailOnError(err, "failed while getting src cluster path")
	restoreTarget := tc.NewTargetCluster(ctx)
	restoreClient := restoreBkp.RestoreClient{
		TenantId:             deployment.GetTenantId(),
		ProjectId:            deployment.GetProjectId(),
		Components:           components,
		Deployment:           deployment,
		RestoreTargetCluster: restoreTarget,
	}
	dsEntity = restoreBkp.DSEntity{
		Deployment: deployment,
	}
	//List all backups created on the deployment and trigger restore
	backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(restoreClient.ProjectId, deployment.GetId())
	log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
	for _, backupJob := range backupJobs {
		log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
		restoredModel, error := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), ns, dsEntity, true, false)
		if error != nil {
			CapturedErrors <- error
			return false, error
		}
		if ResiliencyFlag {
			ResiliencyCondition <- true
		}
		restoredDeployment, error = restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
		if error != nil {
			CapturedErrors <- error
			return false, error
		}
		DynamicDeployments = append(DynamicDeployments, restoredDeployment)
		RestoredDeployments = append(RestoredDeployments, restoredDeployment)
		log.InfoD("Restored successfully. Details: Deployment- %v", restoredDeployment.GetClusterResourceName())
	}
	return true, nil
}

func ResizeDataserviceStorage(deployment *pds.ModelsDeployment, namespace string, resourceTemplate string) (bool, error) {
	log.Debugf("Starting to resize the storage and UpdateDeploymentResourceConfig")
	var (
		resourceTemplateId string
		cpuLimits          int64
		initialCapacity    string
		updatedCapacity    string
	)
	initialCapacity = *deployment.Resources.StorageRequest
	log.InfoD("Initial volume storage size is : %v", initialCapacity)
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(*deployment.TenantId)
	if err != nil {
		if ResiliencyFlag {
			CapturedErrors <- err
		}
		return false, err
	}
	for _, template := range resourceTemplates {
		log.Debugf("template - %v", template.GetName())
		if template.GetDataServiceId() == deployment.GetDataServiceId() && strings.ToLower(template.GetName()) == strings.ToLower(resourceTemplate) {
			cpuLimits, _ = strconv.ParseInt(template.GetCpuLimit(), 10, 64)
			log.Debugf("CpuLimit - %v, %T", cpuLimits, cpuLimits)
			resourceTemplateId = template.GetId()
		}
	}
	if resourceTemplateId == "" {
		return false, fmt.Errorf("resource template - {%v} , not found", resourceTemplate)
	}
	if appConfigTemplateID == "" {
		appConfigTemplateID, err = controlplane.GetAppConfTemplate(*deployment.TenantId, *deployment.Name)
		log.FailOnError(err, "Error while fetching AppConfigID")
	}
	log.Infof("Deployment details: Ds id- %v, appConfigTemplateID - %v, imageId - %v, Node count -%v, resourceTemplateId- %v ", deployment.GetId(),
		appConfigTemplateID, deployment.GetImageId(), deployment.GetNodeCount(), resourceTemplateId)
	updatedDeployment, err := components.DataServiceDeployment.UpdateDeployment(deployment.GetId(),
		appConfigTemplateID, deployment.GetImageId(), deployment.GetNodeCount(), resourceTemplateId, nil)
	if err != nil {
		CapturedErrors <- err
		return false, err
	}
	if ResiliencyFlag {
		ResiliencyCondition <- true
	}
	log.InfoD("Resiliency Condition is met, now proceeding to validate if storage size is increased.")
	err = dataservice.ValidateDataServiceDeployment(updatedDeployment, namespace)
	log.FailOnError(err, "Error while validating dataservices")
	log.InfoD("Data-service: %v is up and healthy", updatedDeployment.Name)
	updatedCapacity = updatedDeployment.Resources.GetStorageRequest()
	if updatedCapacity > initialCapacity {
		log.InfoD("Initial PVC Capacity is- %v and Updated PVC Capacity is- %v", initialCapacity, updatedCapacity)
		log.InfoD("Storage is Successfully increased to  [%v]", updatedCapacity)
	} else {
		log.FailOnError(err, "Failed to verify Storage Resize at PV/PVC level")
	}
	return true, nil
}

// GetDynamicDeployments will fetch deployments created as part of resiliency InduceFailure call
func GetDynamicDeployments() []*pds.ModelsDeployment {
	log.InfoD("Dynamically created deployments are - %v", DynamicDeployments)
	return DynamicDeployments

}

func GetRestoredDeployment() []*pds.ModelsDeployment {
	log.InfoD("Length of Restored deployment - %v", len(RestoredDeployments))
	return RestoredDeployments
}

func NodeRebootDurinAppVersionUpdate(ns string, deployment *pds.ModelsDeployment) error {
	// Get StatefulSet Object
	var ss *v1.StatefulSet
	var testError error
	var nodeToReboot node.Node
	var nodeName, podName string

	// Waiting till atleast first pod have a node assigned
	var pods []corev1.Pod
	err = wait.PollImmediate(resiliencyInterval, timeOut, func() (bool, error) {
		ss, testError = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), ns)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Get Pods of this StatefulSet
		pods, testError = k8sApps.GetStatefulSetPods(ss)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Check if Pods have a node assigned or it's in a window where it's just coming up
		for _, pod := range pods {
			log.Infof("Nodename of pod %v is %v and deletiontimestamp is %v", pod.Name, pod.Spec.NodeName, pod.DeletionTimestamp)
			if pod.DeletionTimestamp != nil {
				podName = pod.Name
				nodeName = pod.Spec.NodeName
				return true, nil
			} else {
				return false, nil
			}
		}
		return true, nil
	})
	nodeToReboot, testError = node.GetNodeByName(nodeName)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	log.InfoD("Going ahead and restarting PX the node %v as there is an "+
		"application pod %v that's coming up on this node", nodeName, podName)

	testError = tests.Inst().N.RebootNode(nodeToReboot, node.RebootNodeOpts{
		Force: true,
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         defaultCommandTimeout,
			TimeBeforeRetry: defaultCommandRetry,
		},
	})
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	log.Infof("Node %v rebooted successfully", nodeName)
	return testError
}

// Reboot the Active Node onto which the application pod is coming up
func RebootActiveNodeDuringDeployment(ns string, deployment *pds.ModelsDeployment, num_reboots int) error {
	// Get StatefulSet Object
	var ss *v1.StatefulSet
	var testError error

	// Waiting till atleast first pod have a node assigned
	var pods []corev1.Pod
	err = wait.Poll(resiliencyInterval, timeOut, func() (bool, error) {
		ss, testError = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), ns)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Get Pods of this StatefulSet
		pods, testError = k8sApps.GetStatefulSetPods(ss)
		if testError != nil {
			CapturedErrors <- testError
			return false, testError
		}
		// Check if Pods have a node assigned or it's in a window where it's just coming up
		for _, pod := range pods {
			log.Infof("Nodename of pod %v is :%v:", pod.Name, pod.Spec.NodeName)
			if pod.Spec.NodeName == "" || pod.Spec.NodeName == " " {
				log.Infof("Pod %v still does not have a node assigned. Retrying in 5 seconds", pod.Name)
				return false, nil
			} else {
				return true, nil
			}
		}
		return true, nil
	})

	// Check which Pod is still not up. Try to reboot the node on which this Pod is hosted.
	for _, pod := range pods {
		log.Infof("Checking Pod %v running on Node: %v", pod.Name, pod.Spec.NodeName)
		if k8sCore.IsPodReady(pod) {
			log.InfoD("This Pod running on Node %v is Ready so skipping this pod......", pod.Spec.NodeName)
			continue
		} else {
			var nodeToReboot node.Node
			nodeToReboot, testError := node.GetNodeByName(pod.Spec.NodeName)
			if testError != nil {
				CapturedErrors <- testError
				return testError
			}
			if nodeToReboot.Name == "" {
				testError = errors.New("Something happened and node is coming out to be empty from Node registry")
				CapturedErrors <- testError
				return testError
			}
			log.Infof("Going ahead and rebooting the node %v as there is an application pod thats coming up on this node", pod.Spec.NodeName)
			testError = tests.Inst().N.RebootNode(nodeToReboot, node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         defaultCommandTimeout,
					TimeBeforeRetry: defaultCommandRetry,
				},
			})
			if testError != nil {
				CapturedErrors <- testError
				return testError
			}
			if num_reboots > 1 {
				for index := 1; index <= num_reboots; index++ {
					log.Infof("wait for node: %s to be back up", nodeToReboot.Name)
					err = tests.Inst().N.TestConnection(nodeToReboot, node.ConnectionOpts{
						Timeout:         defaultTestConnectionTimeout,
						TimeBeforeRetry: defaultWaitRebootRetry,
					})
					if err != nil {
						CapturedErrors <- err
						return err
					}
					testError = tests.Inst().N.RebootNode(nodeToReboot, node.RebootNodeOpts{
						Force: true,
						ConnectionOpts: node.ConnectionOpts{
							Timeout:         defaultCommandTimeout,
							TimeBeforeRetry: defaultCommandRetry,
						},
					})
					if testError != nil {
						CapturedErrors <- testError
						return testError
					}
				}
			}
			log.Infof("Node %v rebooted successfully", pod.Spec.NodeName)
		}
	}
	return testError
}

// Reboot All Worker Nodes while restore is ongoing
func RebootWorkernodesDuringRestore(ns string, deployment *pds.ModelsDeployment, testType string) error {
	// Static sleep to make sure busy box pod is scheduled
	time.Sleep(20 * time.Second)
	err = RebootWorkerNodesDuringDeployment(ns, deployment, testType)
	if err != nil {
		return err
	}
	return nil
}

// Reboot All Worker Nodes while deployment is ongoing
func RebootWorkerNodesDuringDeployment(ns string, deployment *pds.ModelsDeployment, testType string) error {
	// Waiting till atleast first pod have a node assigned
	var pods []corev1.Pod
	err = wait.Poll(resiliencyInterval, timeOut, func() (bool, error) {
		ss, err := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), ns)
		if err != nil {
			CapturedErrors <- err
			return false, err
		}
		// Get Pods of this StatefulSet
		pods, err = k8sApps.GetStatefulSetPods(ss)
		if err != nil {
			CapturedErrors <- err
			return false, err
		}
		// Check if Pods have a node assigned or it's in a window where it's just coming up
		for _, pod := range pods {
			log.Infof("Nodename of pod %v is :%v:", pod.Name, pod.Spec.NodeName)
			if pod.Spec.NodeName == "" || pod.Spec.NodeName == " " {
				log.Infof("Pod %v still does not have a node assigned. Retrying in 5 seconds", pod.Name)
				return false, nil
			} else {
				return true, nil
			}
		}
		return true, nil
	})
	if err != nil {
		CapturedErrors <- err
		return err
	}
	// Reboot Worker Nodes depending on Test Type (all or quorum)
	nodesToReboot := node.GetWorkerNodes()
	if testType == "quorum" {
		num_nodes := len(nodesToReboot)
		if num_nodes < 3 {
			nodesToReboot = nodesToReboot[0:2]
		}
		quorum_nodes := (num_nodes / 2) + 1
		log.InfoD("Total number of nodes in Cluter: %v", num_nodes)
		log.InfoD("Rebooting %v nodes in Cluster", quorum_nodes)
		nodesToReboot = nodesToReboot[0:quorum_nodes]
	}

	for _, n := range nodesToReboot {
		log.InfoD("reboot node: %s", n.Name)
		err = tests.Inst().N.RebootNode(n, node.RebootNodeOpts{
			Force: true,
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         defaultCommandTimeout,
				TimeBeforeRetry: defaultCommandRetry,
			},
		})
		if err != nil {
			CapturedErrors <- err
			return err
		}

		log.Infof("wait for node: %s to be back up", n.Name)
		err = tests.Inst().N.TestConnection(n, node.ConnectionOpts{
			Timeout:         defaultTestConnectionTimeout,
			TimeBeforeRetry: defaultWaitRebootRetry,
		})
		if err != nil {
			CapturedErrors <- err
			return err
		}
	}
	return nil
}

// Kill All pods matching podName string in a given namespace
func KillPodsInNamespace(ns string, podName string) error {
	var Pods []corev1.Pod
	// Fetch All the pods in pds-system namespace
	podList, testError := GetPods(ns)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	// Get List of All Pods matching with the name : podName
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, podName) {
			log.Infof("Pod Name is : %v", pod.Name)
			Pods = append(Pods, pod)
		}
	}
	// Kill All Pods matching with podName
	for _, pod := range Pods {
		log.InfoD("Deleting Pod: %s", pod.Name)
		testError = DeleteK8sPods(pod.Name, ns)
		if testError != nil {
			CapturedErrors <- testError
			return testError
		}
		log.InfoD("Successfully Killed Pod: %v", pod.Name)
	}
	return testError
}

func RestartApplicationDuringResourceUpdate(ns string, deployment *pds.ModelsDeployment) error {
	var ss *v1.StatefulSet
	ss, testError := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), ns)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	// Get Pods of this StatefulSet
	pods, testError := k8sApps.GetStatefulSetPods(ss)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	rand.Seed(time.Now().Unix())
	pod := pods[rand.Intn(len(pods))]
	// Delete the deployment Pods during update.
	testError = DeleteK8sPods(pod.Name, ns)
	if testError != nil {
		CapturedErrors <- testError
		return testError
	}
	return testError
}
