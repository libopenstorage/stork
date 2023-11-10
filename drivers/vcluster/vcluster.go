package vcluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapclientset "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	UpdatedClusterContext string
	CurrentClusterContext string
	ContextChange         = false
	NginxApp              = "nginx"
	k8sCore               = core.Instance()
	k8sStorage            = storage.Instance()
	k8sExternalStorage    = externalstorage.Instance()
	ControlNodeIP         string
)

const (
	vClusterCreationTimeout   = 5 * time.Minute
	VClusterRetryInterval     = 2 * time.Second
	VclusterConnectionTimeout = 60 * time.Second
	VclusterAppTimeout        = 30 * time.Minute
	VClusterAppRetryInterval  = 30 * time.Second
	ClusterWideSecretKey      = "cluster-wide-secret-key"
	PxNamespace               = "kube-system"
)

type VCluster struct {
	Namespace  string
	Name       string
	NodePort   int32
	Clientset  *kubernetes.Clientset
	SnapClient *snapclientset.Clientset
}

type FIOOptions struct {
	Name       string
	IOEngine   string
	RW         string
	BS         string
	NumJobs    int
	Size       string
	TimeBased  bool
	Runtime    string
	Filename   string
	EndFsync   int
	DoVerify   *int
	Verify     *string
	VerifyOnly bool
}

// NewVCluster Creates instance of Vcluster
func NewVCluster(name string) (*VCluster, error) {
	err := SetDefaultStorageClass()
	if err != nil {
		return nil, err
	}
	namespace := fmt.Sprintf("ns-%v-%v", name, time.Now().Unix())
	log.Infof("Namespace for Vcluster %v is : %v", name, namespace)
	return &VCluster{Namespace: namespace, Name: name}, nil
}

// ExecuteVClusterCommand executes any generic vCluster command
func ExecuteVClusterCommand(args ...string) (string, error) {
	cmd := exec.Command("vcluster", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed executing vcluster command with args %v: %w, Output: %s", args, err, output)
	}
	return string(output), nil
}

// TerminateVCluster Terminates Vcluster and runs it in its own context
func (v *VCluster) TerminateVCluster() error {
	_, err := ExecuteVClusterCommand("delete", v.Name)
	return err
}

// CreateVCluster This method creates a vcluster. This requires vcluster.yaml saved in a specific location.
func CreateVCluster(vclusterName, absPath, namespace string) error {
	_, err := ExecuteVClusterCommand("create", vclusterName, "-n", namespace, "-f", absPath, "--connect=false")
	if err != nil {
		return err
	}
	log.Infof("vCluster with the name %v created successfully", vclusterName)
	return nil
}

// SetDefaultStorageClass This method sets a default storage class if there is none set already
func SetDefaultStorageClass() error {
	// Check if there is already a default storage class
	defaultSCs, err := k8sStorage.GetDefaultStorageClasses()
	if err != nil {
		return err
	}
	// If there is no default storage class, set "px-db" as the default
	if len(defaultSCs.Items) == 0 {
		log.Infof("No default StorageClass set. Setting 'px-db' as default StorageClass.")
		err := k8sStorage.AnnotateStorageClassAsDefault("px-db")
		if err != nil {
			return err
		}
		log.Infof("'px-db' successfully set as default StorageClass.")
	} else {
		log.Infof("Default StorageClass already exists. No changes made.")
	}
	return nil
}

// WaitForVClusterRunning This method waits for vcluster to come up in Running state and waits for a specific timeout to throw an error
func WaitForVClusterRunning(vclusterName string, timeout time.Duration) error {
	f := func() (interface{}, bool, error) {
		output, err := ExecuteVClusterCommand("list")
		if err != nil {
			return nil, true, err
		}
		if strings.Contains(output, vclusterName) && strings.Contains(output, "Running") {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Vcluster is not yet in running state")
	}
	_, err := task.DoRetryWithTimeout(f, vClusterCreationTimeout, VClusterRetryInterval)
	return err
}

// GetControlNodeIP fetches the control node IP of host cluster to add it in vcluster config yaml file.
func GetControlNodeIP() (string, error) {
	nodes, err := k8sCore.GetNodes()
	if err != nil {
		return "", err
	}
	for _, node := range nodes.Items {
		_, existsMaster := node.ObjectMeta.Labels["node-role.kubernetes.io/master"]
		_, existsControlPlane := node.ObjectMeta.Labels["node-role.kubernetes.io/control-plane"]
		if existsMaster || existsControlPlane {
			for _, addr := range node.Status.Addresses {
				if addr.Type == v1.NodeInternalIP {
					return addr.Address, nil
				}
			}
		}
	}
	return "", fmt.Errorf("control node IP not found")
}

// UpdateVClusterConfig creates a yaml file from original yaml file to suit the host cluster.
// This yaml will be used to create a vcluster and connect to it later.
func UpdateVClusterConfig(inputFile, outputFile, controlNodeIP string) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		return err
	}
	var config map[string]interface{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return err
	}
	syncer := map[string]interface{}{
		"extraArgs": []string{"--tls-san=" + controlNodeIP},
	}
	config["syncer"] = syncer
	updatedData, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	err = os.WriteFile(outputFile, updatedData, 0644)
	if err != nil {
		return err
	}
	return nil
}

// CreateAndWaitVCluster method creates and waits for vcluster
func (v *VCluster) CreateAndWaitVCluster() error {
	currentDir, err := os.Getwd()
	if err != nil {
		return err
	}
	if ControlNodeIP == "" {
		ControlNodeIP, err = GetControlNodeIP()
		if err != nil {
			return err
		}
	}
	log.Infof("Control Node IP: %v", ControlNodeIP)
	sampleVclusterConfig := filepath.Join(currentDir, "..", "drivers", "vcluster", "vcluster.yaml")
	sampleVclusterConfigAbsPath, err := filepath.Abs(sampleVclusterConfig)
	if err != nil {
		return err
	}

	vcluster_config_filename := "vcluster-" + v.Name + ".yaml"
	vClusterPath := filepath.Join(currentDir, vcluster_config_filename)
	absPath, err := filepath.Abs(vClusterPath)
	if err != nil {
		return err
	}
	err = UpdateVClusterConfig(sampleVclusterConfigAbsPath, absPath, ControlNodeIP)
	if err != nil {
		return err
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Namespace,
			Labels: map[string]string{
				"type": "vcluster",
			},
		},
	}
	if _, err = k8sCore.CreateNamespace(ns); err != nil {
		return err
	}
	if err = v.CreateNodePortService(); err != nil {
		return err
	}
	if err = CreateVCluster(v.Name, absPath, v.Namespace); err != nil {
		return err
	}
	if err = v.SetClientSetForVCluster(); err != nil {
		return err
	}
	if err = WaitForVClusterRunning(v.Name, 10*time.Minute); err != nil {
		return err
	}
	return nil
}

// CreateNodePortService Creates a Node Port service for vcluster context
func (v *VCluster) CreateNodePortService() error {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vcluster-nodeport",
			Namespace: v.Namespace,
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"app":     "vcluster",
				"release": v.Name,
			},
			Ports: []v1.ServicePort{
				{
					Name:       "https",
					Port:       443,
					TargetPort: intstr.FromInt(8443),
					Protocol:   v1.ProtocolTCP,
				},
			},
			Type: v1.ServiceTypeNodePort,
		},
	}
	createdSvc, err := k8sCore.CreateService(service)
	if err != nil {
		return err
	}
	v.NodePort = createdSvc.Spec.Ports[0].NodePort
	return nil
}

// SetClientSetForVCluster method calculates the clientset for vcluster. This also
// takes care of setting any extra params like skip-tls-verify, etc for vcluster
func (v *VCluster) SetClientSetForVCluster() error {
	serverURL := fmt.Sprintf("--server=https://%s:%d", ControlNodeIP, v.NodePort)
	cmd := exec.Command("vcluster", "connect", v.Name, "-n", v.Namespace, "--update-current=false", serverURL)
	_, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	currentDir, _ := os.Getwd()
	kcfPath := filepath.Join(currentDir, "kubeconfig.yaml")
	config, err := clientcmd.BuildConfigFromFlags("", kcfPath)
	if err != nil {
		return err
	}
	// Bypass TLS verification for vCluster - Required for Nodeport way to connect
	config.Insecure = true
	config.TLSClientConfig.CAFile = ""
	config.TLSClientConfig.CAData = nil
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	v.Clientset = clientset
	snapClient, err := snapclientset.NewForConfig(config)
	if err != nil {
		return err
	}
	v.SnapClient = snapClient
	return nil
}

// CreatePVC creates a PVC in vcluster
func (v *VCluster) CreatePVC(pvcName, svcName, appNs, accessMode string) (string, error) {
	if pvcName == "" {
		pvcName = v.Name + "-" + svcName + "-pvc"
	}
	var mode corev1.PersistentVolumeAccessMode
	switch accessMode {
	case "RWO":
		mode = corev1.ReadWriteOnce
	case "RWX":
		mode = corev1.ReadWriteMany
	default:
		mode = corev1.ReadWriteOnce
	}
	createOpts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: appNs,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{mode},
			StorageClassName: &svcName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: appNs,
		},
	}
	// Creating Namespace in VCluster first before creating PVC
	if _, err := v.Clientset.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", err
		}
		log.Infof("Namespace %s already exists. Skipping creation.", ns.Name)
	}
	_, err := v.Clientset.CoreV1().PersistentVolumeClaims(appNs).Create(context.TODO(), createOpts, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return pvcName, nil
}

// int32Ptr converts integer to pointer
func int32Ptr(i int32) *int32 { return &i }

// CreateFIODeployment creates a FIO Batch Job on single PVC
func (v *VCluster) CreateFIODeployment(pvcName string, appNS string, fioOpts FIOOptions, jobName string) error {
	return v.CreateFIOMultiPvcDeployment([]string{pvcName}, appNS, fioOpts, jobName)
}

// CreateFIOMultiPvcDeployment runs FIO Job on multiple PVCs
func (v *VCluster) CreateFIOMultiPvcDeployment(pvcNames []string, appNS string, fioOpts FIOOptions, jobName string) error {
	fioCmd := []string{
		"fio",
		"--name=" + fioOpts.Name,
		"--ioengine=" + fioOpts.IOEngine,
		"--rw=" + fioOpts.RW,
		"--bs=" + fioOpts.BS,
		"--numjobs=" + strconv.Itoa(fioOpts.NumJobs),
		"--size=" + fioOpts.Size,
		"--end_fsync=" + strconv.Itoa(fioOpts.EndFsync),
	}
	if fioOpts.TimeBased {
		fioCmd = append(fioCmd, "--time_based")
	}
	fioCmd = append(fioCmd, "--runtime="+fioOpts.Runtime)
	if fioOpts.DoVerify != nil {
		fioCmd = append(fioCmd, fmt.Sprintf("--do_verify=%v", *fioOpts.DoVerify))
	}
	if fioOpts.Verify != nil {
		fioCmd = append(fioCmd, fmt.Sprintf("--verify=%v", *fioOpts.Verify))
	}
	if fioOpts.VerifyOnly {
		fioCmd = append(fioCmd, "--verify_only")
	}
	volumeMounts := make([]corev1.VolumeMount, len(pvcNames))
	volumes := make([]corev1.Volume, len(pvcNames))
	for i, pvcName := range pvcNames {
		mountPath := fmt.Sprintf("/data%d", i)
		fileName := fmt.Sprintf("file-%d", i)
		mntPath := mountPath + "/" + fileName
		fioCmd = append(fioCmd, "--filename="+mntPath)
		volumeName := "fio-volume-" + strconv.Itoa(i)
		volumeMounts[i] = corev1.VolumeMount{
			MountPath: mountPath,
			Name:      volumeName,
		}
		volumes[i] = corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		}
	}
	fioJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: appNS,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "fio",
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: "Never",
					Containers: []corev1.Container{
						{
							Name:         "fio-container",
							Image:        "xridge/fio:latest",
							Command:      []string{"/bin/sh", "-c"},
							Args:         []string{strings.Join(fioCmd, " ")},
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	log.Infof("Going ahead to run FIO Application on VCluster %v for %v ", v.Name, fioOpts.Runtime)
	log.Infof("%v", fioCmd)
	if _, err := v.Clientset.BatchV1().Jobs(appNS).Create(context.TODO(), fioJob, metav1.CreateOptions{}); err != nil {
		return err
	}
	if err := v.WaitForFIOCompletion(appNS, jobName); err != nil {
		return err
	}
	// Hard sleep to let fio pod finish up
	time.Sleep(10 * time.Second)
	pods, err := v.Clientset.CoreV1().Pods(appNS).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Set{"app": "fio"}.AsSelector().String(),
	})
	if err != nil {
		return err
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("no FIO pods found")
	}
	podName := pods.Items[0].Name
	logs, err := v.FetchFIOLogs(podName, appNS)
	if err != nil {
		return err
	}
	log.Infof("Fio Output is: %v", logs)
	return nil
}

// VClusterCleanup does all the cleanup related to vcluster tests
func (v *VCluster) VClusterCleanup(scName string) error {
	if err := v.TerminateVCluster(); err != nil {
		return err
	}
	if err := DeleteNSFromHost(v.Namespace); err != nil {
		return err
	}
	if err := DeleteStorageclassFromHost(scName); err != nil {
		return err
	}
	return nil
}

// DeleteNSFromHost delete a namespace from host cluster
func DeleteNSFromHost(ns string) error {
	return k8sCore.DeleteNamespace(ns)
}

// DeleteStorageclassFromHost deletes a storageclass from host cluster
func DeleteStorageclassFromHost(sc string) error {
	return k8sStorage.DeleteStorageClass(sc)
}

// WaitForFIOCompletion checks for FIO pod completion in vcluster context
func (v *VCluster) WaitForFIOCompletion(namespace, jobName string) error {
	f := func() (interface{}, bool, error) {
		log.Infof("Entering to see if FIO Job has completed")
		job, err := v.Clientset.BatchV1().Jobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
		if err != nil {
			return nil, false, err
		}
		if job.Status.Succeeded >= 1 {
			return nil, false, nil
		}
		if job.Status.Failed >= 1 {
			return nil, false, fmt.Errorf("FIO Job has failed")
		}
		return nil, true, fmt.Errorf("Still not completed. Looks like we have to wait for 30 seconds")
	}
	_, err := task.DoRetryWithTimeout(f, VclusterAppTimeout, VClusterAppRetryInterval)
	if err != nil {
		return err
	}
	return nil
}

// FetchFIOLogs method streams and fetches FIO pod logs in vcluster
func (v *VCluster) FetchFIOLogs(podName, namespace string) (string, error) {
	podLogOpts := corev1.PodLogOptions{Container: "fio-container"}
	request := v.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &podLogOpts)
	podLogs, err := request.Stream(context.TODO())
	if err != nil {
		return "", err
	}
	defer podLogs.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

// DeleteJobOnVcluster deletes the Job on Vcluster
func (v *VCluster) DeleteJobOnVcluster(appNS string, jobName string) error {
	deletePolicy := metav1.DeletePropagationForeground
	return v.Clientset.BatchV1().Jobs(appNS).Delete(context.TODO(), jobName, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
}

// CreateNginxDeployment Deploys an Nginx Deployment on Vcluster
func (v *VCluster) CreateNginxDeployment(pvcName string, appNS string, deploymentName string) error {
	nginxDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: appNS,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": deploymentName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": deploymentName,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx-container",
							Image: "nginx:latest",
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 80,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									MountPath: "/usr/share/nginx/html",
									Name:      "nginx-volume",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "nginx-volume",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}
	log.Infof("Going ahead to deploy Nginx Application on VCluster %v", v.Name)
	if _, err := v.Clientset.AppsV1().Deployments(appNS).Create(context.TODO(), nginxDeployment, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

// ScaleVclusterDeployment Scales a deployment on VCluster to set replicas
func (v *VCluster) ScaleVclusterDeployment(appNS string, deploymentName string, replicas int32) error {
	deployment, err := v.Clientset.AppsV1().Deployments(appNS).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	deployment.Spec.Replicas = &replicas
	_, err = v.Clientset.AppsV1().Deployments(appNS).Update(context.TODO(), deployment, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	log.Infof("Successfully scaled Deployment %s to %d replicas.", deploymentName, replicas)
	return nil
}

// ListDeploymentPods This method lists all pods in a deployment in vcluster context
func (v *VCluster) ListDeploymentPods(appNS, deploymentName string) (*v1.PodList, error) {
	pods, err := v.Clientset.CoreV1().Pods(appNS).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "app=" + deploymentName,
	})
	return pods, err
}

// ValidateDeploymentScaling Validates if a deployment on Vcluster is having expected number of Replicas or not
func (v *VCluster) ValidateDeploymentScaling(appNS string, deploymentName string, expectedReplicas int32) error {
	checkDeploymentScaling := func() (interface{}, bool, error) {
		pods, err := v.ListDeploymentPods(appNS, deploymentName)
		if err != nil {
			return nil, true, err
		}
		runningPods := 0
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning {
				runningPods++
			}
		}
		if int32(runningPods) == expectedReplicas {
			log.Infof("Deployment %s has successfully scaled to %d replicas.", deploymentName, expectedReplicas)
			return nil, false, nil
		} else {
			log.Infof("Deployment %s has %d replicas. Expected: %d", deploymentName, runningPods, expectedReplicas)
			return nil, true, fmt.Errorf("Deployment %s has not scaled to expected replicas", deploymentName)
		}
	}
	_, err := task.DoRetryWithTimeout(checkDeploymentScaling, VclusterAppTimeout, VClusterAppRetryInterval)
	return err
}

// DeleteDeploymentOnVCluster deletes a deployment on the Vcluister
func (v *VCluster) DeleteDeploymentOnVCluster(appNS string, deploymentName string) error {
	deletePolicy := metav1.DeletePropagationForeground
	return v.Clientset.AppsV1().Deployments(appNS).Delete(context.TODO(), deploymentName, metav1.DeleteOptions{
		PropagationPolicy: &deletePolicy,
	})
}

// IsDeploymentHealthy validates if a deployment is healthy in a vcluster
func (v *VCluster) IsDeploymentHealthy(appNS string, deploymentName string, expectedReplicas int32) error {
	checkDeploymentHealth := func() (interface{}, bool, error) {
		pods, err := v.Clientset.CoreV1().Pods(appNS).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "app=" + deploymentName,
		})
		if err != nil {
			return nil, true, err
		}
		healthyPods := 0
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning {
				allContainersReady := true
				for _, containerStatus := range pod.Status.ContainerStatuses {
					if !containerStatus.Ready {
						allContainersReady = false
						break
					}
				}
				if allContainersReady {
					healthyPods++
				}
			}
		}
		if int32(healthyPods) == expectedReplicas {
			log.Infof("Deployment %s is healthy with %d healthy replicas.", deploymentName, expectedReplicas)
			return nil, false, nil
		} else {
			log.Infof("Deployment %s has %d healthy replicas. Expected: %d", deploymentName, healthyPods, expectedReplicas)
			return nil, true, fmt.Errorf("Deployment %s is not yet healthy", deploymentName)
		}
	}
	_, err := task.DoRetryWithTimeout(checkDeploymentHealth, VclusterAppTimeout, VClusterAppRetryInterval)
	return err
}

// GetDeploymentPodNodes returns the names of the nodes on which pods of this deployment are running
func (v *VCluster) GetDeploymentPodNodes(appNS string, deploymentName string) ([]string, error) {
	pods, err := v.ListDeploymentPods(appNS, deploymentName)
	if err != nil {
		return nil, fmt.Errorf("error listing pods: %v", err)
	}
	var nodeNames []string
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != "" {
			nodeNames = append(nodeNames, pod.Spec.NodeName)
		}
	}
	return nodeNames, nil
}

// CreateClusterWideSecret method creates a cluster wide secret for creating secure storage classes
func CreateClusterWideSecret(name string) error {
	secretValue := "vcluster-tests-secret"
	// Create a new secret object
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: PxNamespace,
		},
		StringData: map[string]string{
			ClusterWideSecretKey: secretValue,
		},
		Type: corev1.SecretTypeOpaque,
	}
	if _, err := k8sCore.CreateSecret(secret); err != nil {
		return err
	}
	return nil
}

// DeleteSecret deletes a secret
func DeleteSecret(name, namespace string) error {
	return k8sCore.DeleteSecret(name, namespace)
}

// CreateVolumeSnapshotClass Creates a VolumeSnapshotclass in Vcluster context
func (v *VCluster) CreateVolumeSnapshotClass(snapClassName string, params map[string]string) error {
	vsc := &snapshotv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapClassName,
		},
		Driver:         k8s.CsiProvisioner,
		DeletionPolicy: snapshotv1.VolumeSnapshotContentDelete,
		Parameters:     params,
	}
	if _, err := v.SnapClient.SnapshotV1().VolumeSnapshotClasses().Create(context.TODO(), vsc, metav1.CreateOptions{}); err != nil {
		return err
	}
	log.Infof("VolumeSnapshotClass %s created successfully", snapClassName)
	return nil
}

// CreateSnapshot creates a volume snapshot for a given PVC in vcluster context
func (v *VCluster) CreateSnapshot(snapshotName, pvcName, snapClassName, namespace string) error {
	vs := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: namespace,
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: &snapClassName,
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}
	_, err := v.SnapClient.SnapshotV1().VolumeSnapshots(namespace).Create(context.TODO(), vs, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	log.Infof("Snapshot %s created successfully", snapshotName)
	return nil
}

// RestorePVCFromSnapshot restores a CSI PVC from a volume snapshot
func (v *VCluster) RestorePVCFromSnapshot(restoredPvcName, snapshotName, appNS, storageClassName, accessMode string) error {
	var restoredAccessMode corev1.PersistentVolumeAccessMode
	switch accessMode {
	case "RWO":
		restoredAccessMode = corev1.ReadWriteOnce
	case "RWX":
		restoredAccessMode = corev1.ReadWriteMany
	case "ROX":
		restoredAccessMode = corev1.ReadOnlyMany
	default:
		restoredAccessMode = corev1.ReadWriteOnce
	}
	restorePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      restoredPvcName,
			Namespace: appNS,
			Annotations: map[string]string{
				"snapshot.alpha.kubernetes.io/snapshot": snapshotName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{restoredAccessMode},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	_, err := v.Clientset.CoreV1().PersistentVolumeClaims(appNS).Create(context.TODO(), restorePVC, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	log.Infof("Restored PVC %s from snapshot %s successfully", restoredPvcName, snapshotName)
	return nil
}

// ListSnapshots lists all snapshots in the Vcluster namespace on Host Context
func (v *VCluster) ListSnapshots() (*snapv1.VolumeSnapshotList, error) {
	snapshotList, err := k8sExternalStorage.ListSnapshots(v.Namespace)
	if err != nil {
		return nil, err
	}
	return snapshotList, nil
}

// PVCRuleByUsageCapacityForVcluster Sets an Autopilot Rule for PVC in context of a Vcluster
func PVCRuleByUsageCapacityForVcluster(usagePercentage int, scalePercentage int, maxSize string) apapi.AutopilotRule {
	return apapi.AutopilotRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("pvc-usage-%d-scale-%d-%v", usagePercentage, scalePercentage, time.Now().Unix()),
		},
		Spec: apapi.AutopilotRuleSpec{
			NamespaceSelector: apapi.RuleObjectSelector{
				LabelSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"type": "vcluster",
					},
				},
			},
			Conditions: apapi.RuleConditions{
				Expressions: []*apapi.LabelSelectorRequirement{
					{
						Key:      aututils.PxVolumeUsagePercentMetric,
						Operator: apapi.LabelSelectorOpGt,
						Values:   []string{fmt.Sprintf("%d", usagePercentage)},
					},
				},
			},
			Actions: []*apapi.RuleAction{
				{
					Name: aututils.VolumeSpecAction,
					Params: map[string]string{
						aututils.RuleActionsScalePercentage: fmt.Sprintf("%d", scalePercentage),
					},
				},
			},
		},
	}
}

// GetPVC returns PVC object of a PVC in vCluster Context
func (v *VCluster) GetPVC(pvcName string, namespace string) (*corev1.PersistentVolumeClaim, error) {
	return v.Clientset.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), pvcName, metav1.GetOptions{})
}

// GetPvcCapacitySize returns capacity of a Pvc object in GiB
func GetPvcCapacitySize(pvc *corev1.PersistentVolumeClaim) float64 {
	size := pvc.Status.Capacity[corev1.ResourceStorage]
	sizeBytes := size.Value()
	sizeGiB := float64(sizeBytes) / (1024 * 1024 * 1024)
	return sizeGiB
}

// GetPvcOriginalSize returns original size of a PVC object in GiB
func GetPvcOriginalSize(pvc *corev1.PersistentVolumeClaim) float64 {
	size := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	sizeBytes := size.Value()
	sizeGiB := float64(sizeBytes) / (1024 * 1024 * 1024)
	return sizeGiB
}

// ListNamespaceVcluster lists all namespaces within a vcluster
func (v *VCluster) ListNamespaceVcluster() (*corev1.NamespaceList, error) {
	return v.Clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
}

// WaitForVClusterAccess method's purpose is to wait for 5 mins max to see if vcluster has become inaccessible
func (v *VCluster) WaitForVClusterAccess() error {
	f := func() (interface{}, bool, error) {
		_, err := v.ListNamespaceVcluster()
		if err != nil {
			return nil, true, err
		} else {
			return nil, false, nil
		}
	}
	_, err := task.DoRetryWithTimeout(f, VclusterAppTimeout, VClusterRetryInterval)
	return err
}

// ReadEnvVariable reads env variable and returns the value
func ReadEnvVariable(envVar string) string {
	envValue, present := os.LookupEnv(envVar)
	if present {
		return envValue
	}
	return ""
}
