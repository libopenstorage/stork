package k8s

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	baseErrors "errors"
	"fmt"
	"io"
	"io/ioutil"
	random "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"

	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"

	yaml2 "gopkg.in/yaml.v2"

	docker_types "github.com/docker/docker/api/types"
	vaultapi "github.com/hashicorp/vault/api"
	volsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	osapi "github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/units"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	admissionregistration "github.com/portworx/sched-ops/k8s/admissionregistration"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/autopilot"
	"github.com/portworx/sched-ops/k8s/batch"
	k8sCommon "github.com/portworx/sched-ops/k8s/common"
	"github.com/portworx/sched-ops/k8s/core"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	csisnapshot "github.com/portworx/sched-ops/k8s/externalsnapshotter"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/k8s/networking"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/sched-ops/k8s/policy"
	"github.com/portworx/sched-ops/k8s/prometheus"
	"github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/k8s/stork"
	tektoncd "github.com/portworx/sched-ops/k8s/tektoncd"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/pureutils"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	tektoncdv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	admissionregistrationv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	appsapi "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	networkingv1beta1 "k8s.io/api/networking/v1beta1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storageapi "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "k8s"
	// SnapshotParent is the parameter key for the parent of a snapshot
	SnapshotParent = "snapshot_parent"
	k8sPodsRootDir = "/var/lib/kubelet/pods"
	// DeploymentSuffix is the suffix for deployment names stored as keys in maps
	DeploymentSuffix = "-dep"
	// StatefulSetSuffix is the suffix for statefulset names stored as keys in maps
	StatefulSetSuffix = "-ss"
	// SystemdSchedServiceName is the name of the system service responsible for scheduling
	SystemdSchedServiceName = "kubelet"
	// ZoneK8SNodeLabel is label describing zone of the k8s node
	ZoneK8SNodeLabel = "failure-domain.beta.kubernetes.io/zone"
	// RegionK8SNodeLabel is node label describing region of the k8s node
	RegionK8SNodeLabel = "failure-domain.beta.kubernetes.io/region"
	// TopologyZoneK8sNodeLabel is label describing topology zone of k8s node
	TopologyZoneK8sNodeLabel = "topology.portworx.io/zone"
	// TopologyRegionK8sNodeLabel is label describing topology region of k8s node
	TopologyRegionK8sNodeLabel = "topology.portworx.io/region"
	// PureFile is the parameter in storageclass to represent FB volume
	PureFile = "pure_file"
	// PureBlock is the parameter in storageclass to represent FA direct access volumes
	PureBlock = "pure_block"
	// PortworxStrict provisioner for using same provisioner as provided in the spec
	PortworxStrict = "strict"
	// CsiProvisioner is csi provisioner
	CsiProvisioner = "pxd.portworx.com"
	// PortworxVolumeProvisioner is a provisioner for portworx volumes
	PortworxVolumeProvisioner = "kubernetes.io/portworx-volume"
	//NodeType for enabling specific features
	NodeType = "node-type"
	//FastpathNodeType fsatpath node type value
	FastpathNodeType = "fastpath"
	// PureDAVolumeLabel is the label added to a volume only if the volume is a Pure Direct Access (FADA/FBDA) volume
	PureDAVolumeLabel = "pure_direct_access"
	// PureDAVolumeLabelValueFA is the value added for the PureDAVolumeLabel key if the volume is an FADA volume
	PureDAVolumeLabelValueFA = "fada"
	// PureDAVolumeLabelValueFB is the value added for the PureDAVolumeLabel key if the volume is an FBDA volume
	PureDAVolumeLabelValueFB = "fbda"
	// FADAVolumeSerialLabel is the label added to a volume (only if FlashArray Direct Access) that holds the volume serial
	FADAVolumeSerialLabel = "fada_volume_serial"

	//ReplVPS volume placement strategy node label value
	ReplVPS = "replvps"
	// PxLabelNameKey is key for map
	PxLabelNameKey = "name"
	// PxLabelValue portworx pod label
	PxLabelValue          = "portworx"
	AutopilotLabelNameKey = "name"
	AutopilotLabelValue   = "autopilot"
	PortworxNotDeployed   = "portworx not deployed"
	// MetadataLabel is added to node if node has to be made kvdb node or never become kvdb node according to key value
	MetaDataLabel = "px/metadata-node"
)

const (
	k8sNodeReadyTimeout    = 5 * time.Minute
	volDirCleanupTimeout   = 5 * time.Minute
	k8sObjectCreateTimeout = 2 * time.Minute
	k8sPodCreateTimeout    = 4 * time.Minute
	k8sDestroyTimeout      = 10 * time.Minute
	// FindFilesOnWorkerTimeout timeout for find files on worker
	FindFilesOnWorkerTimeout = 1 * time.Minute
	deleteTasksWaitTimeout   = 5 * time.Minute
	// DefaultRetryInterval  Default retry interval
	DefaultRetryInterval = 10 * time.Second
	// DefaultTimeout default timeout
	DefaultTimeout = 5 * time.Minute
	// SnapshotReadyTimeout timeout for snapshot to be ready
	SnapshotReadyTimeout             = 5 * time.Minute
	numOfRestoredPVCForCloneManyTest = 500

	autopilotDefaultNamespace         = "kube-system"
	portworxServiceName               = "portworx-service"
	resizeSupportedAnnotationKey      = "torpedo.io/resize-supported"
	autopilotEnabledAnnotationKey     = "torpedo.io/autopilot-enabled"
	pvcLabelsAnnotationKey            = "torpedo.io/pvclabels-enabled"
	pvcNodesAnnotationKey             = "torpedo.io/pvcnodes-enabled"
	deleteStrategyAnnotationKey       = "torpedo.io/delete-strategy"
	specObjAppWorkloadSizeEnvVar      = "SIZE"
	cdiImportSuccessEvent             = "Import Successful"
	cdiPvcRunningMessageAnnotationKey = "cdi.kubevirt.io/storage.condition.running.message"
	cdiPvcImportEndpointAnnotationKey = "cdi.kubevirt.io/storage.import.endpoint"
	cdiImportComplete                 = "Import Complete"
	cdiImageImportTimeout             = 20 * time.Minute
	cdiImageImportRetry               = 30 * time.Second
)

const (
	secretNameKey      = "secret_name"
	secretNamespaceKey = "secret_namespace"
	encryptionKey      = "encryption"

	// Encryption annotations
	encryptionName = "secure"

	// In-Tree Auth annotations
	secretName      = "openstorage.io/auth-secret-name"
	secretNamespace = "openstorage.io/auth-secret-namespace"

	// CsiProvisionerSecretName is required for CSI with Auth enabled
	CsiProvisionerSecretName = "csi.storage.k8s.io/provisioner-secret-name"
	// CsiProvisionerSecretNamespace is required for CSI with Auth enabled
	CsiProvisionerSecretNamespace = "csi.storage.k8s.io/provisioner-secret-namespace"
	// CsiNodePublishSecretName is required for CSI with Auth enabled
	CsiNodePublishSecretName = "csi.storage.k8s.io/node-publish-secret-name"
	// CsiNodePublishSecretNamespace is required for CSI with Auth enabled
	CsiNodePublishSecretNamespace = "csi.storage.k8s.io/node-publish-secret-namespace"
	// CsiControllerExpandSecretName is required for CSI with Auth enabled
	CsiControllerExpandSecretName = "csi.storage.k8s.io/controller-expand-secret-name"
	// CsiControllerExpandSecretNamespace is required for CSI with Auth enabled
	CsiControllerExpandSecretNamespace = "csi.storage.k8s.io/controller-expand-secret-namespace"

	storageClassKey = "volume.beta.kubernetes.io/storage-class"
	// PvcNameKey key used in volume param map to store PVC name
	PvcNameKey = "pvc_name"
	// PvcNamespaceKey key used in volume param map to store PVC namespace
	PvcNamespaceKey = "pvc_namespace"
	// VolumeSnapshotKind type use for restore
	VolumeSnapshotKind = "VolumeSnapshot"
)

var (
	// use underscore to avoid conflicts to text/template from golang
	namespaceRegex      = regexp.MustCompile("_NAMESPACE_")
	defaultTorpedoLabel = map[string]string{
		"creator": "torpedo",
	}
	k8sCore                  = core.Instance()
	k8sApps                  = apps.Instance()
	k8sStork                 = stork.Instance()
	k8sStorage               = storage.Instance()
	k8sExternalStorage       = externalstorage.Instance()
	k8sAutopilot             = autopilot.Instance()
	k8sRbac                  = rbac.Instance()
	k8sNetworking            = networking.Instance()
	k8sBatch                 = batch.Instance()
	k8sMonitoring            = prometheus.Instance()
	k8sPolicy                = policy.Instance()
	k8sAdmissionRegistration = admissionregistration.Instance()
	k8sApiExtensions         = apiextensions.Instance()
	k8sOperator              = operator.Instance()
	k8sKubevirt              = kubevirt.Instance()
	k8stektoncd              = tektoncd.Instance()

	// k8sExternalsnap is a instance of csisnapshot instance
	k8sExternalsnap = csisnapshot.Instance()
	// SnapshotAPIGroup is the group for the resource being referenced.
	SnapshotAPIGroup = "snapshot.storage.k8s.io"
)

// CustomResourceObjectYAML	Used as spec object for all CRs
type CustomResourceObjectYAML struct {
	Path string
	// Namespace will only be assigned DURING creation
	Namespace string
	Name      string
}

// K8s  The kubernetes structure
type K8s struct {
	SpecFactory                      *spec.Factory
	NodeDriverName                   string
	VolDriverName                    string
	secretConfigMapName              string
	customConfig                     map[string]scheduler.AppConfig
	eventsStorage                    map[string][]scheduler.Event
	SecretType                       string
	VaultAddress                     string
	VaultToken                       string
	PureVolumes                      bool
	PureSANType                      string
	PureFADAPod                      string
	RunCSISnapshotAndRestoreManyTest bool
	helmValuesConfigMapName          string
	secureApps                       []string
}

// IsNodeReady  Check whether the cluster node is ready
func (k *K8s) IsNodeReady(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := k8sCore.IsNodeReady(n.Name); err != nil {
			return "", true, &scheduler.ErrNodeNotReady{
				Node:  n,
				Cause: err.Error(),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, k8sNodeReadyTimeout, DefaultRetryInterval); err != nil {
		return err
	}
	return nil
}

// String returns the string name of this driver.
func (k *K8s) String() string {
	return SchedName
}

// Init Initialize the driver
func (k *K8s) Init(schedOpts scheduler.InitOptions) error {
	k.NodeDriverName = schedOpts.NodeDriverName
	k.VolDriverName = schedOpts.VolDriverName
	k.secretConfigMapName = schedOpts.SecretConfigMapName
	k.customConfig = schedOpts.CustomAppConfig
	k.SecretType = schedOpts.SecretType
	k.VaultAddress = schedOpts.VaultAddress
	k.VaultToken = schedOpts.VaultToken
	k.eventsStorage = make(map[string][]scheduler.Event)
	k.PureVolumes = schedOpts.PureVolumes
	k.PureSANType = schedOpts.PureSANType
	k.PureFADAPod = schedOpts.PureFADAPod
	k.RunCSISnapshotAndRestoreManyTest = schedOpts.RunCSISnapshotAndRestoreManyTest
	k.secureApps = schedOpts.SecureApps

	nodes, err := k8sCore.GetNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes.Items {
		if err = k.AddNewNode(n); err != nil {
			return err
		}
	}

	k.SpecFactory, err = spec.NewFactory(schedOpts.SpecDir, schedOpts.VolDriverName, k)
	if err != nil {
		return err
	}

	go func() {
		err := k.collectEvents()
		if err != nil {
			log.Fatalf(fmt.Sprintf("%v", err))
		}
	}()
	return nil
}

// AddNewNode method parse and add node to node registry
func (k *K8s) AddNewNode(newNode corev1.Node) error {
	n := k.parseK8SNode(newNode)
	if err := k.IsNodeReady(n); err != nil {
		return err
	}
	if err := node.AddNode(n); err != nil {
		return err
	}
	return nil
}

// SetConfig sets kubeconfig. If kubeconfigPath == "" then
// sets it to inClusterConfig
func (k *K8s) SetConfig(kubeconfigPath string) error {
	var config *rest.Config
	var err error

	if kubeconfigPath == "" {
		config = nil
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return err
		}
	}
	k8sCore.SetConfig(config)
	k8sApps.SetConfig(config)
	k8sApps.SetConfig(config)
	k8sStork.SetConfig(config)
	k8sStorage.SetConfig(config)
	k8sExternalStorage.SetConfig(config)
	k8sAutopilot.SetConfig(config)
	k8sRbac.SetConfig(config)
	k8sMonitoring.SetConfig(config)
	k8sPolicy.SetConfig(config)
	k8sBatch.SetConfig(config)
	k8sMonitoring.SetConfig(config)
	k8sAdmissionRegistration.SetConfig(config)
	k8sExternalsnap.SetConfig(config)
	k8sApiExtensions.SetConfig(config)
	k8sOperator.SetConfig(config)
	k8sKubevirt.SetConfig(config)
	k8stektoncd.SetConfig(config)

	return nil
}

// SetGkeConfig sets kubeconfig for cloud provider GKE
func (k *K8s) SetGkeConfig(kubeconfigPath string, jsonKey string) error {

	var clientConfig *rest.Config
	var err error

	if kubeconfigPath == "" {
		clientConfig = nil
	} else {
		clientConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return err
		}
	}

	clientConfig.AuthProvider.Config["cred-json"] = jsonKey
	if err != nil {
		return err
	}

	k8sCore.SetConfig(clientConfig)
	k8sApps.SetConfig(clientConfig)
	k8sApps.SetConfig(clientConfig)
	k8sStork.SetConfig(clientConfig)
	k8sStorage.SetConfig(clientConfig)
	k8sExternalStorage.SetConfig(clientConfig)
	k8sAutopilot.SetConfig(clientConfig)
	k8sRbac.SetConfig(clientConfig)
	k8sMonitoring.SetConfig(clientConfig)
	k8sPolicy.SetConfig(clientConfig)
	k8sBatch.SetConfig(clientConfig)
	k8sMonitoring.SetConfig(clientConfig)
	k8sAdmissionRegistration.SetConfig(clientConfig)
	k8sExternalsnap.SetConfig(clientConfig)
	k8sApiExtensions.SetConfig(clientConfig)
	k8sOperator.SetConfig(clientConfig)

	return nil
}

// RescanSpecs Rescan the application spec file for spei
func (k *K8s) RescanSpecs(specDir, storageDriver string) error {
	var err error
	log.Infof("Rescanning specs for %v and driver %s", specDir, storageDriver)
	k.SpecFactory, err = spec.NewFactory(specDir, storageDriver, k)
	if err != nil {
		return err
	}
	return nil
}

// RefreshNodeRegistry update the k8 node list registry
func (k *K8s) RefreshNodeRegistry() error {

	nodes, err := k8sCore.GetNodes()
	if err != nil {
		return err
	}

	node.CleanupRegistry()

	for _, n := range nodes.Items {
		if err = k.AddNewNode(n); err != nil {
			return err
		}
	}
	return nil
}

// ParseSpecs parses the application spec file
func (k *K8s) ParseSpecs(specDir, storageProvisioner string) ([]interface{}, error) {
	log.Debugf("ParseSpecs k.CustomConfig = %v", k.customConfig)
	fileList := make([]string, 0)
	if err := filepath.Walk(specDir, func(path string, f os.FileInfo, err error) error {
		if f != nil && !f.IsDir() {
			if isValidProvider(path, storageProvisioner) {
				log.Debugf("	add filepath: %s", path)
				fileList = append(fileList, path)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	log.Debugf("fileList: %v", fileList)
	var specs []interface{}

	splitPath := strings.Split(specDir, "/")
	appName := splitPath[len(splitPath)-1]

	for _, fileName := range fileList {
		isHelmChart, err := k.IsAppHelmChartType(fileName)
		if err != nil {
			return nil, err
		}

		splitPath := strings.Split(fileName, "/")
		if strings.HasPrefix(splitPath[len(splitPath)-1], "cr-") {
			// TODO: process with templates
			specObj := &CustomResourceObjectYAML{
				Path: fileName,
			}
			specs = append(specs, specObj)
			log.Warnf("custom res: %v", specObj) //TODO: remove
		} else if !isHelmChart {
			file, err := ioutil.ReadFile(fileName)
			if err != nil {
				return nil, err
			}

			var customConfig scheduler.AppConfig
			var ok bool

			if customConfig, ok = k.customConfig[appName]; !ok {
				customConfig = scheduler.AppConfig{}
			} else {
				log.Infof("customConfig[%v] = %v", appName, customConfig)
			}
			var funcs = template.FuncMap{
				"Iterate": func(count int) []int {
					var i int
					var Items []int
					for i = 1; i <= (count); i++ {
						Items = append(Items, i)
					}
					return Items
				},
				"array": func(arr []string) string {
					string := "[\""
					for i, val := range arr {
						if i != 0 {
							string += "\", \""
						}
						string += val
					}
					return string + "\"]"
				},
			}

			tmpl, err := template.New("customConfig").Funcs(funcs).Parse(string(file))
			if err != nil {
				return nil, err
			}
			var processedFile bytes.Buffer
			err = tmpl.Execute(&processedFile, customConfig)
			if err != nil {
				return nil, err
			}

			reader := bufio.NewReader(&processedFile)
			specReader := yaml.NewYAMLReader(reader)

			for {
				specContents, err := specReader.Read()
				if err == io.EOF {
					break
				}
				if len(bytes.TrimSpace(specContents)) > 0 {
					obj, err := decodeSpec(specContents)
					if err != nil {
						log.Warnf("Error decoding spec from %v: %v", fileName, err)
						return nil, err
					}

					specObj, err := validateSpec(obj)
					if err != nil {
						log.Warnf("Error parsing spec from %v: %v", fileName, err)
						return nil, err
					}
					substituteImageWithInternalRegistry(specObj)
					specs = append(specs, specObj)
				}
			}
		} else {
			repoInfo, err := k.ParseCharts(fileName)
			if err != nil {
				return nil, err
			}
			specs = append(specs, repoInfo)
		}
	}
	return specs, nil
}

// IsAppHelmChartType will return true if the specDir has only one file and it has helm repo infos
// else will return false
func (k *K8s) IsAppHelmChartType(fileName string) (bool, error) {

	// Parse the files and check for certain keys for helmRepo info

	log.Debugf("Reading file: %s", fileName)
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		return false, err
	}

	repoInfo := scheduler.HelmRepo{}
	err = yaml2.Unmarshal(file, &repoInfo)
	if err != nil {
		// Ignoring if unmarshalling fails as some app specs (like fio) failed to unmarshall
		log.Errorf("Ignoring the yaml unmarshalling failure , err: %v", err)
		return false, nil
	}

	if repoInfo.RepoName != "" && repoInfo.ChartName != "" && repoInfo.ReleaseName != "" {
		// If the yaml file with helmRepo info for the app is found, exit here.
		log.Infof("Helm chart was found in file: [%s]", fileName)
		return true, nil
	}

	return false, nil

}

// ParseSpecsFromYamlBuf parses the yaml buf content
func (k *K8s) ParseSpecsFromYamlBuf(yamlBuf *bytes.Buffer) ([]interface{}, error) {

	var specs []interface{}

	reader := bufio.NewReader(yamlBuf)
	specReader := yaml.NewYAMLReader(reader)

	for {
		specContents, err := specReader.Read()
		if err == io.EOF {
			break
		}
		if len(bytes.TrimSpace(specContents)) > 0 {
			obj, err := decodeSpec(specContents)
			if err != nil {
				log.Warnf("Error decoding spec: %v", err)
				return nil, err
			}

			specObj, err := validateSpec(obj)
			if err != nil {
				log.Warnf("Error validating spec: %v", err)
				return nil, err
			}

			substituteImageWithInternalRegistry(specObj)
			specs = append(specs, specObj)
		}
	}

	return specs, nil
}

func isValidProvider(specPath, storageProvisioner string) bool {
	// Skip all storage provisioner specific spec except storageProvisioner
	for _, driver := range volume.GetVolumeDrivers() {
		if driver != storageProvisioner && strings.Contains(specPath, "/"+driver+"/") {
			return false
		}
	}
	// Get the rest of specs
	return true
}

func decodeSpec(specContents []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
	if err != nil {
		schemeObj := runtime.NewScheme()
		if err := snapv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := storkapi.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := apapi.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := monitoringv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := apiextensionsv1beta1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := apiextensionsv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := storageapi.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := kubevirtv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}
		if err := tektoncdv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		codecs := serializer.NewCodecFactory(schemeObj)
		obj, _, err = codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func validateSpec(in interface{}) (interface{}, error) {
	if specObj, ok := in.(*appsapi.Deployment); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsapi.StatefulSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsapi.DaemonSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.Service); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.PersistentVolumeClaim); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storageapi.StorageClass); ok {
		return specObj, nil
	} else if specObj, ok := in.(*snapv1.VolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.GroupVolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.Secret); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.ConfigMap); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.Rule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.Pod); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ClusterPair); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.Migration); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.MigrationSchedule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.BackupLocation); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationBackup); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.SchedulePolicy); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationClone); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.VolumeSnapshotRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apapi.AutopilotRule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.ServiceAccount); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.ClusterRole); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.ClusterRoleBinding); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.Role); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.RoleBinding); ok {
		return specObj, nil
	} else if specObj, ok := in.(*batchv1beta1.CronJob); ok {
		return specObj, nil
	} else if specObj, ok := in.(*batchv1.Job); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.LimitRange); ok {
		return specObj, nil
	} else if specObj, ok := in.(*networkingv1beta1.Ingress); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.Prometheus); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.PrometheusRule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*monitoringv1.ServiceMonitor); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.Namespace); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apiextensionsv1.CustomResourceDefinition); ok {
		return specObj, nil
	} else if specObj, ok := in.(*policyv1.PodDisruptionBudget); ok {
		return specObj, nil
	} else if specObj, ok := in.(*netv1.NetworkPolicy); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.Endpoints); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ResourceTransformation); ok {
		return specObj, nil
	} else if specObj, ok := in.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		return specObj, nil
	} else if specObj, ok := in.(*admissionregistrationv1.ValidatingWebhookConfigurationList); ok {
		return specObj, nil
	} else if specObj, ok := in.(*corev1.PersistentVolume); ok {
		return specObj, nil
	} else if specObj, ok := in.(*kubevirtv1.VirtualMachine); ok {
		return specObj, nil
	} else if specObj, ok := in.(*tektoncdv1.Task); ok {
		return specObj, nil
	} else if specObj, ok := in.(*tektoncdv1.Pipeline); ok {
		return specObj, nil
	}

	return nil, fmt.Errorf("unsupported object: %v", reflect.TypeOf(in))
}

// filterPureVolumesIfEnabled will return whether or not the given PVC is a Pure
// direct access volume, only if PureVolumes is true. If PureVolumes is false, all
// volumes are valid.
func (k *K8s) filterPureVolumesIfEnabled(claim *v1.PersistentVolumeClaim) (bool, error) {
	volTypes := []string{PureFile, PureBlock}

	return k.filterPureTypeVolumeIfEnabled(claim, volTypes)
}

// filterPureVolumesIfEnabled will return whether or not the given PVC is a Pure
// direct access volume, only if PureVolumes is true. If PureVolumes is false, all
// volumes are valid.
func (k *K8s) filterPureVolumesIfEnabledByPureVolBackend(claim *v1.PersistentVolumeClaim, pureVolBackend string) (bool, error) {
	volTypes := []string{pureVolBackend}

	return k.filterPureTypeVolumeIfEnabled(claim, volTypes)
}

// filterPureTypeVolumeIfEnabled will return true if matches with given type pure volume
// If PureVolumes is false, all volumes are valid.
func (k *K8s) filterPureTypeVolumeIfEnabled(claim *v1.PersistentVolumeClaim, volTypes []string) (bool, error) {

	if !k.PureVolumes {
		// If we aren't filtering for Pure volumes, all are valid
		return true, nil
	}

	scForPvc, err := k8sCore.GetStorageClassForPVC(claim)
	if err != nil {
		return false, err
	}
	backend, ok := scForPvc.Parameters["backend"]
	if !ok {
		return false, nil
	}
	for _, voltype := range volTypes {
		if backend == voltype {
			return true, nil
		}
	}
	return false, nil
}

// getAddressesForNode  Get IP address for the nodes in the cluster
func (k *K8s) getAddressesForNode(n corev1.Node) []string {
	var addrs []string
	for _, addr := range n.Status.Addresses {
		if addr.Type == corev1.NodeExternalIP || addr.Type == corev1.NodeInternalIP {
			addrs = append(addrs, addr.Address)
		}
	}
	return addrs
}

// parseK8SNode Parse the kubernetes clsuter nodes
func (k *K8s) parseK8SNode(n corev1.Node) node.Node {
	var nodeType node.Type
	var zone, region string

	if k8sCore.IsNodeMaster(n) && k.NodeDriverName != "ibm" {
		nodeType = node.TypeMaster
	} else {
		nodeType = node.TypeWorker
	}

	nodeLabels, err := k8sCore.GetLabelsOnNode(n.GetName())
	if err != nil {
		log.Warn("failed to get node label for ", n.GetName())
	}

	for key, value := range nodeLabels {
		switch key {
		case ZoneK8SNodeLabel:
			zone = value
		case RegionK8SNodeLabel:
			region = value
		}
	}
	log.Infof("Parsed node [%s] as Type: %s, Zone: %s, Region %s", n.Name, nodeType, zone, region)

	return node.Node{
		Name:      n.Name,
		Addresses: k.getAddressesForNode(n),
		Type:      nodeType,
		Zone:      zone,
		Region:    region,
	}
}

func isCsiApp(options scheduler.ScheduleOptions, appName string) bool {
	log.Debugf("checking for app [%s] in [%v] for csi enabled apps", appName, options.CsiAppKeys)
	for _, app := range options.CsiAppKeys {
		if app == appName {
			return true
		}
	}
	return false
}

// Schedule Schedules the application
func (k *K8s) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			appSpec, err := k.SpecFactory.Get(key)
			if err != nil {
				return nil, err
			}
			if isCsiApp(options, key) {
				log.Debugf("app [%s] is csi enabled", key)
				appSpec.IsCSI = true
			}
			apps = append(apps, appSpec)
		}
	} else {
		apps = k.SpecFactory.GetAll()
	}

	var contexts []*scheduler.Context
	oldOptionsNamespace := options.Namespace
	for _, app := range apps {
		appNamespace := app.GetID(instanceID)
		if options.Namespace != "" {
			appNamespace = options.Namespace
		} else {
			options.Namespace = appNamespace
		}
		if len(options.TopologyLabels) > 1 {
			rotateTopologyArray(&options)
		}
		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		helmSpecObjects, err := k.HelmSchedule(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		specObjects = append(specObjects, helmSpecObjects...)
		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:       app.Key,
				SpecList:  specObjects,
				Enabled:   app.Enabled,
				NameSpace: appNamespace,
			},
			ScheduleOptions: options,
		}

		contexts = append(contexts, ctx)
		options.Namespace = oldOptionsNamespace
	}

	return contexts, nil
}

// ScheduleWithCustomAppSpecs Schedules the application with custom app specs
func (k *K8s) ScheduleWithCustomAppSpecs(apps []*spec.AppSpec, instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var contexts []*scheduler.Context
	oldOptionsNamespace := options.Namespace
	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		if options.Namespace != "" {
			appNamespace = options.Namespace
		} else {
			options.Namespace = appNamespace
		}
		if len(options.TopologyLabels) > 1 {
			rotateTopologyArray(&options)
		}

		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		helmSpecObjects, err := k.HelmSchedule(app, appNamespace, options)
		if err != nil {
			return nil, err
		}

		specObjects = append(specObjects, helmSpecObjects...)
		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
			ScheduleOptions: options,
		}

		contexts = append(contexts, ctx)
		options.Namespace = oldOptionsNamespace
	}

	return contexts, nil
}

// CreateSpecObjects Create application
func (k *K8s) CreateSpecObjects(app *spec.AppSpec, namespace string, options scheduler.ScheduleOptions) ([]interface{}, error) {
	var specObjects []interface{}
	ns, err := k.createNamespace(app, namespace, options)
	if err != nil {
		return nil, err
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createMigrationObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCRDObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createVolumeSnapshotRestore(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createStorageObject(appSpec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createTektonObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createRbacObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCoreObject(appSpec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}
	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createBackupObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createNetworkingObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createBatchObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createServiceMonitorObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createPodDisruptionBudgetObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createAdmissionRegistrationObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	// creation of CustomResourceObjects must most likely be done *last*,
	// as it may have resources that depend on other resources, which should be create *before* this
	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCustomResourceObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createVirtualMachineObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}
	return specObjects, nil
}

// AddTasks adds tasks to an existing context
func (k *K8s) AddTasks(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("context to add tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("need to specify list of applications to add to context")
	}

	// new tasks will be added to namespace of current context only
	appNamespace := ctx.ScheduleOptions.Namespace
	var apps []*spec.AppSpec
	specObjects := ctx.App.SpecList
	for _, key := range options.AppKeys {
		appSpec, err := k.SpecFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, appSpec)
	}
	for _, app := range apps {
		objects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return err
		}
		specObjects = append(specObjects, objects...)

		helmSpecObjects, err := k.HelmSchedule(app, appNamespace, options)
		if err != nil {
			return err
		}
		specObjects = append(specObjects, helmSpecObjects...)
	}
	ctx.App.SpecList = specObjects
	return nil
}

// ScheduleUninstall uninstalls tasks from an existing context
func (k *K8s) ScheduleUninstall(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("context to remove tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("need to specify list of applications to remove to context")
	}

	var apps []*spec.AppSpec
	for _, key := range options.AppKeys {
		appSpec, err := k.SpecFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, appSpec)
	}

	var removeSpecs []interface{}
	for _, app := range apps {
		for _, appSpec := range app.SpecList {
			if repoInfo, ok := appSpec.(*scheduler.HelmRepo); ok {
				specs, err := k.UnInstallHelmChart(repoInfo)
				if err != nil {
					return err
				}
				removeSpecs = append(removeSpecs, specs...)
			}
		}
	}
	err := k.RemoveAppSpecsByName(ctx, removeSpecs)
	if err != nil {
		return err
	}

	return nil
}

// RemoveAppSpecsByName removes uninstalled spec objects from an app's spec list
// so those objects will not be accessed during context validation and app destroy
// and during helm uninstall they can be deleted gracefully
func (k *K8s) RemoveAppSpecsByName(ctx *scheduler.Context, removeSpecs []interface{}) error {
	var remainSpecs []interface{}

SPECS:
	for _, spec := range ctx.App.SpecList {
		for _, removeSpec := range removeSpecs {
			if specObj, ok := spec.(*corev1.ConfigMap); ok {
				if removeObj, ok := removeSpec.(*corev1.ConfigMap); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*rbacv1.ClusterRole); ok {
				if removeObj, ok := removeSpec.(*rbacv1.ClusterRole); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*rbacv1.ClusterRoleBinding); ok {
				if removeObj, ok := removeSpec.(*rbacv1.ClusterRoleBinding); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*appsapi.Deployment); ok {
				if removeObj, ok := removeSpec.(*appsapi.Deployment); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*batchv1.Job); ok {
				if removeObj, ok := removeSpec.(*batchv1.Job); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
				if removeObj, ok := removeSpec.(*corev1.PersistentVolumeClaim); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*monitoringv1.Prometheus); ok {
				if removeObj, ok := removeSpec.(*monitoringv1.Prometheus); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*monitoringv1.PrometheusRule); ok {
				if removeObj, ok := removeSpec.(*monitoringv1.PrometheusRule); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*rbacv1.Role); ok {
				if removeObj, ok := removeSpec.(*rbacv1.Role); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*rbacv1.RoleBinding); ok {
				if removeObj, ok := removeSpec.(*rbacv1.RoleBinding); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*corev1.Secret); ok {
				if removeObj, ok := removeSpec.(*corev1.Secret); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*corev1.Service); ok {
				if removeObj, ok := removeSpec.(*corev1.Service); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*corev1.ConfigMap); ok {
				if removeObj, ok := removeSpec.(*corev1.ConfigMap); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*corev1.ServiceAccount); ok {
				if removeObj, ok := removeSpec.(*corev1.ServiceAccount); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*monitoringv1.ServiceMonitor); ok {
				if removeObj, ok := removeSpec.(*monitoringv1.ServiceMonitor); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*appsapi.StatefulSet); ok {
				if removeObj, ok := removeSpec.(*appsapi.StatefulSet); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			} else if specObj, ok := spec.(*storkapi.ClusterPair); ok {
				if removeObj, ok := removeSpec.(*storkapi.ClusterPair); ok {
					if specObj.Name == removeObj.Name {
						continue SPECS
					}
				}
			}
		}
		remainSpecs = append(remainSpecs, spec)
	}

	ctx.App.SpecList = remainSpecs
	return nil
}

// UpdateTasksID updates task IDs in the given context
func (k *K8s) UpdateTasksID(ctx *scheduler.Context, id string) error {
	ctx.UID = id

	for _, appSpec := range ctx.App.SpecList {
		metadata, err := meta.Accessor(appSpec)
		if err != nil {
			return err
		}
		metadata.SetNamespace(id)
	}
	return nil
}

// GetUpdatedSpec gets the updated spec of a K8s Object. Just `name`, and if required `namespace` must be specified on `spec` in order to GET the spec from K8s (if it exists).
func GetUpdatedSpec(spec interface{}) (interface{}, error) {
	if specObj, ok := spec.(*appsapi.Deployment); ok {
		dep, err := k8sApps.GetDeployment(specObj.Name, specObj.Namespace)
		if err == nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		dep.Kind = "Deployment"

		return dep, nil
	} else if specObj, ok := spec.(*appsapi.StatefulSet); ok {
		ss, err := k8sApps.GetStatefulSet(specObj.Name, specObj.Namespace)
		if err == nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		ss.Kind = "StatefulSet"

		return ss, nil
	} else if specObj, ok := spec.(*appsapi.DaemonSet); ok {
		ds, err := k8sApps.GetDaemonSet(specObj.Name, specObj.Namespace)
		if err == nil {
			return nil, err
		}
		return ds, nil
	} else if specObj, ok := spec.(*corev1.Service); ok {
		svc, err := k8sCore.GetService(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		svc.Kind = "Service"

		return svc, nil
	} else if specObj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		pvc, err := k8sCore.GetPersistentVolumeClaim(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		pvc.Kind = "PersistentVolumeClaim"

		return pvc, nil
	} else if specObj, ok := spec.(*storageapi.StorageClass); ok {
		sc, err := k8sStorage.GetStorageClass(specObj.Name)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		sc.Kind = "StorageClass"

		return sc, nil
	} else if specObj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		snap, err := k8sExternalStorage.GetSnapshot(specObj.Metadata.Name, specObj.Metadata.Namespace)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		snap.Kind = "VolumeSnapshot"

		return snap, nil
	} else if specObj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		snap, err := k8sStork.GetGroupSnapshot(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return snap, nil
	} else if specObj, ok := spec.(*corev1.Secret); ok {
		secret, err := k8sCore.GetSecret(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		secret.Kind = "Secret"

		return secret, nil
	} else if specObj, ok := spec.(*corev1.ConfigMap); ok {
		cm, err := k8sCore.GetConfigMap(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		cm.Kind = "ConfigMap"

		return cm, nil
	} else if specObj, ok := spec.(*storkapi.Rule); ok {
		rule, err := k8sStork.GetRule(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return rule, nil
	} else if specObj, ok := spec.(*corev1.Pod); ok {
		pod, err := k8sCore.GetPodByName(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return pod, nil
	} else if specObj, ok := spec.(*storkapi.ClusterPair); ok {
		cp, err := k8sStork.GetClusterPair(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return cp, nil
	} else if specObj, ok := spec.(*storkapi.Migration); ok {
		m, err := k8sStork.GetMigration(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return m, nil
	} else if specObj, ok := spec.(*storkapi.MigrationSchedule); ok {
		ms, err := k8sStork.GetMigrationSchedule(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return ms, nil
	} else if specObj, ok := spec.(*storkapi.BackupLocation); ok {
		bl, err := k8sStork.GetBackupLocation(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return bl, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationBackup); ok {
		ab, err := k8sStork.GetApplicationBackup(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return ab, nil
	} else if specObj, ok := spec.(*storkapi.SchedulePolicy); ok {
		sp, err := k8sStork.GetSchedulePolicy(specObj.Name)
		if err != nil {
			return nil, err
		}
		return sp, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationRestore); ok {
		ar, err := k8sStork.GetApplicationRestore(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return ar, nil
	} else if specObj, ok := spec.(*storkapi.ApplicationClone); ok {
		obj, err := k8sStork.GetApplicationClone(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*storkapi.VolumeSnapshotRestore); ok {
		obj, err := k8sStork.GetVolumeSnapshotRestore(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*apapi.AutopilotRule); ok {
		obj, err := k8sAutopilot.GetAutopilotRule(specObj.Name)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*corev1.ServiceAccount); ok {
		obj, err := k8sCore.GetServiceAccount(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*rbacv1.ClusterRole); ok {
		obj, err := k8sRbac.GetClusterRole(specObj.Name)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*rbacv1.ClusterRoleBinding); ok {
		obj, err := k8sRbac.GetClusterRoleBinding(specObj.Name)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*rbacv1.Role); ok {
		obj, err := k8sRbac.GetRole(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*rbacv1.RoleBinding); ok {
		obj, err := k8sRbac.GetRoleBinding(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*batchv1beta1.CronJob); ok {
		obj, err := k8sBatch.GetCronJob(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*batchv1.Job); ok {
		obj, err := k8sBatch.GetJob(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*corev1.LimitRange); ok {
		obj, err := k8sCore.GetLimitRange(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*networkingv1beta1.Ingress); ok {
		obj, err := k8sNetworking.GetIngress(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*monitoringv1.Prometheus); ok {
		obj, err := k8sMonitoring.GetPrometheus(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*monitoringv1.PrometheusRule); ok {
		obj, err := k8sMonitoring.GetPrometheusRule(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*monitoringv1.ServiceMonitor); ok {
		obj, err := k8sMonitoring.GetServiceMonitor(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*corev1.Namespace); ok {
		obj, err := k8sCore.GetNamespace(specObj.Name)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		obj, err := k8sApiExtensions.GetCRD(specObj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*apiextensionsv1.CustomResourceDefinition); ok {
		obj, err := k8sApiExtensions.GetCRDV1beta1(specObj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*policyv1.PodDisruptionBudget); ok {
		obj, err := k8sPolicy.GetPodDisruptionBudget(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*netv1.NetworkPolicy); ok {
		obj, err := k8sCore.GetNetworkPolicy(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*corev1.Endpoints); ok {
		obj, err := k8sCore.GetEndpoints(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*storkapi.ResourceTransformation); ok {
		obj, err := k8sStork.GetMigrationSchedule(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		obj, err := k8sAdmissionRegistration.GetValidatingWebhookConfiguration(specObj.Name)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*kubevirtv1.VirtualMachine); ok {
		obj, err := k8sKubevirt.GetVirtualMachine(specObj.Name, specObj.Namespace)
		if err != nil {
			return nil, err
		}
		return obj, nil
	} else if specObj, ok := spec.(*tektoncdv1.Pipeline); ok {
		obj, err := k8stektoncd.GetPipeline(specObj.Namespace, specObj.Name)
		if err != nil {
			return nil, err
		}
		obj.Kind = "Pipeline"
		return obj, nil
	} else if specObj, ok := spec.(*tektoncdv1.Task); ok {
		obj, err := k8stektoncd.GetTask(specObj.Namespace, specObj.Name)
		if err != nil {
			return nil, err
		}
		obj.Kind = "Task"
		return obj, nil
	} else if specObj, ok := spec.(*tektoncdv1.TaskRun); ok {
		obj, err := k8stektoncd.GetTaskRun(specObj.Namespace, specObj.Name)
		if err != nil {
			return nil, err
		}
		obj.Kind = "TaskRun"
		return obj, nil
	} else if specObj, ok := spec.(*tektoncdv1.PipelineRun); ok {
		obj, err := k8stektoncd.GetPipelineRun(specObj.Namespace, specObj.Name)
		if err != nil {
			return nil, err
		}
		obj.Kind = "PipelineRun"
		return obj, nil
	}

	return nil, fmt.Errorf("unsupported object: %v", reflect.TypeOf(spec))
}

func (k *K8s) createNamespace(app *spec.AppSpec, namespace string, options scheduler.ScheduleOptions) (*corev1.Namespace, error) {
	k8sOps := k8sCore

	t := func() (interface{}, bool, error) {
		metadata := defaultTorpedoLabel
		metadata["app"] = app.Key
		if len(options.Labels) > 0 {
			for k, v := range options.Labels {
				metadata[k] = v
			}
		}

		nsSpec := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   namespace,
				Labels: metadata,
			},
		}
		ns, err := k8sOps.CreateNamespace(nsSpec)

		if k8serrors.IsAlreadyExists(err) {
			if ns, err = k8sOps.GetNamespace(namespace); err == nil {
				return ns, false, nil
			}
		}

		if err != nil {
			return nil, true, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create namespace: %v. Err: %v", namespace, err),
			}
		}

		return ns, false, nil
	}

	nsObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nsObj.(*corev1.Namespace), nil
}

func (k *K8s) isSecureEnabled(appName string, secureApps []string) bool {

	for _, name := range secureApps {
		if name == appName {
			return true
		}
	}
	return false
}

func (k *K8s) createStorageObject(spec interface{}, ns *corev1.Namespace, app *spec.AppSpec,
	options scheduler.ScheduleOptions) (interface{}, error) {

	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sCore.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}

		err = k.addSecurityAnnotation(spec, configMap, app)
		if err != nil {
			return nil, fmt.Errorf("failed to add annotations to storage object: %v", err)
		}

	}

	if strings.Contains(app.Key, "fastpath") {
		vpsSpec := "/torpedo/deployments/customconfigs/fastpath-vps.yaml"
		if _, err := os.Stat(vpsSpec); baseErrors.Is(err, os.ErrNotExist) {
			log.Warnf("Cannot find fastpath-vps.yaml in path %s", vpsSpec)
		} else {
			cmdArgs := []string{"apply", "-f", vpsSpec}
			err = osutils.Kubectl(cmdArgs)
			if err != nil {
				log.Errorf("Error applying spec %s", vpsSpec)
			}
		}
	}

	if strings.Contains(app.Key, "repl-vps") {
		vpsSpec := "/torpedo/deployments/customconfigs/repl-vps.yaml"
		if _, err := os.Stat(vpsSpec); baseErrors.Is(err, os.ErrNotExist) {
			log.Warnf("Cannot find repl-vps.yaml in path %s", vpsSpec)
		} else {
			cmdArgs := []string{"apply", "-f", vpsSpec}
			err = osutils.Kubectl(cmdArgs)
			if err != nil {
				log.Errorf("Error applying spec %s", vpsSpec)
			}
		}
	}

	if obj, ok := spec.(*storageapi.StorageClass); ok {
		obj.Namespace = ns.Name

		if volume.GetStorageProvisioner() != PortworxStrict {
			if options.StorageProvisioner == string(volume.DefaultStorageProvisioner) || options.StorageProvisioner == CsiProvisioner {
				if app.IsCSI {
					obj.Provisioner = CsiProvisioner
				} else {
					obj.Provisioner = volume.GetStorageProvisioner()
				}
			}
		}
		log.Infof("Setting provisioner of %v to %v", obj.Name, obj.Provisioner)

		if k.PureVolumes {
			// Pure NVMe volumes don't support QoS yet, so we need to remove it for NVMe tests
			if k.PureSANType == "NVMEOF-RDMA" {
				delete(obj.Parameters, "max_iops")
				delete(obj.Parameters, "max_bandwidth")
				log.Infof("Removing QoS parameters in %v for Pure NVMeoF-RDMA SAN type", obj.Name)
			}
			if k.RunCSISnapshotAndRestoreManyTest {
				immediate := storageapi.VolumeBindingImmediate
				obj.VolumeBindingMode = &immediate
				log.Infof("Setting SC %s volumebinding mode to immediate ", obj.Name)
			}
			// If we specified a pod name, we need to set the Pure FA pod name in the storage class
			if k.PureFADAPod != "" {
				if backend, ok := obj.Parameters["backend"]; ok && backend == "pure_block" {
					obj.Parameters["pure_fa_pod_name"] = k.PureFADAPod
				}
			}
		}
		sc, err := k8sStorage.CreateStorageClass(obj)
		if k8serrors.IsAlreadyExists(err) {
			if sc, err = k8sStorage.GetStorageClass(obj.Name); err == nil {
				log.Infof("[%v] Found existing storage class: %v", app.Key, sc.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				sc.Kind = "StorageClass"

				return sc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create storage class: %v. Err: %v", obj.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		sc.Kind = "StorageClass"

		log.Infof("[%v] Created storage class: %v", app.Key, sc.Name)
		return sc, nil

	} else if obj, ok := spec.(*corev1.PersistentVolumeClaim); ok {
		clonedPVC := false
		if source, exists := obj.Annotations["pvc.source"]; exists {
			if source == "cloned" {
				clonedPVC = true
			}
		}
		if !clonedPVC {
			obj.Namespace = ns.Name
			k.substituteNamespaceInPVC(obj, ns.Name)
		}

		if pvcLabelsAnnotationValue, ok := obj.Annotations[pvcLabelsAnnotationKey]; ok {
			pvcLabelsEnabled, _ := strconv.ParseBool(pvcLabelsAnnotationValue)
			if pvcLabelsEnabled {
				if len(options.Labels) > 0 {
					k.addLabelsToPVC(obj, options.Labels)
				}
			}
		}

		if pvcNodesAnnotationValue, ok := obj.Annotations[pvcNodesAnnotationKey]; ok {
			pvcNodesEnabled, _ := strconv.ParseBool(pvcNodesAnnotationValue)
			if pvcNodesEnabled {
				if len(options.PvcNodesAnnotation) > 0 {
					k.addAnnotationsToPVC(obj, map[string]string{"nodes": strings.Join(options.PvcNodesAnnotation, ",")})
				}
			}
		}

		newPvcObj := obj.DeepCopy()
		if options.PvcSize > 0 {
			autopilotEnabled := false
			if pvcAnnotationValue, ok := newPvcObj.Annotations[autopilotEnabledAnnotationKey]; ok {
				autopilotEnabled, _ = strconv.ParseBool(pvcAnnotationValue)
			}
			if autopilotEnabled {
				newPvcSize, parseErr := resource.ParseQuantity(strconv.FormatInt(options.PvcSize, 10))
				if parseErr != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: fmt.Sprintf("Failed to create PVC: %v. Err: %v", obj.Name, parseErr),
					}
				}
				log.Infof("[%v] Using custom PVC size: %v for PVC: %v", app.Key, newPvcSize.String(), obj.Name)
				newPvcObj.Spec.Resources.Requests[corev1.ResourceStorage] = newPvcSize
			}
		}
		pvc, err := k8sCore.CreatePersistentVolumeClaim(newPvcObj)
		if k8serrors.IsAlreadyExists(err) {
			if pvc, err = k8sCore.GetPersistentVolumeClaim(newPvcObj.Name, newPvcObj.Namespace); err == nil {
				log.Infof("[%v] Found existing PVC: %v", app.Key, pvc.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				pvc.Kind = "PersistentVolumeClaim"

				return pvc, nil
			}
		}
		if clonedPVC {
			endpointAnnotation, ok := pvc.Annotations[cdiPvcImportEndpointAnnotationKey]
			if ok && endpointAnnotation != "" {
				t := func() (interface{}, bool, error) {
					pvc, err = k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
					messageAnnotation, ok := pvc.Annotations[cdiPvcRunningMessageAnnotationKey]
					if ok {
						if messageAnnotation == cdiImportComplete {
							log.Infof("%s - [%s]", cdiPvcRunningMessageAnnotationKey, pvc.Annotations[cdiPvcRunningMessageAnnotationKey])
							return "", false, nil
						}
						return "", true, fmt.Errorf("waiting for annotation [%s] in pvc [%s] in namespace [%s] to be %s, but got %s",
							cdiPvcRunningMessageAnnotationKey, obj.Name, obj.Namespace, cdiImportComplete, pvc.Annotations[cdiPvcRunningMessageAnnotationKey])
					} else {
						return "", true, fmt.Errorf("annotation [%s] not found in pvc [%s] in namespace [%s]",
							cdiPvcRunningMessageAnnotationKey, obj.Name, obj.Namespace)
					}
				}
				_, err = task.DoRetryWithTimeout(t, cdiImageImportTimeout, cdiImageImportRetry)
				if err != nil {
					return pvc, err
				}
			}

		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PVC: %v. Err: %v", newPvcObj.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		pvc.Kind = "PersistentVolumeClaim"

		log.Infof("[%v] Created PVC: %v", app.Key, pvc.Name)

		autopilotEnabled := false
		if pvcAnnotationValue, ok := pvc.Annotations[autopilotEnabledAnnotationKey]; ok {
			autopilotEnabled, _ = strconv.ParseBool(pvcAnnotationValue)
		}
		if autopilotEnabled {
			apRule := options.AutopilotRule
			labels := options.Labels
			if apRule.Name != "" {
				apRule.Labels = defaultTorpedoLabel
				labelSelector := metav1.LabelSelector{MatchLabels: labels}
				apRule.Spec.Selector = apapi.RuleObjectSelector{LabelSelector: labelSelector}
				apRule.Spec.NamespaceSelector = apapi.RuleObjectSelector{LabelSelector: labelSelector}
				_, err := k.CreateAutopilotRule(apRule)
				if err != nil {
					return nil, err
				}
			}
		}
		return pvc, nil

	} else if obj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		obj.Metadata.Namespace = ns.Name
		snap, err := k8sExternalStorage.CreateSnapshot(obj)
		if k8serrors.IsAlreadyExists(err) {
			if snap, err = k8sExternalStorage.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err == nil {
				log.Infof("[%v] Found existing snapshot: %v", app.Key, snap.Metadata.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				snap.Kind = "VolumeSnapshot"

				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Snapshot: %v. Err: %v", obj.Metadata.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		snap.Kind = "VolumeSnapshot"

		log.Infof("[%v] Created Snapshot: %v", app.Key, snap.Metadata.Name)
		return snap, nil
	} else if obj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		obj.Namespace = ns.Name
		snap, err := k8sStork.CreateGroupSnapshot(obj)
		if k8serrors.IsAlreadyExists(err) {
			if snap, err = k8sStork.GetGroupSnapshot(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing group snapshot: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create group snapshot: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Group snapshot: %v", app.Key, snap.Name)
		return snap, nil

	} else if obj, ok := spec.(*corev1.PersistentVolume); ok {
		obj.Spec.ClaimRef.Namespace = ns.Name
		pv, err := k8sCore.CreatePersistentVolume(obj)
		if k8serrors.IsAlreadyExists(err) {
			if pv, err = k8sCore.GetPersistentVolume(obj.Name); err == nil {
				log.Infof("[%v] Found existing PersistentVolume: %v", app.Key, pv.Name)
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PersistentVolume: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created PersistentVolume: %v", app.Key, pv.Name)
		return pv, nil

	}
	return nil, nil
}

func (k *K8s) substituteNamespaceInPVC(pvc *corev1.PersistentVolumeClaim, ns string) {
	pvc.Name = namespaceRegex.ReplaceAllString(pvc.Name, ns)
	for k, v := range pvc.Annotations {
		pvc.Annotations[k] = namespaceRegex.ReplaceAllString(v, ns)
	}
}

func (k *K8s) createVolumeSnapshotRestore(specObj interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sCore.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}

		err = k.addSecurityAnnotation(specObj, configMap, app)
		if err != nil {
			return nil, fmt.Errorf("failed to add annotations to storage object: %v", err)
		}

	}

	if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
		obj.Namespace = ns.Name
		snapRestore, err := k8sStork.CreateVolumeSnapshotRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created VolumeSnapshotRestore: %v", app.Key, snapRestore.Name)
		return snapRestore, nil
	}

	return nil, nil
}

func (k *K8s) addSecurityAnnotation(spec interface{}, configMap *corev1.ConfigMap, app *spec.AppSpec) error {
	// log.Debugf("Config Map details: %v", configMap.Data)

	secretNameKeyFlag := false
	secretNamespaceKeyFlag := false
	encryptionFlag := false
	if _, ok := configMap.Data[secretNameKey]; ok {
		secretNameKeyFlag = true
	}
	if _, ok := configMap.Data[secretNamespaceKey]; ok {
		secretNamespaceKeyFlag = true
	}
	if _, ok := configMap.Data[encryptionKey]; ok {
		encryptionFlag = true
	}

	setSecureParams := func(obj *storageapi.StorageClass) {
		if encryptionFlag {
			log.Infof("Adding encryption parameter to storage class app %s", app.Key)
			obj.Parameters[encryptionName] = "true"
		}
		if app.IsCSI {
			obj.Parameters[CsiProvisionerSecretName] = configMap.Data[secretNameKey]
			obj.Parameters[CsiProvisionerSecretNamespace] = configMap.Data[secretNamespaceKey]
			obj.Parameters[CsiNodePublishSecretName] = configMap.Data[secretNameKey]
			obj.Parameters[CsiNodePublishSecretNamespace] = configMap.Data[secretNamespaceKey]
			obj.Parameters[CsiControllerExpandSecretName] = configMap.Data[secretNameKey]
			obj.Parameters[CsiControllerExpandSecretNamespace] = configMap.Data[secretNamespaceKey]
		}
		if strings.Contains(volume.GetStorageProvisioner(), "pxd") {
			if secretNameKeyFlag {
				obj.Parameters[CsiProvisionerSecretName] = configMap.Data[secretNameKey]
				obj.Parameters[CsiNodePublishSecretName] = configMap.Data[secretNameKey]
				obj.Parameters[CsiControllerExpandSecretName] = configMap.Data[secretNameKey]
			}
			if secretNamespaceKeyFlag {
				obj.Parameters[CsiProvisionerSecretNamespace] = configMap.Data[secretNamespaceKey]
				obj.Parameters[CsiNodePublishSecretNamespace] = configMap.Data[secretNamespaceKey]
				obj.Parameters[CsiControllerExpandSecretNamespace] = configMap.Data[secretNamespaceKey]
			}
		} else {
			if secretNameKeyFlag {
				obj.Parameters[secretName] = configMap.Data[secretNameKey]
			}
			if secretNamespaceKeyFlag {
				obj.Parameters[secretNamespace] = configMap.Data[secretNamespaceKey]
			}
		}
	}

	if obj, ok := spec.(*storageapi.StorageClass); ok {

		if k.secretConfigMapName != "" {
			if obj.Parameters == nil {
				obj.Parameters = make(map[string]string)
			}

			//if secure-apps list is provided, check if the app is in the list and set the secure parameters for only that app
			if len(k.secureApps) > 0 && k.isSecureEnabled(app.Key, k.secureApps) {
				setSecureParams(obj)
			}
			//if secure-apps list is not provided, set the secure parameters for all apps if secret config map is provided
			if len(k.secureApps) == 0 {
				setSecureParams(obj)
			}
		}

	} else if obj, ok := spec.(*corev1.PersistentVolumeClaim); ok {

		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		log.Infof("Secret name key flag and CSI flag for PVC: %v %v", secretNameKeyFlag, app.IsCSI)
		if secretNameKeyFlag && !app.IsCSI {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag && !app.IsCSI {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}

	} else if obj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		if obj.Metadata.Annotations == nil {
			obj.Metadata.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Metadata.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Metadata.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		var pvcList []corev1.PersistentVolumeClaim
		for _, pvc := range obj.Spec.VolumeClaimTemplates {
			if pvc.Annotations == nil {
				pvc.Annotations = make(map[string]string)
			}
			if secretNameKeyFlag {
				pvc.Annotations[secretName] = configMap.Data[secretNameKey]
			}
			if secretNamespaceKeyFlag {
				pvc.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
			}
			pvcList = append(pvcList, pvc)
		}
		obj.Spec.VolumeClaimTemplates = pvcList
	} else if obj, ok := spec.(*storkapi.ApplicationBackup); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.ApplicationClone); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.ApplicationRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.Migration); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.MigrationSchedule); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.VolumeSnapshotRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	} else if obj, ok := spec.(*storkapi.ClusterPair); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		if secretNameKeyFlag {
			obj.Annotations[secretName] = configMap.Data[secretNameKey]
		}
		if secretNamespaceKeyFlag {
			obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
		}
	}
	return nil
}

func (k *K8s) createCoreObject(spec interface{}, ns *corev1.Namespace, app *spec.AppSpec,
	options scheduler.ScheduleOptions) (interface{}, error) {
	if obj, ok := spec.(*appsapi.Deployment); ok {
		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		if options.Scheduler != "" {
			obj.Spec.Template.Spec.SchedulerName = options.Scheduler
		}
		if len(options.TopologyLabels) > 0 {
			Affinity := getAffinity(options.TopologyLabels)
			obj.Spec.Template.Spec.Affinity = Affinity.DeepCopy()
		}
		fmt.Printf("%+v\n", obj.Spec.Template.Spec)
		if obj.Spec.Template.Spec.Affinity != nil && obj.Spec.Template.Spec.Affinity.NodeAffinity != nil {
			nodeAff := obj.Spec.Template.Spec.Affinity.NodeAffinity
			labels := getLabelsFromNodeAffinity(nodeAff)
			val, ok := labels[NodeType]
			if ok {
				if val == FastpathNodeType {
					k.AddLabelOnNode(node.GetStorageDriverNodes()[0], NodeType, FastpathNodeType)
				}
			}

		}

		secret, err := k.createDockerRegistrySecret(app.Key, obj.Namespace)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Docker registry secret for deployment: %s. Err: %v", obj.Name, err),
			}
		}
		if secret != nil {
			obj.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{{Name: secret.Name}}
		}
		if len(options.Nodes) > 0 && len(options.Labels) > 0 {
			obj.Spec.Template.Spec.NodeSelector = options.Labels
		}
		dep, err := k8sApps.CreateDeployment(obj, metav1.CreateOptions{})
		if k8serrors.IsAlreadyExists(err) {
			if dep, err = k8sApps.GetDeployment(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing deployment: %v", app.Key, dep.Name)
				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				dep.Kind = "Deployment"
				return dep, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Deployment: %v. Err: %v", obj.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		dep.Kind = "Deployment"

		log.Infof("[%v] Created deployment: %v", app.Key, dep.Name)
		return dep, nil

	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		// Add security annotations if running with auth-enabled
		configMapName := k.secretConfigMapName
		if configMapName != "" {
			configMap, err := k8sCore.GetConfigMap(configMapName, "default")
			if err != nil {
				return nil, &scheduler.ErrFailedToGetConfigMap{
					Name:  configMapName,
					Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
				}
			}

			err = k.addSecurityAnnotation(obj, configMap, app)
			if err != nil {
				return nil, fmt.Errorf("failed to add annotations to core object: %v", err)
			}
		}

		if len(options.TopologyLabels) > 0 {
			Affinity := getAffinity(options.TopologyLabels)
			obj.Spec.Template.Spec.Affinity = Affinity.DeepCopy()
		}

		obj.Spec.Template.Spec.Containers = k.substituteNamespaceInContainers(obj.Spec.Template.Spec.Containers, ns.Name)

		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		if options.Scheduler != "" {
			obj.Spec.Template.Spec.SchedulerName = options.Scheduler
		}

		var pvcList []corev1.PersistentVolumeClaim
		for _, pvc := range obj.Spec.VolumeClaimTemplates {
			if pvc.Annotations == nil {
				pvc.Annotations = make(map[string]string)
			}

			pvcList = append(pvcList, pvc)
		}
		obj.Spec.VolumeClaimTemplates = pvcList

		secret, err := k.createDockerRegistrySecret(app.Key, obj.Namespace)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Docker registry secret for statefulset: %s. Err: %v", obj.Name, err),
			}
		}
		if secret != nil {
			obj.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{{Name: secret.Name}}
		}
		if len(options.Nodes) > 0 && len(options.Labels) > 0 {
			obj.Spec.Template.Spec.NodeSelector = options.Labels
		}
		ss, err := k8sApps.CreateStatefulSet(obj, metav1.CreateOptions{})
		if k8serrors.IsAlreadyExists(err) {
			if ss, err = k8sApps.GetStatefulSet(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing StatefulSet: %v", app.Key, ss.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				ss.Kind = "StatefulSet"

				return ss, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create StatefulSet: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created StatefulSet: %v", app.Key, ss.Name)

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		ss.Kind = "StatefulSet"

		return ss, nil

	} else if obj, ok := spec.(*corev1.Service); ok {
		obj.Namespace = ns.Name
		svc, err := k8sCore.CreateService(obj)
		if k8serrors.IsAlreadyExists(err) {
			if svc, err = k8sCore.GetService(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Service: %v", app.Key, svc.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				svc.Kind = "Service"

				return svc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service: %v. Err: %v", obj.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		svc.Kind = "Service"

		log.Infof("[%v] Created Service: %v", app.Key, svc.Name)
		return svc, nil

	} else if obj, ok := spec.(*corev1.Secret); ok {
		obj.Namespace = ns.Name
		if k.SecretType == scheduler.SecretVault {
			if err := k.createVaultSecret(obj); err != nil {
				return nil, err
			}
		}
		secret, err := k8sCore.CreateSecret(obj)
		if k8serrors.IsAlreadyExists(err) {
			if secret, err = k8sCore.GetSecret(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Secret: %v", app.Key, secret.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				secret.Kind = "Secret"

				return secret, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Secret: %v. Err: %v", obj.Name, err),
			}
		}

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		secret.Kind = "Secret"

		log.Infof("[%v] Created Secret: %v", app.Key, secret.Name)
		return secret, nil
	} else if obj, ok := spec.(*storkapi.Rule); ok {
		if obj.Namespace != "kube-system" {
			obj.Namespace = ns.Name
		}
		rule, err := k8sStork.CreateRule(obj)
		if k8serrors.IsAlreadyExists(err) {
			if rule, err = k8sStork.GetRule(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Rule: %v", app.Key, rule.GetName())
				return rule, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Rule: %v, Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created Rule: %v", app.Key, rule.GetName())
		return rule, nil
	} else if obj, ok := spec.(*corev1.Pod); ok {
		obj.Namespace = ns.Name
		if options.Scheduler != "" {
			obj.Spec.SchedulerName = options.Scheduler
		}
		secret, err := k.createDockerRegistrySecret(obj.Namespace, obj.Namespace)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Docker registry secret for pod: %s. Err: %v", obj.Name, err),
			}
		}
		if secret != nil {
			obj.Spec.ImagePullSecrets = []v1.LocalObjectReference{{Name: secret.Name}}
		}

		pod, err := k8sCore.CreatePod(obj)
		if k8serrors.IsAlreadyExists(err) {
			if pod, err := k8sCore.GetPodByName(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Pods: %v", app.Key, pod.Name)
				return pod, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToSchedulePod{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Pod: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Pod: %v", app.Key, pod.Name)
		return pod, nil
	} else if obj, ok := spec.(*corev1.ConfigMap); ok {
		obj.Namespace = ns.Name
		configMap, err := k8sCore.CreateConfigMap(obj)
		if k8serrors.IsAlreadyExists(err) {
			if configMap, err = k8sCore.GetConfigMap(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Config Maps: %v", app.Key, configMap.Name)

				// This is a hack because the `Kind` field is empty due to K8s bug.
				// Refer https://github.com/portworx/torpedo/pull/1345
				configMap.Kind = "ConfigMap"

				return configMap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Config Map: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Config Map: %v", app.Key, configMap.Name)

		// This is a hack because the `Kind` field is empty due to K8s bug.
		// Refer https://github.com/portworx/torpedo/pull/1345
		configMap.Kind = "ConfigMap"

		return configMap, nil
	} else if obj, ok := spec.(*v1.Endpoints); ok {
		obj.Namespace = ns.Name
		endpoints, err := k8sCore.CreateEndpoints(obj)
		if k8serrors.IsAlreadyExists(err) {
			if endpoints, err = k8sCore.GetEndpoints(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Endpoints: %v", app.Key, endpoints.Name)
				return endpoints, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Endpoints: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Endpoints: %v", app.Key, endpoints.Name)
		return endpoints, nil
	} else if obj, ok := spec.(*netv1.NetworkPolicy); ok {
		obj.Namespace = ns.Name
		networkPolicy, err := k8sCore.CreateNetworkPolicy(obj)
		if k8serrors.IsAlreadyExists(err) {
			if networkPolicy, err = k8sCore.GetNetworkPolicy(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing NetworkPolicy: %v", app.Key, networkPolicy.Name)
				return networkPolicy, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create NetworkPolicy: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created NetworkPolicy: %v", app.Key, networkPolicy.Name)
		return networkPolicy, nil
	}

	return nil, nil
}

func (k *K8s) createVaultSecret(obj *corev1.Secret) error {
	client, err := vaultapi.NewClient(nil)
	if err != nil {
		return err
	}
	if err = client.SetAddress(k.VaultAddress); err != nil {
		return err
	}
	client.SetToken(k.VaultToken)

	c := client.Logical()
	data := make(map[string]interface{})
	for key, value := range obj.Data {
		data[key] = string(value)
	}

	if _, err := c.Write(fmt.Sprintf("secret/%s", obj.Name), data); err != nil {
		return err
	}
	return nil
}

func (k *K8s) destroyCoreObject(spec interface{}, opts map[string]bool, app *spec.AppSpec) (interface{}, error) {
	var pods interface{}
	var podList []*corev1.Pod
	var err error
	if obj, ok := spec.(*appsapi.Deployment); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sApps.GetDeploymentPods(obj); err != nil {
				log.Warnf("[%s] Error getting deployment pods. Err: %v", app.Key, err)
			}
		}
		err := k8sApps.DeleteDeployment(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Deployment: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sApps.GetStatefulSetPods(obj); err != nil {
				log.Warnf("[%v] Error getting statefulset pods. Err: %v", app.Key, err)
			}
		}
		err := k8sApps.DeleteStatefulSet(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy stateful set: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*corev1.Service); ok {
		err := k8sCore.DeleteService(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Service: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed Service: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*storkapi.Rule); ok {
		err := k8sStork.DeleteRule(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Rule: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed Rule: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*corev1.Pod); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			pod, err := k8sCore.GetPodByName(obj.Name, obj.Namespace)
			if err != nil {
				log.Warnf("[%v] Error getting pods. Err: %v", app.Key, err)
			}
			podList = append(podList, pod)
			pods = podList
		}
		err := k8sCore.DeletePod(obj.Name, obj.Namespace, false)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyPod{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Pod: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed Pod: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*corev1.ConfigMap); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			_, err := k8sCore.GetConfigMap(obj.Name, obj.Namespace)
			if err != nil {
				log.Warnf("[%v] Error getting config maps. Err: %v", app.Key, err)
			}
		}
		err := k8sCore.DeleteConfigMap(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy config map: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed Config Map: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*apapi.AutopilotRule); ok {
		err := k8sAutopilot.DeleteAutopilotRule(obj.Name)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy AutopilotRule: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed AutopilotRule: %v", app.Key, obj.Name)
	}

	return pods, nil

}

// destroyAdmissionRegistrationObjects destroys objects in the `AdmissionRegistration` group (like ValidatingWebhookConfiguration)
func (k *K8s) destroyAdmissionRegistrationObjects(spec interface{}, app *spec.AppSpec) error {

	if obj, ok := spec.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		err := k8sAdmissionRegistration.DeleteValidatingWebhookConfiguration(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ValidatingWebhookConfiguration: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed ValidatingWebhookConfiguration: %v", app.Key, obj.Name)
			return nil
		}
	} else if obj, ok := spec.(admissionregistrationv1beta1.ValidatingWebhookConfiguration); ok {
		err := k8sAdmissionRegistration.DeleteValidatingWebhookConfigurationV1beta1(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ValidatingWebhookConfigurationV1beta1: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed ValidatingWebhookConfigurationV1beta1: %v", app.Key, obj.Name)
			return nil
		}
	}

	return nil
}

// destroyCRDObjects is used to destroy Resources in the group `apiextensions` (like CRDs)
func (k *K8s) destroyCRDObjects(spec interface{}, app *spec.AppSpec) error {

	if obj, ok := spec.(*apiextensionsv1.CustomResourceDefinition); ok {
		err := k8sApiExtensions.DeleteCRD(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy CRD: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed CRD: %v", app.Key, obj.Name)
			return nil
		}
	} else if obj, ok := spec.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		err := k8sApiExtensions.DeleteCRDV1beta1(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy CRDV1beta1: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed CRDV1beta1: %v", app.Key, obj.Name)
			return nil
		}
	}

	return nil
}

func (k *K8s) substituteNamespaceInContainers(containers []corev1.Container, ns string) []corev1.Container {
	var updatedContainers []corev1.Container
	for _, container := range containers {
		var temp []string
		for _, arg := range container.Args {
			temp = append(temp, namespaceRegex.ReplaceAllString(arg, ns))
		}
		container.Args = temp
		updatedContainers = append(updatedContainers, container)
	}
	return updatedContainers
}

func (k *K8s) substituteNamespaceInVolumes(volumes []corev1.Volume, ns string) []corev1.Volume {
	var updatedVolumes []corev1.Volume
	for _, vol := range volumes {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			claimName := namespaceRegex.ReplaceAllString(vol.VolumeSource.PersistentVolumeClaim.ClaimName, ns)
			vol.VolumeSource.PersistentVolumeClaim.ClaimName = claimName
		}
		updatedVolumes = append(updatedVolumes, vol)
	}
	return updatedVolumes
}

// ValidateTopologyLabel validate Topology for Running Pods
func (k *K8s) ValidateTopologyLabel(ctx *scheduler.Context) error {
	var err error
	var zone string
	var podList *corev1.PodList

	log.Info("Validating pods topology")
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			var dep *appsapi.Deployment
			if dep, err = k8sApps.GetDeployment(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
			nodeAff := dep.Spec.Template.Spec.Affinity.NodeAffinity
			labels := getLabelsFromNodeAffinity(nodeAff)
			zone = labels[TopologyZoneK8sNodeLabel]
			if podList, err = k8sCore.GetPods(obj.Namespace, nil); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
			if err = k.validatePodsTopology(podList, zone); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			var ss *appsapi.StatefulSet
			if ss, err = k8sApps.GetStatefulSet(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
			nodeAff := ss.Spec.Template.Spec.Affinity.NodeAffinity
			labels := getLabelsFromNodeAffinity(nodeAff)
			zone = labels[TopologyZoneK8sNodeLabel]
			if podList, err = k8sCore.GetPods(obj.Namespace, nil); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
			if err = k.validatePodsTopology(podList, zone); err != nil {
				return &scheduler.ErrFailedToValidateTopologyLabel{
					NameSpace: obj.Namespace,
					Cause:     err,
				}
			}
		}
	}
	return nil
}

// validatePodsTopology validates pods scheduled on matched label node
func (k *K8s) validatePodsTopology(podList *v1.PodList, labelValue string) error {
	for _, pod := range podList.Items {
		hostIP := pod.Status.HostIP
		if node, err := node.GetNodeByIP(hostIP); err != nil {
			if node.TopologyZone != labelValue || node.TopologyRegion != labelValue {
				return &scheduler.ErrTopologyLabelMismatch{
					PodName: pod.Name,
					Cause: fmt.Sprintf(
						"Topology Mismatch.Pod [%s] on zone [%s] scheduled on a node [%s].",
						pod.Name, labelValue, node.Name),
				}
			}
		}
		log.Infof("Successfully matched Pod: [%s] topology", pod.Name)
	}
	return nil
}

// WaitForRunning   wait for running
func (k *K8s) WaitForRunning(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			if err := k8sApps.ValidateDeployment(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Deployment: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*pds.ModelsDeployment); ok {
			var ss *appsapi.StatefulSet
			var err error
			if ss, err = k8sApps.GetStatefulSet(obj.GetClusterResourceName(), *obj.Namespace.Name); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v,Namespace: %v. Err: %v", obj.GetClusterResourceName(), *obj.Namespace.Name, err),
				}
			}

			if err := k8sApps.ValidateStatefulSet(ss, timeout*time.Duration(*ss.Spec.Replicas)); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v,Namespace: %v. Err: %v", obj.Name, *obj.Namespace.Name, err),
				}
			}

			log.Infof("[%v] Validated statefulset: %v", ctx.App.Key, *obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if err := k8sApps.ValidateStatefulSet(obj, timeout*time.Duration(*obj.Spec.Replicas)); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated statefulset: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*corev1.Service); ok {
			svc, err := k8sCore.GetService(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Service: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated Service: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := specObj.(*storkapi.Rule); ok {
			svc, err := k8sStork.GetRule(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Rule: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated Rule: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := specObj.(*corev1.Pod); ok {
			if err := k8sCore.ValidatePod(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidatePod{
					App: ctx.App,
					Cause: fmt.Sprintf("Failed to validate Pod: [%s] %s. Err: Pod is not ready %v",
						obj.Namespace, obj.Name, obj.Status),
				}
			}

			log.Infof("[%v] Validated pod: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ClusterPair); ok {
			if err := k8sStork.ValidateClusterPair(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated ClusterPair: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.Migration); ok {
			if err := k8sStork.ValidateMigration(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated Migration: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
			if _, err := k8sStork.ValidateMigrationSchedule(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated MigrationSchedule: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.BackupLocation); ok {
			if err := k8sStork.ValidateBackupLocation(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate BackupLocation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated BackupLocation: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
			if err := k8sStork.ValidateApplicationBackup(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationBackup: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated ApplicationBackup: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
			if err := k8sStork.ValidateApplicationRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated ApplicationRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
			if err := k8sStork.ValidateApplicationClone(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationClone: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated ApplicationClone: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			if err := k8sStork.ValidateVolumeSnapshotRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout,
				retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Metadata.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshot: %v. Err: %v", obj.Metadata.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*apapi.AutopilotRule); ok {
			if _, err := k8sAutopilot.GetAutopilotRule(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate AutopilotRule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated AutopilotRule: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*networkingv1beta1.Ingress); ok {
			if err := k8sNetworking.ValidateIngress(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Ingress: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated Ingress: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*batchv1beta1.CronJob); ok {
			if err := k8sBatch.ValidateCronJobV1beta1(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate CronJob: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated CronJob: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*batchv1.Job); ok {
			if err := k8sBatch.ValidateJob(obj.Name, obj.ObjectMeta.Namespace, timeout); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Job: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}

			log.Infof("[%v] Validated Job: %v", ctx.App.Key, obj.Name)

		} else if obj, ok := specObj.(*storkapi.ResourceTransformation); ok {
			if err := k8sStork.ValidateResourceTransformation(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ResourceTransformation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated ResourceTransformation: %v", ctx.App.Key, obj.Name)

		} else if obj, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
			if err := k8sKubevirt.ValidateVirtualMachineRunning(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate VirtualMachineRunning State: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			log.Infof("[%v] Validated VirtualMachine running state: %v", ctx.App.Key, obj.Name)

		}
	}

	isPodTerminating := func() (interface{}, bool, error) {
		var terminatingPods []string
		pods, err := k.getPodsForApp(ctx)
		// ignore error if no pods are found; retry for other cases
		if err == schederrors.ErrPodsNotFound {
			return nil, false, nil
		} else if err != nil {
			return nil, true, fmt.Errorf("failed to get pods for app %v: %w", ctx.App.Key, err)
		}
		for _, pod := range pods {
			if !pod.DeletionTimestamp.IsZero() {
				terminatingPods = append(terminatingPods, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
			}
		}
		if len(terminatingPods) > 0 {
			return nil, true, fmt.Errorf("terminating pods: %v", terminatingPods)
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(isPodTerminating, k8sDestroyTimeout, DefaultRetryInterval)
	if err != nil {
		log.Warnf("Timed out waiting for app %v's pods to terminate: %v", ctx.App.Key, err)
		return err
	}
	return nil
}

// Destroy destroy
func (k *K8s) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	var podList []corev1.Pod

	// destruction of CustomResourceObjects must most likely be done *first*,
	// as it may have resources that depend on other resources, which should be deleted *after* this
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyCustomResourceObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			} else {
				return nil, false, nil
			}
		}
		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	var removeSpecs []interface{}
	for _, appSpec := range ctx.App.SpecList {
		if repoInfo, ok := appSpec.(*scheduler.HelmRepo); ok {
			specs, err := k.UnInstallHelmChart(repoInfo)
			if err != nil {
				return err
			}
			removeSpecs = append(removeSpecs, specs...)
		}
	}
	// helm uninstall would delete objects automatically so skip destroy for those
	err := k.RemoveAppSpecsByName(ctx, removeSpecs)
	if err != nil {
		return err
	}

	k8sOps := k8sAutopilot
	apRule := ctx.ScheduleOptions.AutopilotRule
	if apRule.Name != "" {
		if err := k8sOps.DeleteAutopilotRule(apRule.ObjectMeta.Name); err != nil {
			if err != nil {
				if strings.Contains(err.Error(), "not found") {
					log.Infof("deletion of AR failed: %s, expected since we dont deploy new AR", apRule.ObjectMeta.Name)
					return nil
				}
				return err
			}
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyAdmissionRegistrationObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			} else {
				return nil, false, nil
			}
		}
		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyRbacObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			} else {
				return nil, false, nil
			}
		}
		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			currPods, err := k.destroyCoreObject(appSpec, opts, ctx.App)
			// during helm upgrade or uninstall, objects may be deleted but not removed from the SpecList
			// so tolerate non-existing errors for those objects during tear down
			if err != nil && !strings.Contains(err.Error(), "not found") {
				return nil, true, err
			}
			return currPods, false, nil
		}
		pods, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval)
		if err != nil {
			// in case we're not waiting for resource cleanup
			if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; !ok || !value {
				return err
			}
			if pods != nil {
				podList = append(podList, pods.([]corev1.Pod)...)
			}
			// we're ignoring this error since we want to verify cleanup down below, so simply logging it
			log.Warnf("Failed to destroy core objects. Cause: %v", err)
		}
	}
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyVolumeSnapshotRestoreObject(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyMigrationObject(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyBackupObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyServiceMonitorObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyPodDisruptionBudgetObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyVirtualMachineObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyCRDObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			} else {
				return nil, false, nil
			}
		}
		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		if err := k.WaitForDestroy(ctx, k8sDestroyTimeout); err != nil {
			return err
		}
		if err := k.waitForCleanup(ctx, podList); err != nil {
			return err
		}
	} else if value, ok = opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := k.WaitForDestroy(ctx, k8sDestroyTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (k *K8s) waitForCleanup(ctx *scheduler.Context, podList []corev1.Pod) error {
	for _, pod := range podList {
		t := func() (interface{}, bool, error) {
			return nil, true, k.validateVolumeDirCleanup(pod.UID, ctx.App)
		}
		if _, err := task.DoRetryWithTimeout(t, volDirCleanupTimeout, DefaultRetryInterval); err != nil {
			return err
		}
		log.Infof("Validated resource cleanup for pod: %v", pod.UID)
	}
	return nil
}

func (k *K8s) validateVolumeDirCleanup(podUID types.UID, app *spec.AppSpec) error {
	podVolDir := k.getVolumeDirPath(podUID)
	driver, _ := node.Get(k.NodeDriverName)
	options := node.FindOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		MinDepth: 1,
		MaxDepth: 1,
	}

	for _, n := range node.GetWorkerNodes() {
		if volDir, err := driver.FindFiles(podVolDir, n, options); err != nil {
			return err
		} else if strings.TrimSpace(volDir) != "" {
			return &scheduler.ErrFailedToDeleteVolumeDirForPod{
				App:   app,
				Cause: fmt.Sprintf("Volume directory for pod %v still exists in node: %v", podUID, n.Name),
			}
		}
	}

	return nil
}

func (k *K8s) getVolumeDirPath(podUID types.UID) string {
	return filepath.Join(k8sPodsRootDir, string(podUID), "volumes")
}

// WaitForDestroy wait for schedule context destroy
func (k *K8s) WaitForDestroy(ctx *scheduler.Context, timeout time.Duration) error {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			if err := k8sApps.ValidateTerminatedDeployment(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of deployment: %v, namespace: %s. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated destroy of Deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if err := k8sApps.ValidateTerminatedStatefulSet(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of statefulset: %v, namespace: %s Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated destroy of StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*corev1.Service); ok {
			if err := k8sCore.ValidateDeletedService(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of service: %v, namespace: %s. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated destroy of Service: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*corev1.Pod); ok {
			if err := k8sCore.WaitForPodDeletion(obj.UID, obj.Namespace, deleteTasksWaitTimeout); err != nil {
				return &scheduler.ErrFailedToValidatePodDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of pod: %v,namespace:%s. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated destroy of Pod: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

// SelectiveWaitForTermination waits for application pods to be terminated except on the nodes
// provided in the exclude list
func (k *K8s) SelectiveWaitForTermination(ctx *scheduler.Context, timeout time.Duration, excludeList []node.Node) error {
	t := func() (interface{}, bool, error) {
		podNames, err := filterPodsByNodes(ctx, excludeList)
		if err != nil {
			return nil, true, err
		}
		if len(podNames) > 0 {
			return nil, true, fmt.Errorf("pods %s still present in the system", podNames)
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, DefaultRetryInterval); err != nil {
		return err
	}
	return nil
}

// filterPodsByNodes returns a list of pod names started as part of the provided context
// and not running on the nodes provided in the exclude list
func filterPodsByNodes(ctx *scheduler.Context, excludeList []node.Node) ([]string, error) {
	allPods := make(map[types.UID]corev1.Pod)
	namespaces := make(map[string]string)
	for _, specObj := range ctx.App.SpecList {
		var pods []corev1.Pod
		var err error
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			if pods, err = k8sApps.GetDeploymentPods(obj); err != nil && err != schederrors.ErrPodsNotFound {
				return nil, &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get deployment: %v. Err: %v", obj.Name, err),
				}
			}
			namespaces[obj.Namespace] = ""

		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if pods, err = k8sApps.GetStatefulSetPods(obj); err != nil && err != schederrors.ErrPodsNotFound {
				return nil, &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get statefulset: %v. Err: %v", obj.Name, err),
				}
			}
			namespaces[obj.Namespace] = ""

		} else if obj, ok := specObj.(*corev1.Pod); ok {
			pod, err := k8sCore.GetPodByUID(obj.UID, obj.Namespace)
			if err != nil && err != schederrors.ErrPodsNotFound {
				return nil, &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get pod: %v. Err: %v", obj.Name, err),
				}
			}
			if pod != nil {
				pods = []corev1.Pod{*pod}
				namespaces[obj.Namespace] = ""
			}
		}
		for _, pod := range pods {
			allPods[pod.UID] = pod
		}
	}

	// Compare the two pod maps. Create a list of pod names which are not running
	// on the excluded nodes
	var podList []string
	for _, pod := range allPods {
		excludePod := false
		for _, excludedNode := range excludeList {
			if pod.Spec.NodeName == excludedNode.Name {
				excludePod = true
				break
			}
		}
		if !excludePod {
			podInfo := pod.Namespace + "/" + pod.Name + " (" + pod.Spec.NodeName + ")"
			podList = append(podList, podInfo)
		}
	}
	return podList, nil
}

// DeleteTasks delete the task
func (k *K8s) DeleteTasks(ctx *scheduler.Context, opts *scheduler.DeleteTasksOptions) error {
	fn := "DeleteTasks"
	deleteTasks := func() error {
		k8sOps := k8sCore
		pods, err := k.getPodsForApp(ctx)
		if err != nil {
			return &scheduler.ErrFailedToDeleteTasks{
				App:   ctx.App,
				Cause: fmt.Sprintf("failed to get pods due to: %v", err),
			}
		}
		if k.isRollingDeleteStrategyEnabled(ctx) {
			for _, pod := range pods {
				if err := k8sOps.DeletePod(pod.Name, pod.Namespace, false); err != nil {
					return &scheduler.ErrFailedToDeleteTasks{
						App:   ctx.App,
						Cause: fmt.Sprintf("failed to delete pods due to: %v", err),
					}
				}
				if err := k8sOps.WaitForPodDeletion(pod.UID, pod.Namespace, deleteTasksWaitTimeout); err != nil {
					log.Errorf("k8s %s failed to wait for pod: [%s] %s to terminate. err: %v", fn, pod.Namespace, pod.Name, err)
					return fmt.Errorf("k8s %s failed to wait for pod: [%s] %s to terminate. err: %v", fn, pod.Namespace, pod.Name, err)
				}

				if err := k.WaitForRunning(ctx, DefaultTimeout, DefaultRetryInterval); err != nil {
					return &scheduler.ErrFailedToValidatePod{
						App: ctx.App,
						Cause: fmt.Sprintf("Failed to validate Pod: [%s] %s. Err: Pod is not ready %v",
							pod.Namespace, pod.Name, pod.Status),
					}
				}
			}
		} else {
			if err := k8sOps.DeletePods(pods, false); err != nil {
				return &scheduler.ErrFailedToDeleteTasks{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to delete pods due to: %v", err),
				}
			}

			// Ensure the pods are deleted and removed from the system
			for _, pod := range pods {
				err = k8sOps.WaitForPodDeletion(pod.UID, pod.Namespace, deleteTasksWaitTimeout)
				if err != nil {
					log.Errorf("k8s %s failed to wait for pod: [%s] %s to terminate. err: %v", fn, pod.Namespace, pod.Name, err)
					return fmt.Errorf("k8s %s failed to wait for pod: [%s] %s to terminate. err: %v", fn, pod.Namespace, pod.Name, err)
				}
			}
		}

		return nil
	}

	if opts == nil {
		return deleteTasks()
	}

	return api.PerformTask(deleteTasks, &opts.TriggerOptions)
}

// GetVolumeDriverVolumeName returns name of volume which is refered by volume driver
func (k *K8s) GetVolumeDriverVolumeName(name string, namespace string) (string, error) {
	pvc, err := k8sCore.GetPersistentVolumeClaim(name, namespace)
	if err != nil {
		return "", fmt.Errorf("failed to get PVC: %v in namespace %v. Err: %v", name, namespace, err)
	}
	return pvc.Spec.VolumeName, nil
}

// GetVolumeParameters Get the volume parameters
func (k *K8s) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			params, err := k8sCore.GetPersistentVolumeClaimParams(obj)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get params for volume: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			pvc, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get PVC: %v,Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			for k, v := range pvc.Annotations {
				params[k] = v
			}
			params[PvcNameKey] = pvc.GetName()
			params[PvcNamespaceKey] = pvc.GetNamespace()

			result[pvc.Spec.VolumeName] = params
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			snap, err := k8sExternalStorage.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get Snapshot: %v,Namespace: %v. Err: %v", obj.Metadata.Name, obj.Metadata.Namespace, err),
				}
			}

			snapDataName := snap.Spec.SnapshotDataName
			if len(snapDataName) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App: ctx.App,
					Cause: fmt.Sprintf("snapshot: [%s] %s does not have snapshotdata set",
						snap.Metadata.Namespace, snap.Metadata.Name),
				}
			}

			snapData, err := k8sExternalStorage.GetSnapshotData(snapDataName)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get volumesnapshotdata: [%s] %s due to: %v", snapData.Metadata.Namespace, snapDataName, err),
				}
			}

			if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
				len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("volumesnapshotdata: [%s] %s does not have portworx volume source set", snapData.Metadata.Namespace, snapDataName),
				}
			}

			result[snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID] = map[string]string{
				SnapshotParent: snap.Spec.PersistentVolumeClaimName,
			}
		} else if obj, ok := specObj.(*pds.ModelsDeployment); ok {
			var labels map[string]string
			ss, err := k8sApps.GetStatefulSet(obj.GetClusterResourceName(), *obj.Namespace.Name)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v, Namespace : %v. Err: %v", obj.GetClusterResourceName(), *obj.Namespace.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v, Namespace: %v. Err: %v", ss.Name, ss.Namespace, err),
				}
			}

			if len(ctx.ScheduleOptions.TopologyLabels) > 0 {
				nodeAff := ss.Spec.Template.Spec.Affinity.NodeAffinity
				labels = getLabelsFromNodeAffinity(nodeAff)
			}

			for _, pvc := range pvcList.Items {
				params, err := k8sCore.GetPersistentVolumeClaimParams(&pvc)
				if err != nil {
					return nil, &scheduler.ErrFailedToGetVolumeParameters{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to get params for volume: %v, namespace: %v. Err: %v", pvc.Name, pvc.Namespace, err),
					}
				}

				for k, v := range pvc.Annotations {
					params[k] = v
				}
				params[PvcNameKey] = pvc.GetName()
				params[PvcNamespaceKey] = pvc.GetNamespace()

				if len(pvc.Spec.VolumeName) > 0 && len(ctx.ScheduleOptions.TopologyLabels) > 0 {
					for key, val := range labels {
						params[key] = val
						log.Infof("Topology labels for volume [%s] are: [%s]", pvc.Spec.VolumeName, params[key])
					}
				}
				result[pvc.Spec.VolumeName] = params

			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			var labels map[string]string
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v, Namespace : %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v, Namespace: %v. Err: %v", ss.Name, ss.Namespace, err),
				}
			}

			if len(ctx.ScheduleOptions.TopologyLabels) > 0 {
				nodeAff := ss.Spec.Template.Spec.Affinity.NodeAffinity
				labels = getLabelsFromNodeAffinity(nodeAff)
			}

			for _, pvc := range pvcList.Items {
				params, err := k8sCore.GetPersistentVolumeClaimParams(&pvc)
				if err != nil {
					return nil, &scheduler.ErrFailedToGetVolumeParameters{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to get params for volume: %v, namespace: %v. Err: %v", pvc.Name, pvc.Namespace, err),
					}
				}

				for k, v := range pvc.Annotations {
					params[k] = v
				}
				params[PvcNameKey] = pvc.GetName()
				params[PvcNamespaceKey] = pvc.GetNamespace()

				if len(pvc.Spec.VolumeName) > 0 && len(ctx.ScheduleOptions.TopologyLabels) > 0 {
					for key, val := range labels {
						params[key] = val
						log.Infof("Topology labels for volume [%s] are: [%s]", pvc.Spec.VolumeName, params[key])
					}
				}
				result[pvc.Spec.VolumeName] = params

			}
		}
	}
	return result, nil
}

// ValidateVolumes Validates the volumes
func (k *K8s) ValidateVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration,
	options *scheduler.VolumeOptions) error {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*storageapi.StorageClass); ok {
			if ctx.SkipClusterScopedObject {
				log.Infof("Skip storage class %s validation", obj.Name)
				continue
			}
			if _, err := k8sStorage.GetStorageClass(obj.Name); err != nil {
				if options != nil && options.SkipClusterScopedObjects {
					log.Warnf("[%v] Skipping validation of storage class: %v", ctx.App.Key, obj.Name)
				} else {
					return &scheduler.ErrFailedToValidateStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to validate StorageClass: %v. Err: %v", obj.Name, err),
					}
				}
			}
		} else if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			err := k8sCore.ValidatePersistentVolumeClaim(obj, timeout, retryInterval)
			if err != nil {
				if options != nil && options.ExpectError {
					// ignore
				} else {
					return &scheduler.ErrFailedToValidateStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to validate PVC: %v, Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
					}
				}
			}

			log.Infof("[%v] Validated PVC: %v, Namespace: %v", ctx.App.Key, obj.Name, obj.Namespace)

			autopilotEnabledOnPvc := false
			if pvcAnnotationValue, ok := obj.Annotations[autopilotEnabledAnnotationKey]; ok {
				autopilotEnabledOnPvc, err = strconv.ParseBool(pvcAnnotationValue)
				if err != nil {
					return err
				}
			}
			if autopilotEnabledOnPvc {
				isAutopilotPodAvailable, err := k.IsAutopilotEnabled()
				if err != nil {
					return err
				} else if !isAutopilotPodAvailable {
					return fmt.Errorf("Autopilot is not enabled")
				}
				listApRules, err := k8sAutopilot.ListAutopilotRules()
				if err != nil {
					return err
				}
				for _, rule := range listApRules.Items {
					for _, a := range rule.Spec.Actions {
						if a.Name == aututils.VolumeSpecAction && isAutopilotMatchPvcLabels(rule, obj) {
							err := k.validatePVCSize(ctx, obj, rule, 5*timeout, retryInterval)
							if err != nil {
								return err
							}
						}
					}
				}
				log.Infof("[%v] Validated PVC: %v size based on Autopilot rules", ctx.App.Key, obj.Name)
			} else {
				log.Warnf("[%v] Autopilot is not enabled, PVC: %v size validation is not possible", ctx.App.Key, obj.Name)
			}
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout,
				retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate snapshot: %v, Namespace: %v. Err: %v", obj.Metadata.Name, obj.Metadata.Namespace, err),
				}
			}

			log.Infof("[%v] Validated snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			if err := k8sStork.ValidateGroupSnapshot(obj.Name, obj.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate group snapshot: %v, Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Infof("[%v] Validated group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*pds.ModelsDeployment); ok {
			ss, err := k8sApps.GetStatefulSet(obj.GetClusterResourceName(), *obj.Namespace.Name)
			if err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v , Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}
			// Providing the scaling factor in timeout
			scalingFactor := *ss.Spec.Replicas
			if *ss.Spec.Replicas > *obj.NodeCount {
				scalingFactor = int32(*ss.Spec.Replicas - *ss.Spec.Replicas)
			}
			if err := k8sApps.ValidatePVCsForStatefulSet(ss, timeout*time.Duration(scalingFactor), retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVCs for statefulset: %v,Namespace: %v. Err: %v", ss.Name, ss.Namespace, err),
				}
			}
			log.Infof("[%v] Validated PVCs from StatefulSet: %v", ctx.App.Key, obj.GetClusterResourceName())
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v, Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}
			// Providing the scaling factor in timeout
			scalingFactor := *obj.Spec.Replicas
			if *ss.Spec.Replicas > *obj.Spec.Replicas {
				scalingFactor = int32(*ss.Spec.Replicas - *obj.Spec.Replicas)
			}
			if err := k8sApps.ValidatePVCsForStatefulSet(ss, timeout*time.Duration(scalingFactor), retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVCs for statefulset: %v,Namespace: %v. Err: %v", ss.Name, ss.Namespace, err),
				}
			}
			log.Infof("[%v] Validated PVCs from StatefulSet: %v", ctx.App.Key, obj.Name)
		}
	}
	return nil
}

// GetSnapShotData returns the snapshotdata
func (k *K8s) GetSnapShotData(ctx *scheduler.Context, snapshotName, snapshotNameSpace string) (*snapv1.VolumeSnapshotData, error) {

	snap, err := k8sExternalStorage.GetSnapshot(snapshotName, snapshotNameSpace)
	if err != nil {
		return nil, &scheduler.ErrFailedToGetSnapShot{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to get Snapshot: %v. Err: %v", snapshotName, err),
		}
	}

	snapDataName := snap.Spec.SnapshotDataName
	log.Infof("Got SnapData Name: %v", snapDataName)
	if len(snapDataName) == 0 {
		return nil, &scheduler.ErrFailedToGetSnapShotDataName{
			App: ctx.App,
			Cause: fmt.Sprintf("snapshot: [%s] %s does not have snapshotdata set",
				snap.Metadata.Namespace, snap.Metadata.Name),
		}
	}

	snapData, err := k8sExternalStorage.GetSnapshotData(snapDataName)
	if err != nil {
		return nil, &scheduler.ErrFailedToGetSnapShotData{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to get volumesnapshotdata: %s due to: %v", snapDataName, err),
		}
	}

	return snapData, nil
}

// GetWorkloadSizeFromAppSpec gets workload size from an application spec
func (k *K8s) GetWorkloadSizeFromAppSpec(context *scheduler.Context) (uint64, error) {
	var err error
	var wSize uint64
	appEnvVar := getSpecAppEnvVar(context, specObjAppWorkloadSizeEnvVar)
	if appEnvVar != "" {
		wSize, err = strconv.ParseUint(appEnvVar, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("can't parse value %v of environment variable. Err: %v", appEnvVar, err)
		}

		// if size less than 1024 we assume that value is in Gb
		if wSize < 1024 {
			return wSize * units.GiB, nil
		}
	}
	return 0, nil
}

func getSpecAppEnvVar(ctx *scheduler.Context, key string) string {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			for _, container := range obj.Spec.Template.Spec.Containers {
				for _, env := range container.Env {
					if env.Name == key {
						return env.Value
					}
				}
			}
		}
	}
	return ""
}

func (k *K8s) validatePVCSize(ctx *scheduler.Context, obj *corev1.PersistentVolumeClaim, rule apapi.AutopilotRule, timeout time.Duration, retryInterval time.Duration) error {
	wSize, err := k.GetWorkloadSizeFromAppSpec(ctx)
	if err != nil {
		return err
	}
	expectedPVCSize, _, err := k.EstimatePVCExpansion(obj, rule, wSize)
	if err != nil {
		return err
	}
	log.Infof("[%v] expecting PVC size: %v\n", ctx.App.Key, expectedPVCSize)
	err = k8sCore.ValidatePersistentVolumeClaimSize(obj, int64(expectedPVCSize), timeout, retryInterval)
	if err != nil {
		return &scheduler.ErrFailedToValidateStorage{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to validate size: %v of PVC: %v, Namespace: %v. Err: %v", expectedPVCSize, obj.Name, obj.Namespace, err),
		}
	}
	return nil
}

func (k *K8s) isPVCShared(pvc *corev1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == corev1.ReadOnlyMany || mode == corev1.ReadWriteMany {
			return true
		}
	}
	return false
}

// DeleteVolumes  delete the volumes
func (k *K8s) DeleteVolumes(ctx *scheduler.Context, options *scheduler.VolumeOptions) ([]*volume.Volume, error) {
	var vols []*volume.Volume

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*storageapi.StorageClass); ok {
			if options != nil && !options.SkipClusterScopedObjects {
				if err := k8sStorage.DeleteStorageClass(obj.Name); err != nil {
					if k8serrors.IsNotFound(err) {
						log.Infof("[%v] Storage class is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
						continue
					}
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy storage class: %v. Err: %v", obj.Name, err),
					}
				}

				log.Infof("[%v] Destroyed storage class: %v", ctx.App.Key, obj.Name)
			}
		} else if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvcObj, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] PVC is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("[%s] Failed to get PVC: %v. Err: %v", ctx.App.Key, obj.Name, err),
				}
			}

			vols = append(vols, &volume.Volume{
				ID:        string(pvcObj.Spec.VolumeName),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			})

			if err := k8sCore.DeletePersistentVolumeClaim(obj.Name, obj.Namespace); err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] PVC is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("[%s] Failed to destroy PVC: %v. Err: %v", ctx.App.Key, obj.Name, err),
				}
			}

			log.Infof("[%v] Destroyed PVC: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.DeleteSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] Snapshot is not found: %v, skipping deletion", ctx.App.Key, obj.Metadata.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			log.Infof("[%v] Destroyed Snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			if err := k8sStork.DeleteGroupSnapshot(obj.Name, obj.Namespace); err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] Group snapshot is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy group snapshot: %v. Err: %v", obj.Name, err),
				}
			}

			log.Infof("[%v] Destroyed group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			pvcList, err := k8sApps.GetPVCsForStatefulSet(obj)
			if err != nil || pvcList == nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] PVCs for StatefulSet not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				pvcObj, err := k8sCore.GetPersistentVolumeClaim(pvc.Name, pvc.Namespace)
				if err != nil {
					if k8serrors.IsNotFound(err) {
						log.Infof("[%v] PVC is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
						continue
					}
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to get PVC: %v. Err: %v", pvc.Name, err),
					}
				}
				vols = append(vols, &volume.Volume{
					ID:        string(pvcObj.Spec.VolumeName),
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})

				if err := k8sCore.DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace); err != nil {
					if k8serrors.IsNotFound(err) {
						log.Infof("[%v] PVC is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
						continue
					}
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", pvc.Name, err),
					}
				}
			}

			log.Infof("[%v] Destroyed PVCs for StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*corev1.PersistentVolume); ok {
			pvcObj, err := k8sCore.GetPersistentVolume(obj.Name)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] PV is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("[%s] Failed to get PV: %v. Err: %v", ctx.App.Key, obj.Name, err),
				}
			}

			if err := k8sCore.DeletePersistentVolume(pvcObj.Name); err != nil {
				if k8serrors.IsNotFound(err) {
					log.Infof("[%v] PV is not found: %v, skipping deletion", ctx.App.Key, obj.Name)
					continue
				}
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("[%s] Failed to destroy PV: %v. Err: %v", ctx.App.Key, obj.Name, err),
				}
			}

			log.Infof("[%v] Destroyed PV: %v", ctx.App.Key, obj.Name)
		}
	}

	return vols, nil
}

func (k *K8s) appendVolForPVC(vols []*volume.Volume, pvc *v1.PersistentVolumeClaim) ([]*volume.Volume, error) {
	shouldAdd, err := k.filterPureVolumesIfEnabled(pvc)
	if err != nil {
		return nil, err
	}
	if !shouldAdd {
		return vols, nil
	}

	labels := pvc.Labels

	if pvc.Spec.VolumeName != "" {
		log.Debugf("pvc spec volume name [%s]", pvc.Spec.VolumeName)
		// If this is a Pure volume, run some extra checks to get more information.
		// Store them as labels as they are not applicable to all volume types.
		driver, err := volume.Get(k.VolDriverName)
		if err != nil {
			log.Errorf("error getting volume driver name")
			return nil, err
		}

		inspectedVol, err := driver.InspectVolume(pvc.Spec.VolumeName)
		if err != nil {
			log.Warnf("error inspecting volume [%s],err: %v", pvc.Spec.VolumeName, err)
			if _, ok := err.(*errors.ErrNotSupported); !ok {
				return nil, fmt.Errorf("failed to inspect volume %s: %v", pvc.Name, err)
			}
			// If the driver doesn't support InspectVolume, then it's definitely not a Pure volume. Do nothing special
		} else {
			log.Debugf("inspected volume [%s] to add labels", inspectedVol.Id)
			// If this is a Pure volume, run some extra checks to get more information.
			// Store them as labels as they are not applicable to all volume types.
			if labels == nil {
				labels = make(map[string]string)
			}
			if inspectedVol.Spec.IsPureVolume() {
				switch inspectedVol.Spec.ProxySpec.ProxyProtocol {
				case osapi.ProxyProtocol_PROXY_PROTOCOL_PURE_BLOCK:
					labels[PureDAVolumeLabel] = PureDAVolumeLabelValueFA
					labels[FADAVolumeSerialLabel] = inspectedVol.Spec.ProxySpec.PureBlockSpec.SerialNum
				case osapi.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE:
					labels[PureDAVolumeLabel] = PureDAVolumeLabelValueFB
				default:
					return nil, fmt.Errorf("unknown proxy type %v for Pure volume", inspectedVol.Spec.ProxySpec.ProxyProtocol)
				}
			}
		}
	}

	pvcSizeObj := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	pvcSize, _ := pvcSizeObj.AsInt64()
	isRaw := pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == corev1.PersistentVolumeBlock
	vol := &volume.Volume{
		ID:          string(pvc.Spec.VolumeName),
		Name:        pvc.Name,
		Namespace:   pvc.Namespace,
		Shared:      k.isPVCShared(pvc),
		Annotations: pvc.Annotations,
		Labels:      labels,
		Size:        uint64(pvcSize),
		Raw:         isRaw,
	}
	log.Debugf("Adding vol [%s/%s] to volume list", vol.ID, vol.Name)
	return append(vols, vol), nil
}

// GetVolumes  Get the volumes
func (k *K8s) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8sApps
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			if value, exists := obj.Labels["portworx.io/kubevirt"]; exists && value == "true" {
				log.Debugf("PVC [%s] in namespace: [%s] is labeled for kubevirt, skipping...", obj.Name, obj.Namespace)
				continue
			}
			if value, exists := obj.Labels["app"]; exists && value == "containerized-data-importer" {
				log.Debugf("PVC [%s] in namespace: [%s] is a data vol template, skipping...", obj.Name, obj.Namespace)
				continue
			}
			log.Debugf("Getting PVC [%s], namespace: [%s] for depolyment [%s]", obj.Name, obj.Namespace, ctx.App.Key)
			pvcObj, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			log.Debugf("got pvc object [%s]", pvcObj.Name)
			if err != nil {
				return nil, fmt.Errorf("error getting pvc: %s, namespace: %s. Err: %v", obj.Name, obj.Namespace, err)
			}
			vols, err = k.appendVolForPVC(vols, pvcObj)
			if err != nil {
				return nil, err
			}
		} else if pdsobj, ok := specObj.(*pds.ModelsDeployment); ok {
			ss, err := k8sApps.GetStatefulSet(*pdsobj.ClusterResourceName, *pdsobj.Namespace.Name)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v , Namespace: %v. Err: %v", pdsobj.GetClusterResourceName(), *pdsobj.Namespace.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v, Namespace: %s. Err: %v", ss.Name, ss.Namespace, err),
				}
			}
			for _, pvc := range pvcList.Items {
				vols, err = k.appendVolForPVC(vols, &pvc)
				if err != nil {
					return nil, err
				}
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v , Namespace: %v. Err: %v", obj.Name, obj.Namespace, err),
				}
			}

			log.Debugf("Getting PVCs from namespace: [%s] for statefulset [%s]", obj.Namespace, obj.Name)

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v, Namespace: %s. Err: %v", ss.Name, ss.Namespace, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols, err = k.appendVolForPVC(vols, &pvc)
				if err != nil {
					return nil, err
				}
			}
		} else if vm, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
			pvcList, err := k8sCore.GetPersistentVolumeClaims(vm.Namespace, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to get PVCs in namespace %s: %w", vm.Namespace, err)
			}
			for _, pvc := range pvcList.Items {
				// Filter out the volumes with annotations stork.libopenstorage.org/skip-resource: true from the list
				// This is needed in case of NFS backup-location where backup creates a volume for mounting the NFS backup location
				if pvc.Annotations != nil {
					if _, ok := pvc.Annotations["stork.libopenstorage.org/skip-resource"]; ok {
						continue
					}
				}
				// check if the pvc has our VM as the owner
				want := false
				for _, ownerRef := range pvc.OwnerReferences {
					if ownerRef.Kind == vm.Kind {
						if ownerRef.Name == vm.Name {
							want = true
						}
					}
					// If volume is still not found, try the datavolume for the virtual machine
					if !want && ownerRef.Kind == "DataVolume" {
						kvClient := k8sKubevirt.GetKubevirtClient()
						dataVol, err := kvClient.CdiClient().CdiV1beta1().DataVolumes(vm.Namespace).Get(context.TODO(), ownerRef.Name, metav1.GetOptions{})
						if err != nil && !k8serrors.IsNotFound(err) {
							return nil, fmt.Errorf("error getting datavolume for vm: %s in namespace: %s. error: %v", vm.Name, vm.Namespace, err)
						} else {
							if dataVol != nil {
								for _, ownerRef := range dataVol.OwnerReferences {
									if ownerRef.Name == vm.Name {
										want = true
									}
								}
							}
						}
					}
				}
				// If volume is still not found, try the virt-launcher pod as well
				if !want {
					virtLauncherPodsList, err := k8sCore.GetPods(vm.Namespace, nil)
					if err != nil {
						return nil, fmt.Errorf("failed to get pods in namespace: %s: %w", vm.Namespace, err)
					}
					for _, virtLauncher := range virtLauncherPodsList.Items {
						for _, ownerRef := range virtLauncher.OwnerReferences {
							if ownerRef.Kind == "VirtualMachineInstance" && ownerRef.Name == vm.Name {
								// since VMI name is same as VM name
								want = true
							}
						}
					}
				}
				if !want {
					continue
				}
				vols, err = k.appendVolForPVC(vols, &pvc)
				if err != nil {
					return nil, err
				}
			}
		} else if pipeline, ok := specObj.(*tektoncdv1.PipelineRun); ok {
			pvcList, err := k8sCore.GetPersistentVolumeClaims(pipeline.Namespace, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to get PVCs in namespace %s: %w", pipeline.Namespace, err)
			}
			for _, pvc := range pvcList.Items {
				// check if the pvc has our VM as the owner
				want := false
				for _, ownerRef := range pvc.OwnerReferences {
					if ownerRef.Kind == pipeline.Kind && ownerRef.Name == pipeline.Name {
						want = true
					}
				}
				if !want {
					continue
				}
				vols, err = k.appendVolForPVC(vols, &pvc)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	for _, vol := range vols {
		log.Infof("K8s.GetVolumes() found volume %s for app %s", vol.Name, ctx.App.Key)
	}
	return vols, nil
}

// GetPureVolumes  Get the Pure volumes (if enabled) by type (PureFile or PureBlock)
func (k *K8s) GetPureVolumes(ctx *scheduler.Context, pureVolType string) ([]*volume.Volume, error) {
	k8sOps := k8sApps
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvcObj, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}

			shouldAdd, err := k.filterPureVolumesIfEnabledByPureVolBackend(pvcObj, pureVolType)
			if err != nil {
				return nil, err
			}
			if !shouldAdd {
				continue
			}

			pvcSizeObj := pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]
			pvcSize, _ := pvcSizeObj.AsInt64()
			isRaw := *pvcObj.Spec.VolumeMode == corev1.PersistentVolumeBlock
			vol := &volume.Volume{
				ID:          string(pvcObj.Spec.VolumeName),
				Name:        obj.Name,
				Namespace:   obj.Namespace,
				Shared:      k.isPVCShared(obj),
				Annotations: make(map[string]string),
				Labels:      pvcObj.Labels,
				Size:        uint64(pvcSize),
				Raw:         isRaw,
			}
			for key, val := range obj.Annotations {
				vol.Annotations[key] = val
			}
			vols = append(vols, vol)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols = append(vols, &volume.Volume{
					ID:        pvc.Spec.VolumeName,
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})
			}
		}
	}

	return vols, nil
}

// ResizeVolume  Resize the volume
func (k *K8s) ResizeVolume(ctx *scheduler.Context, configMapName string) ([]*volume.Volume, error) {
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		// Add security annotations if running with auth-enabled
		if configMapName != "" {
			configMap, err := k8sCore.GetConfigMap(configMapName, "default")
			if err != nil {
				return nil, &scheduler.ErrFailedToGetConfigMap{
					Name:  configMapName,
					Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
				}
			}

			err = k.addSecurityAnnotation(specObj, configMap, ctx.App)
			if err != nil {
				return nil, fmt.Errorf("failed to add annotations to storage object: %v", err)
			}

		}
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			updatedPVC, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			shouldResize, err := k.filterPureVolumesIfEnabled(updatedPVC)
			if err != nil {
				return nil, err
			}
			if shouldResize {
				vol, err := k.ResizePVC(ctx, updatedPVC, 1)
				if err != nil {
					return nil, err
				}
				vols = append(vols, vol)
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				shouldResize, err := k.filterPureVolumesIfEnabled(&pvc)
				if err != nil {
					return nil, err
				}
				if shouldResize {
					vol, err := k.ResizePVC(ctx, &pvc, 1)
					if err != nil {
						return nil, err
					}
					vols = append(vols, vol)
				}
			}
		} else if obj, ok := specObj.(*pds.ModelsDeployment); ok {
			pdsNs := *obj.Namespace.Name
			ss, err := k8sApps.GetStatefulSet(obj.GetClusterResourceName(), pdsNs)
			if err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v , Namespace: %v. Err: %v", obj.GetClusterResourceName(), pdsNs, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}
			for _, pvc := range pvcList.Items {
				shouldResize, err := k.filterPureVolumesIfEnabled(&pvc)
				if err != nil {
					return nil, err
				}
				if shouldResize {
					vol, err := k.ResizePVC(ctx, &pvc, 1)
					if err != nil {
						return nil, err
					}
					vols = append(vols, vol)
				}
			}
		}
	}

	return vols, nil
}
func (k *K8s) ResizePVC(ctx *scheduler.Context, pvc *corev1.PersistentVolumeClaim, sizeInGb uint64) (*volume.Volume, error) {
	var vol *volume.Volume

	shouldResize, err := k.filterPureVolumesIfEnabled(pvc)
	if err != nil {
		return nil, err
	}
	if shouldResize {
		k8sOps := k8sCore
		storageSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

		// TODO this test is required since stork snapshot doesn't support resizing, remove when feature is added
		resizeSupported := true
		if annotationValue, hasKey := pvc.Annotations[resizeSupportedAnnotationKey]; hasKey {
			resizeSupported, _ = strconv.ParseBool(annotationValue)
		}
		if resizeSupported {
			extraAmount, _ := resource.ParseQuantity(fmt.Sprintf("%dGi", sizeInGb))
			storageSize.Add(extraAmount)
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = storageSize
			if _, err := k8sOps.UpdatePersistentVolumeClaim(pvc); err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: err.Error(),
				}
			}
		}
		sizeInt64, _ := storageSize.AsInt64()
		vol = &volume.Volume{
			ID:            string(pvc.Spec.VolumeName),
			Name:          pvc.Name,
			Namespace:     pvc.Namespace,
			RequestedSize: uint64(sizeInt64),
			Shared:        k.isPVCShared(pvc),
		}
	}
	return vol, nil

}

// GetSnapshots  Get the snapshots
func (k *K8s) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	var snaps []*volume.Snapshot
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			snap := &volume.Snapshot{
				ID:        string(obj.Metadata.UID),
				Name:      obj.Metadata.Name,
				Namespace: obj.Metadata.Namespace,
			}
			snaps = append(snaps, snap)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			snapsForGroupsnap, err := k8sStork.GetSnapshotsForGroupSnapshot(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}

			for _, snapForGroupsnap := range snapsForGroupsnap {
				snap := &volume.Snapshot{
					ID:        string(snapForGroupsnap.Metadata.UID),
					Name:      snapForGroupsnap.Metadata.Name,
					Namespace: snapForGroupsnap.Metadata.Namespace,
				}
				snaps = append(snaps, snap)
			}
		}
	}

	return snaps, nil
}

// DeleteSnapShot delete the snapshots
func (k *K8s) DeleteSnapShot(ctx *scheduler.Context, snapshotName, snapshotNameSpace string) error {

	if err := k8sExternalStorage.DeleteSnapshot(snapshotName, snapshotNameSpace); err != nil {
		if k8serrors.IsNotFound(err) {
			log.Infof("[%v] Snapshot is not found: %v, skipping deletion", ctx.App.Key, snapshotName)

		}
		return &scheduler.ErrFailedToDestroyStorage{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to destroy snapshot: %v. Err: %v", snapshotName, err),
		}
	}

	log.Infof("[%v] Destroyed Snapshot: %v", ctx.App.Key, snapshotName)

	return nil

}

// DeleteCsiSnapshot delete the snapshots
func (k *K8s) DeleteCsiSnapshot(ctx *scheduler.Context, snapshotName, snapshotNameSpace string) error {

	if err := k8sExternalsnap.DeleteSnapshot(snapshotName, snapshotNameSpace); err != nil {
		if k8serrors.IsNotFound(err) {
			log.Infof("[%v] Csi Snapshot not found: %v, skipping deletion", ctx.App.Key, snapshotName)

		}
		return &scheduler.ErrFailedToDestroyStorage{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to delete snapshot: %v. Err: %v", snapshotName, err),
		}
	}

	log.Infof("[%v] Deleted Snapshot: %v", ctx.App.Key, snapshotName)

	return nil

}

// GetSnapshotsInNameSpace get the snapshots list for the namespace
func (k *K8s) GetSnapshotsInNameSpace(ctx *scheduler.Context, snapshotNameSpace string) (*volsnapv1.VolumeSnapshotList, error) {

	time.Sleep(10 * time.Second)
	snapshotList, err := k8sExternalsnap.ListSnapshots(snapshotNameSpace)
	if err != nil {
		log.Infof("Snapshotsnot for app [%v] not found in namespace: %v", ctx.App.Key, snapshotNameSpace)
		return nil, err
	}

	return snapshotList, nil
}

// IsCsiSnapshotExists checks if snapshot exists in namespace
func (k *K8s) IsCsiSnapshotExists(ctx *scheduler.Context, snapshotName string, namespace string) (bool, error) {
	snaplist, err := k.GetSnapshotsInNameSpace(ctx, namespace)
	if err != nil {
		log.InfoD("Failed to get Snapshots for the app [%v] in namespace [%v]", ctx.App.Key, namespace)
		return false, err
	}
	if len(snaplist.Items) == 0 {
		log.InfoD("No Snapshots found ")
		return false, nil
	}
	for _, snap := range snaplist.Items {
		if snap.ObjectMeta.Name == snapshotName {
			log.InfoD("Snapshot [%v] exists in namespace [%v]", snapshotName, namespace)
			return true, nil
		}
	}
	return false, nil

}

// DeleteCsiSnapshotsFromNamespace delete the snapshots from the namespace
func (k *K8s) DeleteCsiSnapshotsFromNamespace(ctx *scheduler.Context, namespace string) error {
	snaplist, err := k.GetSnapshotsInNameSpace(ctx, namespace)
	if err != nil {
		log.InfoD("Failed to get Snapshots for app [%v] in namespace [%v]", ctx.App.Key, namespace)
		return err
	}
	for _, snap := range snaplist.Items {
		err = k.DeleteCsiSnapshot(ctx, snap.ObjectMeta.Name, namespace)
		if err != nil {
			log.InfoD("Failed to delete snapshot [%v] in namespace [%v]", snap.ObjectMeta.Name, namespace)
			return err
		}
	}
	return nil

}

// GetNodesForApp get the node for the app
func (k *K8s) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
	t := func() (interface{}, bool, error) {
		pods, err := k.getPodsForApp(ctx)
		if err != nil {
			return nil, false, &scheduler.ErrFailedToGetNodesForApp{
				App:   ctx.App,
				Cause: fmt.Sprintf("failed to get pods due to: %v", err),
			}
		}

		// We should have pods from a supported application at this point
		var result []node.Node
		nodeMap := node.GetNodesByName()

		for _, p := range pods {
			if strings.TrimSpace(p.Spec.NodeName) == "" {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("pod %s is not scheduled to any node yet", p.Name),
				}
			}
			n, ok := nodeMap[p.Spec.NodeName]
			if !ok {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("node: %v not present in node map", p.Spec.NodeName),
				}
			}

			if node.Contains(result, n) {
				continue
			}

			if k8sCommon.IsPodRunning(p) {
				result = append(result, n)
			}
		}

		if len(result) > 0 {
			return result, false, nil
		}

		return result, true, &scheduler.ErrFailedToGetNodesForApp{
			App:   ctx.App,
			Cause: fmt.Sprintf("no pods in running state %v", pods),
		}
	}

	nodes, err := task.DoRetryWithTimeout(t, DefaultTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nodes.([]node.Node), nil
}

func (k *K8s) getPodsForApp(ctx *scheduler.Context) ([]corev1.Pod, error) {
	k8sOps := k8sApps
	var pods []corev1.Pod

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			depPods, err := k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ssPods, err := k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}

// GetPodsForPVC returns pods for give pvc and namespace
func (k *K8s) GetPodsForPVC(pvcname, namespace string) ([]corev1.Pod, error) {
	return k8sCore.GetPodsUsingPVC(pvcname, namespace)
}

// GetPodLog returns logs for all the pods in the specified context
func (k *K8s) GetPodLog(ctx *scheduler.Context, sinceSeconds int64, containerName string) (map[string]string, error) {
	var sinceSecondsArg *int64
	if sinceSeconds > 0 {
		sinceSecondsArg = &sinceSeconds
	}
	pods, err := k.getPodsForApp(ctx)
	if err != nil {
		return nil, err
	}
	logsByPodName := map[string]string{}
	for _, pod := range pods {
		output, err := k8sCore.GetPodLog(pod.Name, pod.Namespace, &v1.PodLogOptions{SinceSeconds: sinceSecondsArg, Container: containerName})
		if err != nil {
			return nil, fmt.Errorf("failed to get logs for the pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		logsByPodName[pod.Name] = output
	}
	return logsByPodName, nil
}

// Describe describe the test case
func (k *K8s) Describe(ctx *scheduler.Context) (string, error) {
	var buf bytes.Buffer
	var err error
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Deployment: [%s] %s", obj.Namespace, obj.Name)))
			var depStatus *appsapi.DeploymentStatus
			if depStatus, err = k8sApps.DescribeDeployment(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of deployment: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump depStatus
			depStatusString := "nil"
			if depStatus != nil {
				depStatusString = fmt.Sprintf("%+v", *depStatus)
			}
			buf.WriteString(fmt.Sprintf("Status: %s\n", depStatusString))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Deployment", obj.Name)))
			pods, _ := k8sApps.GetDeploymentPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END Deployment"))
		} else if obj, ok := specObj.(*pds.ModelsDeployment); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("StatefulSet: [%s] ", obj.GetClusterResourceName())))
			var ssetStatus *appsapi.StatefulSetStatus
			if ssetStatus, err = k8sApps.DescribeStatefulSet(obj.GetClusterResourceName(), *obj.Namespace.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of statefulset: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump ssetStatus
			ssetStatusString := "nil"
			if ssetStatus != nil {
				ssetStatusString = fmt.Sprintf("%+v", *ssetStatus)
			}
			buf.WriteString(fmt.Sprintf("Status: %s\n", ssetStatusString))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(*obj.Namespace.Name, "StatefulSet", obj.GetClusterResourceName())))
			ss, err := k8sApps.GetStatefulSet(obj.GetClusterResourceName(), *obj.Namespace.Name)
			if err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetPodStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get statefulsets: %v. Err: %v", obj.GetClusterResourceName(), err),
				}))
			}
			pods, _ := k8sApps.GetStatefulSetPods(ss)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END StatefulSet"))
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("StatefulSet: [%s] %s", obj.Namespace, obj.Name)))
			var ssetStatus *appsapi.StatefulSetStatus
			if ssetStatus, err = k8sApps.DescribeStatefulSet(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of statefulset: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump ssetStatus
			ssetStatusString := "nil"
			if ssetStatus != nil {
				ssetStatusString = fmt.Sprintf("%+v", *ssetStatus)
			}
			buf.WriteString(fmt.Sprintf("Status: %s\n", ssetStatusString))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "StatefulSet", obj.Name)))
			pods, _ := k8sApps.GetStatefulSetPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END StatefulSet"))
		} else if obj, ok := specObj.(*corev1.Service); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Service: [%s] %s", obj.Namespace, obj.Name)))
			var svcStatus *corev1.ServiceStatus
			if svcStatus, err = k8sCore.DescribeService(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of service: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump service status
			svcStatusString := "nil"
			if svcStatus != nil {
				svcStatusString = fmt.Sprintf("%+v", *svcStatus)
			}
			buf.WriteString(fmt.Sprintf("Status: %s\n", svcStatusString))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Service", obj.Name)))
			buf.WriteString(insertLineBreak("END Service"))
		} else if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("PersistentVolumeClaim: [%s] %s", obj.Namespace, obj.Name)))
			var pvcStatus *corev1.PersistentVolumeClaimStatus
			if pvcStatus, err = k8sCore.GetPersistentVolumeClaimStatus(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetStorageStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of persistent volume claim: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump persistent volume claim status
			pvcStatusString := "nil"
			if pvcStatus != nil {
				pvcStatusString = fmt.Sprintf("%+v", *pvcStatus)
			}
			buf.WriteString(fmt.Sprintf("Status: %s\n", pvcStatusString))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "PersistentVolumeClaim", obj.Name)))
			buf.WriteString(insertLineBreak("END PersistentVolumeClaim"))
		} else if obj, ok := specObj.(*storageapi.StorageClass); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("StorageClass: %s", obj.Name)))
			var scParams map[string]string
			if scParams, err = k8sStorage.GetStorageClassParams(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get parameters of storage class: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump storage class parameters
			buf.WriteString(fmt.Sprintf("%+v\n", scParams))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "StorageClass", obj.Name)))
			buf.WriteString(insertLineBreak("END StorageClass"))
		} else if obj, ok := specObj.(*corev1.Pod); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Pod: [%s] %s", obj.Namespace, obj.Name)))
			var podStatus *corev1.PodList
			if podStatus, err = k8sCore.GetPods(obj.Name, nil); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetPodStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of pod: %v. Err: %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", podStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Pod", obj.Name)))
			buf.WriteString(insertLineBreak("END Pod"))
		} else if obj, ok := specObj.(*storkapi.ClusterPair); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ClusterPair: [%s] %s", obj.Namespace, obj.Name)))
			var clusterPair *storkapi.ClusterPair
			if clusterPair, err = k8sStork.GetClusterPair(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", clusterPair))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ClusterPair", obj.Name)))
			buf.WriteString(insertLineBreak("END ClusterPair"))
		} else if obj, ok := specObj.(*storkapi.Migration); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Migration: [%s] %s", obj.Namespace, obj.Name)))
			var migration *storkapi.Migration
			if migration, err = k8sStork.GetMigration(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", migration))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Migration", obj.Name)))
			buf.WriteString(insertLineBreak("END Migration"))
		} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("MigrationSchedule: [%s] %s", obj.Namespace, obj.Name)))
			var migrationSchedule *storkapi.MigrationSchedule
			if migrationSchedule, err = k8sStork.GetMigrationSchedule(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", migrationSchedule))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "MigrationSchedule", obj.Name)))
			buf.WriteString(insertLineBreak("END MigrationSchedule"))
		} else if obj, ok := specObj.(*storkapi.BackupLocation); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("BackupLocation: [%s] %s", obj.Namespace, obj.Name)))
			var backupLocation *storkapi.BackupLocation
			if backupLocation, err = k8sStork.GetBackupLocation(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get BackupLocation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", backupLocation))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "BackupLocation", obj.Name)))
			buf.WriteString(insertLineBreak("END BackupLocation"))
		} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationBackup: [%s] %s", obj.Namespace, obj.Name)))
			var applicationBackup *storkapi.ApplicationBackup
			if applicationBackup, err = k8sStork.GetApplicationBackup(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationBackup: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationBackup))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationBackup", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationBackup"))
		} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationRestore: [%s] %s", obj.Namespace, obj.Name)))
			var applicationRestore *storkapi.ApplicationRestore
			if applicationRestore, err = k8sStork.GetApplicationRestore(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationRestore))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationRestore", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationRestore"))
		} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationClone: [%s] %s", obj.Namespace, obj.Name)))
			var applicationClone *storkapi.ApplicationClone
			if applicationClone, err = k8sStork.GetApplicationClone(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationClone: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationClone))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationClone", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationClone"))
		} else if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("VolumeSnapshotRestore: [%s] %s", obj.Namespace, obj.Name)))
			var volumeSnapshotRestore *storkapi.VolumeSnapshotRestore
			if volumeSnapshotRestore, err = k8sStork.GetVolumeSnapshotRestore(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", volumeSnapshotRestore))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "VolumeSnapshotRestore", obj.Name)))
			buf.WriteString(insertLineBreak("END VolumeSnapshotRestore"))
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("VolumeSnapshot: [%s] %s", obj.Metadata.Namespace, obj.Metadata.Name)))
			var volumeSnapshotStatus *snapv1.VolumeSnapshotStatus
			if volumeSnapshotStatus, err = k8sExternalStorage.GetSnapshotStatus(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Metadata.Name,
					Cause: fmt.Sprintf("Failed to get VolumeSnapshot: %v. Err: %v", obj.Metadata.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", volumeSnapshotStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Metadata.Name, "VolumeSnapshot", obj.Metadata.Name)))
			buf.WriteString(insertLineBreak("END VolumeSnapshot"))
		} else if obj, ok := specObj.(*apapi.AutopilotRule); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("AutopilotRule: [%s] %s", obj.Namespace, obj.Name)))
			var autopilotRule *apapi.AutopilotRule
			if autopilotRule, err = k8sAutopilot.GetAutopilotRule(obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get AutopilotRule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%v\n", autopilotRule))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "AutopilotRule", obj.Name)))
			buf.WriteString(insertLineBreak("END AutopilotRule"))
		} else if obj, ok := specObj.(*corev1.Secret); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Secret: [%s] %s", obj.Namespace, obj.Name)))
			var secret *corev1.Secret
			if secret, err = k8sCore.GetSecret(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetSecret{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get secret : %v. Error is : %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", secret))
			buf.WriteString(insertLineBreak("END Secret"))
		} else if obj, ok := specObj.(*kubevirtv1.VirtualMachine); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("VirtualMachine: [%s] %s", obj.Namespace, obj.Name)))
			var virtualMachine *kubevirtv1.VirtualMachine
			if virtualMachine, err = k8sKubevirt.GetVirtualMachine(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get VirtualMachine: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", virtualMachine))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "VirtualMachine", obj.Name)))
			buf.WriteString(insertLineBreak("END VirtualMachine"))
		} else if obj, ok := specObj.(*tektoncdv1.Pipeline); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Pipeline: [%s] %s", obj.Namespace, obj.Name)))
			var pipeline *tektoncdv1.Pipeline
			if pipeline, err = k8stektoncd.GetPipeline(obj.Namespace, obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get Pipeline: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", pipeline))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Pipeline", obj.Name)))
			buf.WriteString(insertLineBreak("END Pipeline"))
		} else if obj, ok := specObj.(*tektoncdv1.Task); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Task: [%s] %s", obj.Namespace, obj.Name)))
			var task *tektoncdv1.Task
			if task, err = k8stektoncd.GetTask(obj.Namespace, obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get Task: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", task))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Task", obj.Name)))
			buf.WriteString(insertLineBreak("END Task"))
		} else if obj, ok := specObj.(*tektoncdv1.PipelineRun); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("PipelineRun: [%s] %s", obj.Namespace, obj.Name)))
			var pipelineRun *tektoncdv1.PipelineRun
			if pipelineRun, err = k8stektoncd.GetPipelineRun(obj.Namespace, obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get PipelineRun: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", pipelineRun))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "PipelineRun", obj.Name)))
			buf.WriteString(insertLineBreak("END PipelineRun"))
		} else if obj, ok := specObj.(*tektoncdv1.TaskRun); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("TaskRun: [%s] %s", obj.Namespace, obj.Name)))
			var taskRun *tektoncdv1.TaskRun
			if taskRun, err = k8stektoncd.GetTaskRun(obj.Namespace, obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get TaskRun: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", taskRun))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "TaskRun", obj.Name)))
			buf.WriteString(insertLineBreak("END TaskRun"))
		} else {
			log.Warnf("Object type unknown/not supported: %v", obj)
		}
	}
	return buf.String(), nil
}

// ScaleApplication  Scale the application
func (k *K8s) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	k8sOps := k8sApps
	for _, specObj := range ctx.App.SpecList {
		if !k.IsScalable(specObj) {
			continue
		}
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			log.Infof("Scale all Deployments")
			newScaleFactor := scaleFactorMap[obj.Name+DeploymentSuffix]

			t := func() (interface{}, bool, error) {
				dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
				if err != nil {
					return "", true, err // failed to get deployment, retry
				}
				*dep.Spec.Replicas = newScaleFactor
				dep, err = k8sOps.UpdateDeployment(dep)
				if err == nil {
					return dep, false, nil // succeeded, no retry
				}
				return "", true, err // failed to update deployment, retry
			}
			_, err := task.DoRetryWithTimeout(t, 2*time.Minute, time.Second)
			if err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update Deployment: %v. Err: %v", obj.Name, err),
				}
			}
			log.Infof("Deployment %s scaled to %d successfully.", obj.Name, newScaleFactor)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			log.Infof("Scale all Stateful sets")
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return err
			}
			newScaleFactor := scaleFactorMap[obj.Name+StatefulSetSuffix]
			*ss.Spec.Replicas = newScaleFactor
			if _, err := k8sOps.UpdateStatefulSet(ss); err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}
			log.Infof("StatefulSet %s scaled to %d successfully.", obj.Name, int(newScaleFactor))
		}
	}
	return nil
}

// GetScaleFactorMap Get scale Factory map
func (k *K8s) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	k8sOps := k8sApps
	scaleFactorMap := make(map[string]int32, len(ctx.App.SpecList))
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+DeploymentSuffix] = *dep.Spec.Replicas
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+StatefulSetSuffix] = *ss.Spec.Replicas
		}
	}
	return scaleFactorMap, nil
}

// StopSchedOnNode stop schedule on node
func (k *K8s) StopSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		Action: "stop",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStopSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

// StartSchedOnNode start schedule on node
func (k *K8s) StartSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         DefaultTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
		},
		Action: "start",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStartSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

// EnableSchedulingOnNode enable apps to be scheduled to a given k8s worker node
func (k *K8s) EnableSchedulingOnNode(n node.Node) error {
	return k8sCore.UnCordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

// DisableSchedulingOnNode disable apps to be scheduled to a given k8s worker node
func (k *K8s) DisableSchedulingOnNode(n node.Node) error {
	return k8sCore.CordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

// IsScalable check whether scalable
func (k *K8s) IsScalable(spec interface{}) bool {
	if obj, ok := spec.(*appsapi.Deployment); ok {
		dep, err := k8sApps.GetDeployment(obj.Name, obj.Namespace)
		if err != nil {
			log.Errorf("Failed to retrieve deployment [%s] %s. Cause: %v", obj.Namespace, obj.Name, err)
			return false
		}
		for _, vol := range dep.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				pvcName := vol.PersistentVolumeClaim.ClaimName
				pvc, err := k8sCore.GetPersistentVolumeClaim(pvcName, dep.Namespace)
				if err != nil {
					log.Errorf("Failed to retrieve PVC [%s] %s. Cause: %v", obj.Namespace, pvcName, err)
					return false
				}
				for _, ac := range pvc.Spec.AccessModes {
					if ac == corev1.ReadWriteOnce {
						return false
					}
				}
			}
		}
		return true
	} else if _, ok := spec.(*appsapi.StatefulSet); ok {
		return true
	}
	return false
}

// GetTokenFromConfigMap -  Retrieve the config map object and get auth-token
func (k *K8s) GetTokenFromConfigMap(configMapName string) (string, error) {
	var token string
	var err error
	var configMap *corev1.ConfigMap
	k8sOps := k8sCore
	if configMap, err = k8sOps.GetConfigMap(configMapName, "default"); err == nil {
		if secret, err := k8sOps.GetSecret(configMap.Data[secretNameKey], configMap.Data[secretNamespaceKey]); err == nil {
			if tk, ok := secret.Data["auth-token"]; ok {
				token = string(tk)
			}
		}
	}
	log.Infof("Token from secret: %s", token)
	return token, err
}

// createAdmissionRegistrationObjects creates objects in the `AdmissionRegistration` group (like ValidatingWebhookConfiguration)
func (k *K8s) createAdmissionRegistrationObjects(
	specObj interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8sAdmissionRegistration

	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sCore.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}
		err = k.addSecurityAnnotation(specObj, configMap, app)
		if err != nil {
			return nil, fmt.Errorf("failed to add annotations to migration object: %v", err)
		}

	}

	if obj, ok := specObj.(*admissionregistrationv1.ValidatingWebhookConfiguration); ok {
		obj.Namespace = ns.Name
		for i := range obj.Webhooks {
			obj.Webhooks[i].ClientConfig.Service.Namespace = ns.Name
		}

		vbc, err := k8sOps.CreateValidatingWebhookConfiguration(obj)

		if k8serrors.IsAlreadyExists(err) {
			if vbc, err := k8sOps.GetValidatingWebhookConfiguration(obj.Name); err == nil {
				log.Infof("[%v] Found existing ValidatingWebhookConfiguration: %v", app.Key, vbc.Name)
				return vbc, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to Create ValidatingWebhookConfiguration: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ValidatingWebhookConfiguration: %v", app.Key, vbc.Name)
		return vbc, nil
	} else if obj, ok := specObj.(*admissionregistrationv1beta1.ValidatingWebhookConfiguration); ok {
		obj.Namespace = ns.Name
		for i := range obj.Webhooks {
			obj.Webhooks[i].ClientConfig.Service.Namespace = ns.Name
		}

		vbc, err := k8sOps.CreateValidatingWebhookConfigurationV1beta1(obj)

		if k8serrors.IsAlreadyExists(err) {
			if vbc, err := k8sOps.GetValidatingWebhookConfigurationV1beta1(obj.Name); err == nil {
				log.Infof("[%v] Found existing ValidatingWebhookConfiguration: %v", app.Key, vbc.Name)
				return vbc, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to Create ValidatingWebhookConfiguration: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ValidatingWebhookConfiguration: %v", app.Key, vbc.Name)
		return vbc, nil
	}

	return nil, nil
}

// createVirtualMachineObjects creates the kubevirt VirtualMachines
func (k *K8s) createVirtualMachineObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	isPVCcloned := false
	if obj, ok := spec.(*kubevirtv1.VirtualMachine); ok {
		if source, exists := obj.Annotations["pvc.source"]; exists {
			if source == "cloned" {
				isPVCcloned = true
			}
		}
		if !isPVCcloned {
			// Validating to make sure the desired images are imported by CDI and volumes are ready to be used by Kubevirt VM
			virtualMachineVolumes := obj.Spec.Template.Spec.Volumes
			if len(virtualMachineVolumes) > 0 {
				for _, v := range virtualMachineVolumes {
					err := k.WaitForImageImportForVM(obj.Name, ns.Name, v)
					if err != nil {
						return nil, err
					}
				}
			}
			// Create VirtualMachine Spec
			if obj.Namespace != "kube-system" {
				obj.Namespace = ns.Name
			}
			vm, err := k8sKubevirt.CreateVirtualMachine(obj)
			if k8serrors.IsAlreadyExists(err) {
				if vm, err = k8sKubevirt.GetVirtualMachine(obj.Name, obj.Namespace); err == nil {
					log.Infof("[%v] Found existing VirtualMachine: %v", app.Key, obj.Name)
					return vm, nil
				}
			}

			if err != nil {
				return nil, &scheduler.ErrFailedToScheduleApp{
					App:   app,
					Cause: fmt.Sprintf("Failed to create VirtualMachine: %v, Err: %v", obj.Name, err),
				}
			}
			log.Infof("[%v] Created VirtualMachine: %v", app.Key, obj.Name)
			return vm, nil
		} else {
			// Create VirtualMachine Spec
			if obj.Namespace != "kube-system" {
				obj.Namespace = ns.Name
			}
			vm, err := k8sKubevirt.CreateVirtualMachine(obj)
			if k8serrors.IsAlreadyExists(err) {
				if vm, err = k8sKubevirt.GetVirtualMachine(obj.Name, obj.Namespace); err == nil {
					log.Infof("[%v] Found existing VirtualMachine: %v", app.Key, obj.Name)
					return vm, nil
				}
			}
			// After VM creation, wait for all associated DataVolumes to be ready.
			for _, dvt := range obj.Spec.DataVolumeTemplates {
				err = k.WaitForCloneToSucceed(ns.Name, dvt.Name)
				if err != nil {
					return nil, err
				}
			}
			log.Infof("Sleeping for 30 seconds to let data volume settle")
			time.Sleep(30 * time.Second)
			vm, err = k8sKubevirt.GetVirtualMachine(obj.Name, obj.Namespace)
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve VM after creating/waiting for DataVolumes: %v", err)
			}
			// Check if the VM is in 'Running' phase.
			if !vm.Status.Ready {
				return nil, fmt.Errorf("VM is not in the expected 'Running' state")
			}
			return vm, nil
		}
	}
	return nil, nil
}

func (k *K8s) WaitForCloneToSucceed(namespace, pvcName string) error {
	t := func() (interface{}, bool, error) {
		pvc, err := k8sCore.GetPersistentVolumeClaim(pvcName, namespace)
		if err != nil {
			return nil, true, err
		}
		if clonePhase, ok := pvc.Annotations["cdi.kubevirt.io/storage.condition.source.running.message"]; ok && clonePhase == "Clone Complete" {
			log.Infof("Clone operation succeeded for PVC: %s", pvcName)
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("waiting for clone operation to succeed for PVC [%s] in namespace [%s], current phase: %s", pvcName, namespace, pvc.Annotations["cdi.kubevirt.io/clonePhase"])
	}
	_, err := task.DoRetryWithTimeout(t, cdiImageImportTimeout, cdiImageImportRetry)
	return err
}

// createTektonObjects creates the Tektoncd objects
func (k *K8s) createTektonObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*tektoncdv1.Task); ok {

		// Create VirtualMachine Spec
		if obj.Namespace != "kube-system" {
			obj.Namespace = ns.Name
		}
		task, err := k8stektoncd.CreateTask(obj, obj.Namespace)
		if k8serrors.IsAlreadyExists(err) {
			if task, err = k8stektoncd.GetTask(obj.Namespace, obj.Name); err == nil {
				log.Infof("[%v] Found existing task: %v", app.Key, obj.Name)
				return task, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create task: %v, Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created task: %v", app.Key, obj.Name)
		return task, nil
	} else if obj, ok := spec.(*tektoncdv1.Pipeline); ok {

		// Create VirtualMachine Spec
		if obj.Namespace != "kube-system" {
			obj.Namespace = ns.Name
		}
		pipeline, err := k8stektoncd.CreatePipeline(obj, obj.Namespace)
		if k8serrors.IsAlreadyExists(err) {
			if pipeline, err = k8stektoncd.GetPipeline(obj.Namespace, obj.Name); err == nil {
				log.Infof("[%v] Found existing pipeline: %v", app.Key, obj.Name)
				return pipeline, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create pipeline: %v, Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created pipeline: %v", app.Key, obj.Name)
		return pipeline, nil
	} else {
		log.Infof("Object type unknown/not supported: %v", obj)
	}

	return nil, nil
}

func (k *K8s) WaitForImageImportForVM(vmName string, namespace string, v kubevirtv1.Volume) error {
	// Validating Volume Source of type PersistentVolumeClaim is ready to be used by Kubevirt VM
	if v.VolumeSource.PersistentVolumeClaim != nil {
		pvcName := v.VolumeSource.PersistentVolumeClaim.ClaimName
		pvc, err := k8sCore.GetPersistentVolumeClaim(pvcName, namespace)
		if err != nil {
			return err
		}
		endpointAnnotation, ok := pvc.Annotations[cdiPvcImportEndpointAnnotationKey]
		if ok && endpointAnnotation != "" {
			t := func() (interface{}, bool, error) {
				pvc, err = k8sCore.GetPersistentVolumeClaim(pvcName, namespace)
				messageAnnotation, ok := pvc.Annotations[cdiPvcRunningMessageAnnotationKey]
				if ok {
					if messageAnnotation == cdiImportComplete {
						log.Infof("%s - [%s]", cdiPvcRunningMessageAnnotationKey, pvc.Annotations[cdiPvcRunningMessageAnnotationKey])
						return "", false, nil
					}
					return "", true, fmt.Errorf("waiting for annotation [%s] in pvc [%s] in namespace [%s] for virtual machine [%s] to be %s, but got %s",
						cdiPvcRunningMessageAnnotationKey, pvcName, namespace, vmName, cdiImportComplete, pvc.Annotations[cdiPvcRunningMessageAnnotationKey])
				} else {
					return "", true, fmt.Errorf("annotation [%s] not found in pvc [%s] in namespace [%s] for virtual machine [%s]",
						cdiPvcRunningMessageAnnotationKey, pvcName, namespace, vmName)
				}
			}
			_, err = task.DoRetryWithTimeout(t, cdiImageImportTimeout, cdiImageImportRetry)
			if err != nil {
				return err
			}
		}
	}
	// TODO: For other Volume Source types like Data Volumes, validation logic should come here
	return nil
}

// destroyVirtualMachineObjects deletes the kubevirt VirtualMachines
func (k *K8s) destroyVirtualMachineObjects(
	spec interface{},
	app *spec.AppSpec,
) error {
	if obj, ok := spec.(*kubevirtv1.VirtualMachine); ok {
		err := k8sKubevirt.DeleteVirtualMachine(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy VirtualMachine: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed VirtualMachine: %v", app.Key, obj.Name)
	}
	return nil
}

// createCustomResourceObjects is used to create objects whose resource `kind` is defined by a CRD. NOTE: this is done using the `kubectl apply -f` command instead of the conventional method of using an api library
func (k *K8s) createCustomResourceObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {

	if obj, ok := spec.(*CustomResourceObjectYAML); ok {
		log.Warn("applying custom resources")
		cryaml := obj.Path
		if _, err := os.Stat(cryaml); baseErrors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("Cannot find yaml in path %s", cryaml)
		}
		cmdArgs := []string{"apply", "-f", cryaml, "-n", ns.Name}
		err := osutils.Kubectl(cmdArgs)
		if err != nil {
			return nil, fmt.Errorf("Error applying spec [%s], Error: %s", cryaml, err)
		}
		obj.Namespace = ns.Name
		obj.Name = "placeholder" //TODO1
		return obj, nil
	}

	return nil, nil
}

// destroyCustomResourceObjects is used to delete objects whose resource `kind` is defined by a CRD. NOTE: this is done using the `kubectl delete -f` command instead of the conventional method of using an api library
func (k *K8s) destroyCustomResourceObjects(spec interface{}, app *spec.AppSpec) error {

	if obj, ok := spec.(*CustomResourceObjectYAML); ok {
		cryaml := obj.Path
		if _, err := os.Stat(cryaml); baseErrors.Is(err, os.ErrNotExist) {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Custom Resource Object: %v. Err: Cannot find yaml in path: %v", obj.Name, cryaml),
			}
		}

		cmdArgs := []string{"delete", "-f", cryaml, "-n", obj.Namespace}
		err := osutils.Kubectl(cmdArgs)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Custom Resource Object: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed CustomResourceObject: %v", app.Key, obj.Name)
			return nil
		}
	}

	return nil
}

// createCRDObjects is used to create Resources in the group `apiextensions` group (like CRDs)
func (k *K8s) createCRDObjects(
	specObj interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {

	if obj, ok := specObj.(*apiextensionsv1.CustomResourceDefinition); ok {
		obj.Namespace = ns.Name
		err := k8sApiExtensions.RegisterCRD(obj)

		if k8serrors.IsAlreadyExists(err) {
			options := metav1.GetOptions{}
			if crd, err := k8sApiExtensions.GetCRD(obj.Name, options); err == nil {
				log.Infof("[%v] Found existing CRD: %v", app.Key, crd.Name)
				return crd, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to Register CRD: %v. Err: %v", obj.Name, err),
			}
		} else {
			options := metav1.GetOptions{}
			if crd, err := k8sApiExtensions.GetCRD(obj.Name, options); err == nil {
				log.Infof("[%v] Registered CRD: %v", app.Key, crd.Name)
				return crd, nil
			} else {
				// if it fails, then you need to `validate` before `get`
				return nil, &scheduler.ErrFailedToScheduleApp{
					App:   app,
					Cause: fmt.Sprintf("Failed to Get CRD after Registration: %v. Err: %v", obj.Name, err),
				}
			}
		}
	} else if obj, ok := specObj.(*apiextensionsv1beta1.CustomResourceDefinition); ok {
		obj.Namespace = ns.Name
		err := k8sApiExtensions.RegisterCRDV1beta1(obj)

		if k8serrors.IsAlreadyExists(err) {
			options := metav1.GetOptions{}
			if crd, err := k8sApiExtensions.GetCRDV1beta1(obj.Name, options); err == nil {
				log.Infof("[%v] Found existing CRDV1beta1: %v", app.Key, crd.Name)
				return crd, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to Register CRDV1beta1: %v. Err: %v", obj.Name, err),
			}
		} else {
			options := metav1.GetOptions{}
			if crd, err := k8sApiExtensions.GetCRDV1beta1(obj.Name, options); err == nil {
				log.Infof("[%v] Registered CRDV1beta1: %v", app.Key, crd.Name)
				return crd, nil
			} else {
				// if it fails, then you need to `validate` before `get`
				return nil, &scheduler.ErrFailedToScheduleApp{
					App:   app,
					Cause: fmt.Sprintf("Failed to Get CRDV1beta1 after Registration: %v. Err: %v", obj.Name, err),
				}
			}
		}
	}

	return nil, nil
}

func (k *K8s) createMigrationObjects(
	specObj interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8sStork
	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sCore.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}
		err = k.addSecurityAnnotation(specObj, configMap, app)
		if err != nil {
			return nil, fmt.Errorf("failed to add annotations to migration object: %v", err)
		}

	}

	if obj, ok := specObj.(*storkapi.ClusterPair); ok {
		obj.Namespace = ns.Name
		clusterPair, err := k8sOps.CreateClusterPair(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ClusterPair: %v", app.Key, clusterPair.Name)
		return clusterPair, nil
	} else if obj, ok := specObj.(*storkapi.Migration); ok {
		obj.Namespace = ns.Name
		migration, err := k8sOps.CreateMigration(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Migration: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created Migration: %v", app.Key, migration.Name)
		return migration, nil
	} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
		obj.Namespace = ns.Name
		migrationSchedule, err := k8sOps.CreateMigrationSchedule(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created MigrationSchedule: %v", app.Key, migrationSchedule.Name)
		return migrationSchedule, nil
	} else if obj, ok := specObj.(*storkapi.SchedulePolicy); ok {
		schedPolicy, err := k8sOps.CreateSchedulePolicy(obj)
		if k8serrors.IsAlreadyExists(err) {
			if schedPolicy, err = k8sOps.GetSchedulePolicy(obj.Name); err == nil {
				log.Infof("[%v] Found existing schedule policy: %v", app.Key, schedPolicy.Name)
				return schedPolicy, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created SchedulePolicy: %v", app.Key, schedPolicy.Name)
		return schedPolicy, nil
	} else if obj, ok := specObj.(*storkapi.ResourceTransformation); ok {
		obj.Namespace = ns.Name
		transform, err := k8sOps.CreateResourceTransformation(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ResourceTransformation: %v/%v. Obj: %v, Err: %v", obj.Name, obj.Namespace, obj, err),
			}
		}
		log.Infof("[%v] Created ResourceTransformation: %v", app.Key, transform.Name)
		return transform, nil
	}

	return nil, nil
}

func (k *K8s) getPodsUsingStorage(pods []corev1.Pod, provisioner string) []corev1.Pod {
	k8sOps := k8sCore
	podsUsingStorage := make([]corev1.Pod, 0)
	for _, pod := range pods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			pvc, err := k8sOps.GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
			if err != nil {
				log.Errorf("failed to get pvc [%s] %s. Cause: %v", vol.PersistentVolumeClaim.ClaimName, pod.Namespace, err)
				return podsUsingStorage
			}
			if scProvisioner, err := k8sOps.GetStorageProvisionerForPVC(pvc); err == nil && scProvisioner == volume.GetStorageProvisioner() {
				podsUsingStorage = append(podsUsingStorage, pod)
				break
			}
		}
	}
	return podsUsingStorage
}

// PrepareNodeToDecommission Prepare the Node for decommission
func (k *K8s) PrepareNodeToDecommission(n node.Node, provisioner string) error {
	k8sOps := k8sCore
	pods, err := k8sOps.GetPodsByNode(n.Name, "")
	if err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to get pods on the node: %v. Err: %v", n.Name, err),
		}
	}
	podsUsingStorage := k.getPodsUsingStorage(pods.Items, provisioner)
	// double the timeout every 40 pods
	timeout := DefaultTimeout * time.Duration(len(podsUsingStorage)/40+1)
	if err = k8sOps.DrainPodsFromNode(n.Name, podsUsingStorage, timeout, DefaultRetryInterval); err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to drain pods from node: %v. Err: %v", n.Name, err),
		}
	}
	return nil
}

func (k *K8s) destroyMigrationObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.ClusterPair); ok {
		err := k8sOps.DeleteClusterPair(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed ClusterPair: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.Migration); ok {
		err := k8sOps.DeleteMigration(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete Migration: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed Migration: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
		err := k8sOps.DeleteMigrationSchedule(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed MigrationSchedule: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.SchedulePolicy); ok {
		err := k8sOps.DeleteSchedulePolicy(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Destroyed SchedulePolicy: %v", app.Key, obj.Name)

	} else if obj, ok := specObj.(*storkapi.ResourceTransformation); ok {
		err := k8sOps.DeleteResourceTransformation(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ResourceTransformation: %v. Err: %v", obj.Name, err),
			}
		}

	}
	return nil
}

func (k *K8s) destroyVolumeSnapshotRestoreObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
		err := k8sOps.DeleteVolumeSnapshotRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed VolumeSnapshotRestore: %v", app.Key, obj.Name)
	}
	return nil
}

// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
// parent volumes
func (k *K8s) ValidateVolumeSnapshotRestore(ctx *scheduler.Context, timeStart time.Time) error {
	var err error
	var snapRestore *storkapi.VolumeSnapshotRestore
	if ctx == nil {
		return fmt.Errorf("no context provided")
	}
	// extract volume name and snapshotname from context
	// can do it using snapRestore.Status.Volume
	k8sOps := k8sStork
	specObjects := ctx.App.SpecList
	driver, err := volume.Get(k.VolDriverName)
	if err != nil {
		return err
	}

	for _, specObj := range specObjects {
		if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			snapRestore, err = k8sOps.GetVolumeSnapshotRestore(obj.Name, obj.Namespace)
			if err != nil {
				return fmt.Errorf("unable to restore volumesnapshotrestore details %v", err)
			}
			err = k8sOps.ValidateVolumeSnapshotRestore(snapRestore.Name, snapRestore.Namespace, DefaultTimeout,
				DefaultRetryInterval)
			if err != nil {
				return err
			}

		}
	}
	if snapRestore == nil {
		return fmt.Errorf("no valid volumesnapshotrestore specs found")
	}

	for _, vol := range snapRestore.Status.Volumes {
		log.Infof("validating volume %v is restored from %v", vol.Volume, vol.Snapshot)
		snapshotData, err := k8sExternalStorage.GetSnapshotData(vol.Snapshot)
		if err != nil {
			return fmt.Errorf("failed to retrieve VolumeSnapshotData %s: %v",
				vol.Snapshot, err)
		}
		err = k8sExternalStorage.ValidateSnapshotData(snapshotData.Metadata.Name, false, DefaultTimeout, DefaultRetryInterval)
		if err != nil {
			return fmt.Errorf("snapshot: %s is not complete. %v", snapshotData.Metadata.Name, err)
		}
		// validate each snap restore
		if err := driver.ValidateVolumeSnapshotRestore(vol.Volume, snapshotData, timeStart); err != nil {
			return err
		}
	}

	return nil
}

func (k *K8s) createBackupObjects(
	specObj interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.BackupLocation); ok {
		obj.Namespace = ns.Name
		backupLocation, err := k8sOps.CreateBackupLocation(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created BackupLocation: %v", app.Key, backupLocation.Name)
		return backupLocation, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
		obj.Namespace = ns.Name
		applicationBackup, err := k8sOps.CreateApplicationBackup(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ApplicationBackup: %v", app.Key, applicationBackup.Name)
		return applicationBackup, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
		obj.Namespace = ns.Name
		applicationRestore, err := k8sOps.CreateApplicationRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ApplicationRestore: %v", app.Key, applicationRestore.Name)
		return applicationRestore, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
		applicationClone, err := k8sOps.CreateApplicationClone(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Created ApplicationClone: %v", app.Key, applicationClone.Name)
		return applicationClone, nil
	}
	return nil, nil
}

func (k *K8s) destroyBackupObjects(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.BackupLocation); ok {
		err := k8sOps.DeleteBackupLocation(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed BackupLocation: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
		err := k8sOps.DeleteApplicationBackup(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed ApplicationBackup: %v", app.Key, obj.Name)

	} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
		err := k8sOps.DeleteApplicationRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed ApplicationRestore: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
		err := k8sOps.DeleteApplicationClone(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		log.Infof("[%v] Destroyed ApplicationClone: %v", app.Key, obj.Name)
	}
	return nil
}

func (k *K8s) createRbacObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*rbacv1.Role); ok {
		obj.Namespace = ns.Name
		role, err := k8sRbac.CreateRole(obj)
		if k8serrors.IsAlreadyExists(err) {
			if role, err = k8sRbac.GetRole(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Role: %v", app.Key, role.Name)
				return role, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Role: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Role: %v", app.Key, role.Name)
		return role, nil
	} else if obj, ok := spec.(*rbacv1.RoleBinding); ok {
		obj.Namespace = ns.Name
		rolebinding, err := k8sRbac.CreateRoleBinding(obj)
		if k8serrors.IsAlreadyExists(err) {
			if rolebinding, err = k8sRbac.GetRoleBinding(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Role Binding: %v", app.Key, rolebinding.Name)
				return rolebinding, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Role Binding: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Role Binding: %v", app.Key, rolebinding.Name)
		return rolebinding, nil
	} else if obj, ok := spec.(*rbacv1.ClusterRole); ok {
		obj.Namespace = ns.Name
		clusterrole, err := k8sRbac.CreateClusterRole(obj)
		if k8serrors.IsAlreadyExists(err) {
			if clusterrole, err = k8sRbac.GetClusterRole(obj.Name); err == nil {
				log.Infof("[%v] Found existing Role Binding: %v", app.Key, clusterrole.Name)
				return clusterrole, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Cluster Role: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Cluster Role: %v", app.Key, clusterrole.Name)
		return clusterrole, nil
	} else if obj, ok := spec.(*rbacv1.ClusterRoleBinding); ok {
		obj.Namespace = ns.Name
		for i := range obj.Subjects {
			// since everything in a spec is in the same namespace in cluster, we can set here ONLY for namespaced object:
			if obj.Subjects[i].Kind == "ServiceAccount" {
				obj.Subjects[i].Namespace = ns.Name
			}
		}
		clusterrolebinding, err := k8sRbac.CreateClusterRoleBinding(obj)
		if k8serrors.IsAlreadyExists(err) {
			if clusterrolebinding, err = k8sRbac.GetClusterRoleBinding(obj.Name); err == nil {
				log.Infof("[%v] Found existing Cluster Role Binding: %v", app.Key, clusterrolebinding.Name)
				return clusterrolebinding, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Cluster Role Binding: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Cluster Role: %v", app.Key, clusterrolebinding.Name)
		return clusterrolebinding, nil
	} else if obj, ok := spec.(*corev1.ServiceAccount); ok {
		obj.Namespace = ns.Name
		serviceaccount, err := k8sCore.CreateServiceAccount(obj)
		if k8serrors.IsAlreadyExists(err) {
			if serviceaccount, err = k8sCore.GetServiceAccount(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Service Account: %v", app.Key, serviceaccount.Name)
				return serviceaccount, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service Account: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Service Account: %v", app.Key, serviceaccount.Name)
		return serviceaccount, nil
	}

	return nil, nil
}

// destroyRbacObjects destroys objects in the `Rbac.authorization` group (like ClusterRole, ClusterRoleBinding, ServiceAccount)
func (k *K8s) destroyRbacObjects(spec interface{}, app *spec.AppSpec) error {

	if obj, ok := spec.(*rbacv1.ClusterRole); ok {
		err := k8sRbac.DeleteClusterRole(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ClusterRole: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed ClusterRole: %v", app.Key, obj.Name)
			return nil
		}
	} else if obj, ok := spec.(*rbacv1.ClusterRoleBinding); ok {
		err := k8sRbac.DeleteClusterRoleBinding(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ClusterRoleBinding: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed ClusterRoleBinding: %v", app.Key, obj.Name)
			return nil
		}
	} else if obj, ok := spec.(*corev1.ServiceAccount); ok {
		err := k8sCore.DeleteServiceAccount(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ServiceAccount: %v. Err: %v", obj.Name, err),
			}
		} else {
			log.Infof("[%v] Destroyed ServiceAccount: %v", app.Key, obj.Name)
			return nil
		}
	}

	return nil
}

func (k *K8s) createNetworkingObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*networkingv1beta1.Ingress); ok {
		obj.Namespace = ns.Name
		ingress, err := k8sNetworking.CreateIngress(obj)
		if k8serrors.IsAlreadyExists(err) {
			if ingress, err = k8sNetworking.GetIngress(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Ingress: %v", app.Key, ingress.Name)
				return ingress, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Ingress: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created Ingress: %v", app.Key, ingress.Name)
		return ingress, nil
	}
	return nil, nil
}

func (k *K8s) createBatchObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*batchv1beta1.CronJob); ok {
		obj.Namespace = ns.Name
		cronjob, err := k8sBatch.CreateCronJobV1beta1(obj)
		if k8serrors.IsAlreadyExists(err) {
			if cronjob, err = k8sBatch.GetCronJobV1beta1(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing CronJob: %v", app.Key, cronjob.Name)
				return cronjob, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create CronJob: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created CronJob: %v", app.Key, cronjob.Name)
		return cronjob, nil
	} else if obj, ok := spec.(*batchv1.Job); ok {
		obj.Namespace = ns.Name
		job, err := k8sBatch.CreateJob(obj)
		if k8serrors.IsAlreadyExists(err) {
			if job, err = k8sBatch.GetJob(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing Job: %v", app.Key, job.Name)
				return job, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Job: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created CronJob: %v", app.Key, job.Name)
		return job, nil
	}
	return nil, nil
}

func (k *K8s) createServiceMonitorObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*monitoringv1.ServiceMonitor); ok {
		if obj.Namespace == "" {
			obj.Namespace = ns.Name
		}
		serviceMonitor, err := k8sMonitoring.CreateServiceMonitor(obj)
		if k8serrors.IsAlreadyExists(err) {
			if serviceMonitor, err = k8sMonitoring.GetServiceMonitor(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing ServiceMonitor: %v", app.Key, serviceMonitor.Name)
				return serviceMonitor, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ServiceMonitor: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created ServiceMonitor: %v", app.Key, serviceMonitor.Name)
		return serviceMonitor, nil
	}
	return nil, nil
}

func (k *K8s) createPodDisruptionBudgetObjects(
	spec interface{},
	ns *corev1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	if obj, ok := spec.(*policyv1.PodDisruptionBudget); ok {
		if obj.Namespace == "" {
			obj.Namespace = ns.Name
		}
		podDisruptionBudget, err := k8sPolicy.CreatePodDisruptionBudget(obj)
		if k8serrors.IsAlreadyExists(err) {
			if podDisruptionBudget, err = k8sPolicy.GetPodDisruptionBudget(obj.Name, obj.Namespace); err == nil {
				log.Infof("[%v] Found existing PodDisruptionBudget: %v", app.Key, podDisruptionBudget.Name)
				return podDisruptionBudget, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PodDisruptionBudget: %v. Err: %v", obj.Name, err),
			}
		}

		log.Infof("[%v] Created PodDisruptionBudget: %v", app.Key, podDisruptionBudget.Name)
		return podDisruptionBudget, nil
	}
	return nil, nil
}

func (k *K8s) destroyPodDisruptionBudgetObjects(
	spec interface{},
	app *spec.AppSpec,
) error {
	if obj, ok := spec.(*policyv1.PodDisruptionBudget); ok {
		err := k8sPolicy.DeletePodDisruptionBudget(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy PodDisruptionBudget: %v. Err: %v", obj.Name, err),
			}
		}
	}
	return nil
}

func (k *K8s) destroyServiceMonitorObjects(spec interface{},
	app *spec.AppSpec,
) error {
	if obj, ok := spec.(*monitoringv1.ServiceMonitor); ok {
		err := k8sMonitoring.DeleteServiceMonitor(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy ServiceMonitor: %v. Err: %v", obj.Name, err),
			}
		}
	}
	return nil
}

// EstimatePVCExpansion calculates expected size of PVC based on autopilot rule and workload
func (k *K8s) EstimatePVCExpansion(pvc *corev1.PersistentVolumeClaim, apRule apapi.AutopilotRule, wSize uint64) (uint64, int, error) {
	pvcObjSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	pvcSize, _ := pvcObjSize.AsInt64()
	initialPVCSize := uint64(pvcSize)

	volDriver, err := volume.Get(k.VolDriverName)
	if err != nil {
		return 0, 0, err
	}
	if isAutopilotMatchPvcLabels(apRule, pvc) {
		return volDriver.EstimateVolumeExpand(apRule, initialPVCSize, wSize)
	}
	return initialPVCSize, 0, nil
}

// ValidateAutopilotEvents verifies proper alerts and events on resize completion
func (k *K8s) ValidateAutopilotEvents(ctx *scheduler.Context) error {

	eventMap := make(map[string]int32)
	for _, event := range k.GetEvents()["AutopilotRule"] {
		eventMap[event.Message] = event.Count
	}

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {

			autopilotEnabled := false
			var err error
			if pvcAnnotationValue, ok := obj.Annotations[autopilotEnabledAnnotationKey]; ok {
				autopilotEnabled, err = strconv.ParseBool(pvcAnnotationValue)
				if err != nil {
					return err
				}
			}
			if autopilotEnabled {
				listApRules, err := autopilot.Instance().ListAutopilotRules()
				if err != nil {
					return err
				}
				wSize, err := k.GetWorkloadSizeFromAppSpec(ctx)
				if err != nil {
					return err
				}
				for _, rule := range listApRules.Items {
					_, resizeCount, err := k.EstimatePVCExpansion(obj, rule, wSize)
					if err != nil {
						return err
					}
					coolDownPeriod := rule.Spec.ActionsCoolDownPeriod
					if coolDownPeriod == 0 {
						coolDownPeriod = 5 // default autopilot cool down period
					}
					// sleep to wait until all events are published
					sleepTime := time.Second * time.Duration(coolDownPeriod+10)
					log.Infof("sleep %s until all events are published", sleepTime)
					time.Sleep(sleepTime)

					objectToValidateName := fmt.Sprintf("%s:pvc-%s", rule.Name, obj.UID)
					log.Infof("[%s] Validating events", objectToValidateName)
					err = k.validateEvents(objectToValidateName, eventMap, int32(resizeCount))
					if err != nil {
						return err
					}
				}
			}
		}
	}
	log.Infof("Finished validating events")
	return nil
}

// ValidateAutopilotRuleObjects validates autopilot rule objects
func (k *K8s) ValidateAutopilotRuleObjects() error {

	// TODO: Implement ARO validation for specific pool if autopilot rule has pool LabelSelector
	namespace, err := k.GetAutopilotNamespace()
	if err != nil {
		return err
	}

	expectedAroStates := []apapi.RuleState{
		apapi.RuleStateTriggered,
		apapi.RuleStateActiveActionsPending,
		apapi.RuleStateActiveActionsInProgress,
		apapi.RuleStateActiveActionsTaken,
		apapi.RuleStateNormal,
	}

	listAutopilotRuleObjects, err := k8sAutopilot.ListAutopilotRuleObjects(namespace)
	if err != nil {
		return err
	}
	if len(listAutopilotRuleObjects.Items) == 0 {
		log.Warnf("the list of autopilot rule objects is empty, please make sure that you have an appropriate autopilot rule")
		return nil
	}
	for _, aro := range listAutopilotRuleObjects.Items {
		var aroStates []apapi.RuleState
		for _, aroStatusItem := range aro.Status.Items {
			if aroStatusItem.State == "" {
				continue
			}
			aroStates = append(aroStates, aroStatusItem.State)
		}
		if reflect.DeepEqual(aroStates, expectedAroStates) {
			log.Debugf("autopilot rule object: %s has all expected states", aro.Name)
		} else {
			formattedObject, _ := json.MarshalIndent(listAutopilotRuleObjects.Items, "", "\t")
			log.Debugf("autopilot rule objects items: %s", string(formattedObject))
			return fmt.Errorf("autopilot rule object: %s doesn't have all expected states", aro.Name)
		}
	}
	return nil
}

// VerifyPoolResizeARO() created and resize completed
func (k *K8s) VerifyPoolResizeARO(ruleName apapi.AutopilotRule) (bool, error) {
	var ruleTriggered bool
	namespace, err := k.GetAutopilotNamespace()
	if err != nil {
		return false, fmt.Errorf("No Namespace")
	}

	expectedAroStates := []apapi.RuleState{
		apapi.RuleStateTriggered,
		apapi.RuleStateActiveActionsPending,
		apapi.RuleStateActiveActionsInProgress,
		apapi.RuleStateActiveActionsTaken,
	}

	listAutopilotRuleObjects, err := k8sAutopilot.ListAutopilotRuleObjects(namespace)
	if err != nil {
		return false, fmt.Errorf("No ARO available")
	}
	if len(listAutopilotRuleObjects.Items) == 0 {
		return false, fmt.Errorf("The list of autopilot rule objects is empty, please make sure that you have an appropriate autopilot rule")
	}
	//Find  AROs  which has matching rule Name
	for _, aro := range listAutopilotRuleObjects.Items {
		log.InfoD("Rule Name %v", aro.GetObjectMeta().GetName())
		if strings.Contains(aro.GetObjectMeta().GetName(), ruleName.Name) {
			var aroStates []apapi.RuleState
			for _, aroStatusItem := range aro.Status.Items {
				if aroStatusItem.State == "" {
					continue
				}
				if len(aroStates) == 0 {
					aroStates = append(aroStates, aroStatusItem.State)
				} else {
					var existState bool
					for _, state := range aroStates {
						if aroStatusItem.State == state {
							existState = true
						}
					}
					if !existState {
						aroStates = append(aroStates, aroStatusItem.State)
					}
				}
			}
			counter := len(expectedAroStates)
			for _, expectState := range expectedAroStates {
				for _, actualState := range aroStates {
					if expectState == actualState {
						counter -= 1
					}
				}
			}
			if counter == 0 {
				log.Debugf("autopilot rule object: %s has all expected states", aro.Name)
				ruleTriggered = true
			} else {
				log.Debugf("Observed ARO STATE: %v", aroStates)
				log.Debugf("expected  ARO STATEs: %v", expectedAroStates)
				formattedObject, _ := json.MarshalIndent(listAutopilotRuleObjects.Items, "", "\t")
				log.Debugf("autopilot rule objects items: %s", string(formattedObject))
				return false, fmt.Errorf("autopilot rule object: %s doesn't have all expected states", aro.Name)
			}
		} else {
			log.InfoD("Rule Name observed %v", aro.GetObjectMeta().GetName())
			log.InfoD("Rule Name observed %v", ruleName.Name)
		}
	}
	if ruleTriggered {
		return true, nil
	}
	return false, fmt.Errorf("No ARO found for rule: %v ", ruleName)
}

// WaitForRebalanceAROToComplete Wait for Rebalance to complete.
func (k *K8s) WaitForRebalanceAROToComplete() error {
	var eventCheckInterval = 60 * time.Second
	var eventCheckTimeout = 30 * time.Minute
	t := func() (interface{}, bool, error) {
		namespace, err := k.GetAutopilotNamespace()
		if err != nil {
			return nil, false, err
		}
		listAutopilotRuleObjects, err := k8sAutopilot.ListAutopilotRuleObjects(namespace)
		if err != nil {
			return nil, true, err
		}
		if len(listAutopilotRuleObjects.Items) == 0 {
			return nil, true, fmt.Errorf("The list of autopilot rule objects is empty, please make sure that you have an appropriate autopilot rule")
		}
		// IF Reabalance summary started and it's timestamp before "ActiveActionsTaken" timestamp
		// means rebalance has been started and action has been taken.
		for _, aro := range listAutopilotRuleObjects.Items {
			hasRebalanceStarted := false
			var rebalanceStartedTimeStamp int64
			log.InfoD("Rule Name %v", aro.GetObjectMeta().GetName())
			for _, aroStatusItem := range aro.Status.Items {
				if aroStatusItem.State == "" {
					continue
				}
				if strings.Contains(aroStatusItem.Message, "Rebalance summary:") {
					hasRebalanceStarted = true
					log.InfoD("Rebalance has started ")
					rebalanceStartedTimeStamp = aroStatusItem.LastProcessTimestamp.Unix()
				}
				if aroStatusItem.State == apapi.RuleStateActiveActionsTaken {
					if aroStatusItem.LastProcessTimestamp.Unix() > rebalanceStartedTimeStamp && hasRebalanceStarted == true {
						log.InfoD("Rebalance Action has been taken on ARO %s ", aro.GetObjectMeta().GetName())
						return nil, false, nil
					} else {
						log.InfoD(" Expected  state: %s available but it is before Rebalance Started ", aroStatusItem.State)
					}
				}
				if aroStatusItem.State == apapi.RuleStateNormal && strings.Contains(aroStatusItem.Message, "ActiveActionsPending => Normal") {
					if aroStatusItem.LastProcessTimestamp.Unix() > rebalanceStartedTimeStamp && hasRebalanceStarted == true {
						log.InfoD("Rebalance Action has been taken on ARO %s ", aro.GetObjectMeta().GetName())
						return nil, false, nil
					} else {
						log.InfoD(" Expected  state: %s available but it is before Rebalance Started ", aroStatusItem.State)
					}
				}
			}
		}
		return nil, true, fmt.Errorf("Rebalance ARO not completed or did not start yet")
	}

	if _, err := task.DoRetryWithTimeout(t, eventCheckTimeout, eventCheckInterval); err != nil {
		return err
	}
	return nil
}

// GetIOBandwidth takes in the pod name and namespace and returns the IOPs speed
func (k *K8s) GetIOBandwidth(podName string, namespace string) (int, error) {
	log.Infof("Getting the IO Speed in pod %s", podName)
	pod, err := k8sCore.GetPodByName(podName, namespace)
	if err != nil {
		return 0, fmt.Errorf("error in getting FIO PODS")
	}
	logOptions := corev1.PodLogOptions{
		// Getting 250 lines from the pod logs to get the io_bytes
		TailLines: getInt64Address(250),
	}
	log, err := k8sCore.GetPodLog(pod.Name, pod.Namespace, &logOptions)
	if err != nil {
		return 0, err
	}
	outputLines := strings.Split(log, "\n")
	for _, line := range outputLines {
		if strings.Contains(line, "iops") {
			re := regexp.MustCompile(`[0-9]+`)
			speedBytes := string(re.FindAll([]byte(line), -1)[0])
			speed, err := strconv.Atoi(speedBytes)
			if err != nil {
				return 0, fmt.Errorf("Error in getting the speed")
			}
			// We need to consider non Zero number from the speeds returned,
			// since it will return read speed, trim speed and we are performing only write operation
			if speed == 0 {
				continue
			}
			return speed, nil
		}
	}
	return 0, fmt.Errorf("pod %s does not have bandwidth in logs", podName)
}

func getInt64Address(x int64) *int64 {
	return &x
}

func (k *K8s) validateEvents(objName string, events map[string]int32, count int32) error {
	log.Debugf("expected %d resized in events validation", count)
	if count == 0 {
		// nothing to validate
		return nil
	}
	expectedEvents := map[string]int32{
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateInit, apapi.RuleStateNormal):                                  1,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateNormal, apapi.RuleStateTriggered):                             count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateTriggered, apapi.RuleStateActiveActionsPending):               count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsPending, apapi.RuleStateActiveActionsInProgress): count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsTaken):   count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsTaken, apapi.RuleStateNormal):                    count,
	}

	for message, expectedCount := range expectedEvents {
		if _, ok := events[message]; !ok {
			return fmt.Errorf("expected event with message '%s'", message)
		}
		occurrences := events[message]
		// Object should change its state exactly as expected amount for all following states
		if strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateInit, apapi.RuleStateNormal)) ||
			strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsTaken)) ||
			strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsTaken, apapi.RuleStateNormal)) {
			if occurrences != expectedCount {
				return fmt.Errorf("expected number of occurances %d for event message '%s'. Got: %d", expectedCount, message, occurrences)
			}
			continue
		}
		// it's possible that object changes the following states more times than expected if action is declined
		// Normal => Triggered
		// Triggered => ActiveActionsPending
		// ActiveActionsPending => ActiveActionsInProgress
		if !(occurrences >= expectedCount) {
			return fmt.Errorf("expected number of occurances >= %d for event message '%s'. Got: %d", expectedCount, message, occurrences)
		}
	}

	// TODO: for negative case (when added) if action is declined, check for the following states:
	// ActiveActionsInProgress => ActionsDeclined
	// ActionsDeclined => Triggered
	return nil
}

func isAutopilotMatchPvcLabels(apRule apapi.AutopilotRule, pvc *corev1.PersistentVolumeClaim) bool {
	apRuleLabels := apRule.Spec.Selector.LabelSelector.MatchLabels
	for k, v := range apRuleLabels {
		if pvcLabelValue, ok := pvc.Labels[k]; ok {
			if pvcLabelValue == v {
				return true
			}
		}
	}
	return false
}

// collectEvents collects all autopilot events until caller stops the process
func (k *K8s) collectEvents() error {
	log.Info("Started collecting events")

	lock := sync.Mutex{}
	t := time.Now()
	namespace := ""
	clientset, err := k.getKubeClient("")
	if err != nil {
		return err
	}

	iface, err := clientset.CoreV1().Events(namespace).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range iface.ResultChan() {
		event, ok := i.Object.(*corev1.Event)
		if event == nil {
			continue
		}
		if ok {
			if !event.LastTimestamp.After(t) {
				// skip this event since it happened before event collection has been started
				continue
			}
			e := scheduler.Event{
				Message:   event.Message,
				EventTime: event.EventTime,
				Count:     event.Count,
				LastSeen:  event.LastTimestamp,
				Kind:      event.Kind,
				Type:      event.Type,
			}

			lock.Lock()
			storage := k.eventsStorage[event.InvolvedObject.Kind]
			storage = append(storage, e)
			k.eventsStorage[event.InvolvedObject.Kind] = storage
			lock.Unlock()
		}
	}
	return nil
}

func (k *K8s) getKubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error

	if len(kubeconfig) == 0 {
		kubeconfig = os.Getenv("KUBECONFIG")
	}

	if len(kubeconfig) > 0 {
		log.Debugf("using kubeconfig: %s to create k8s client", kubeconfig)
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		log.Debugf("will use in-cluster config to create k8s client")
		cfg, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return kubeClient, nil
}

// GetEvents dumps events from event storage
func (k *K8s) GetEvents() map[string][]scheduler.Event {
	log.Infof("Getting events for validation")
	copyMap := make(map[string][]scheduler.Event)
	// Copy from the original map to the target map
	for key, value := range k.eventsStorage {
		copyMap[key] = value
	}
	return copyMap
}

// AddLabelOnNode adds label for a given node
func (k *K8s) AddLabelOnNode(n node.Node, lKey string, lValue string) error {
	k8sOps := k8sCore

	if err := k8sOps.AddLabelOnNode(n.Name, lKey, lValue); err != nil {
		return &scheduler.ErrFailedToAddLabelOnNode{
			Key:   lKey,
			Value: lValue,
			Node:  n,
			Cause: fmt.Sprintf("Failed to add label on node. Err: %v", err),
		}
	}
	log.Infof("Added label on %s node: %s=%s", n.Name, lKey, lValue)
	return nil
}

// RemoveLabelOnNode adds label for a given node
func (k *K8s) RemoveLabelOnNode(n node.Node, lKey string) error {
	k8sOps := k8sCore

	if err := k8sOps.RemoveLabelOnNode(n.Name, lKey); err != nil {
		return &scheduler.ErrFailedToRemoveLabelOnNode{
			Key:   lKey,
			Node:  n,
			Cause: fmt.Sprintf("Failed to remove label on node. Err: %v", err),
		}
	}
	log.Infof("Removed label: %s on node: %s", lKey, n.Name)
	return nil
}

// IsAutopilotEnabledForVolume checks if autopilot enabled for a given volume
func (k *K8s) IsAutopilotEnabledForVolume(vol *volume.Volume) bool {
	autopilotEnabled := false
	if volAnnotationValue, ok := vol.Annotations[autopilotEnabledAnnotationKey]; ok {
		autopilotEnabled, _ = strconv.ParseBool(volAnnotationValue)
	}
	return autopilotEnabled
}

// SaveSchedulerLogsToFile gathers all scheduler logs into a file
func (k *K8s) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(k.NodeDriverName)
	cmd := fmt.Sprintf("journalctl -lu %s* > %s/kubelet.log", SystemdSchedServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         DefaultTimeout,
		TimeBeforeRetry: DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

func (k *K8s) addLabelsToPVC(pvc *corev1.PersistentVolumeClaim, labels map[string]string) {
	if len(pvc.Labels) == 0 {
		pvc.Labels = map[string]string{}
	}
	for k, v := range labels {
		pvc.Labels[k] = v
	}
}

func (k *K8s) addAnnotationsToPVC(pvc *corev1.PersistentVolumeClaim, annotations map[string]string) {
	if len(pvc.Annotations) == 0 {
		pvc.Annotations = map[string]string{}
	}
	for k, v := range annotations {
		pvc.Annotations[k] = v
	}
}

// GetAutopilotNamespace returns the autopilot namespace
func (k *K8s) GetAutopilotNamespace() (string, error) {
	var pods *corev1.PodList
	var err error
	var ns string
	ns, err = k.GetPortworxNamespace()
	if err != nil {
		if strings.Contains(err.Error(), PortworxNotDeployed) {
			//Return default AutopilotNamespace
			return autopilotDefaultNamespace, nil
		}
		return "", fmt.Errorf("Failed to find portworx namespace %v", err)
	}
	log.Infof("Portworx installed on namespace %s", ns)
	//Find out autopilot pod deployed namespace by label
	pods, err = k8sCore.ListPods(map[string]string{
		AutopilotLabelNameKey: AutopilotLabelValue,
	})
	if err != nil {
		return ns, err
	}
	ns = ""
	if len(pods.Items) > 0 {
		ns = pods.Items[0].Namespace
	}
	if len(ns) == 0 {
		return ns, fmt.Errorf("Failed to find autopilot namespace using pods with label [%s=%s]", AutopilotLabelNameKey, AutopilotLabelValue)
	}
	return ns, nil
}

// IsAutopilotEnabled returns the autopilot enabled or not
func (k *K8s) IsAutopilotEnabled() (bool, error) {
	var pods *corev1.PodList
	var err error
	//Find out autopilot pod deployed or not
	pods, err = k8sCore.ListPods(map[string]string{
		AutopilotLabelNameKey: AutopilotLabelValue,
	})
	if err != nil {
		return false, err
	}
	if len(pods.Items) == 0 {
		return false, fmt.Errorf("Failed to find autopilot pods with label [%s=%s]", AutopilotLabelNameKey, AutopilotLabelValue)
	}
	return true, nil
}

// GetPortworxNamespace returns namespace where Portworx is deployed based on the portworx-service location
func (k *K8s) GetPortworxNamespace() (string, error) {
	allServices, err := k8sCore.ListServices("", metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	var namespaces []string
	for _, svc := range allServices.Items {
		if svc.Name == portworxServiceName {
			namespaces = append(namespaces, svc.Namespace)
		}
	}
	if len(namespaces) > 0 {
		// If portworx-service is deployed in 2 namespaces, return the none kube-system namespace
		if len(namespaces) == 2 {
			log.Debugf("Found [%s] service in 2 different namespaces %s", portworxServiceName, namespaces)
			for _, namespace := range namespaces {
				if namespace != "kube-system" {
					log.Debugf("When Portworx deployed outside of [kube-system] namespace, it also creates [%s] service in the [kube-system] namespace as well as in the namespace it is deployed in", portworxServiceName)
					log.Debugf("Will assume Portworx is deployed in [%s] namespace", namespace)
					return namespace, nil
				}
			}
		}

		// If portworx-service is deployed in more than 2 namespaces, something is wrong here
		if len(namespaces) > 2 {
			return "", fmt.Errorf("Portworx service [%s] is deployed in too many namespaces %s, something is wrong here", portworxServiceName, namespaces)
		}

		// portworx-service is deployed in kube-system if only 1 namesapce is found
		return namespaces[0], nil
	}

	return "", fmt.Errorf("%s on this K8S cluster", PortworxNotDeployed)
}

// CreateAutopilotRule creates the AutopilotRule object
func (k *K8s) CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	t := func() (interface{}, bool, error) {
		apRule.Labels = defaultTorpedoLabel
		aRule, err := k8sAutopilot.CreateAutopilotRule(&apRule)
		if k8serrors.IsAlreadyExists(err) {
			if rule, err := k8sAutopilot.GetAutopilotRule(apRule.Name); err == nil {
				log.Infof("Using existing AutopilotRule: %v", rule.Name)
				return aRule, false, nil
			}
		}
		if err != nil {
			return nil, true, fmt.Errorf("failed to create autopilot rule: %v. Err: %v", apRule.Name, err)
		}
		return aRule, false, nil
	}
	apRuleObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}
	apRuleObjString, _ := json.MarshalIndent(apRuleObj, "", "\t")
	log.Infof("Created autopilot rule: %s", apRuleObjString)

	return apRuleObj.(*apapi.AutopilotRule), nil
}

// GetAutopilotRule gets the AutopilotRule for the provided name
func (k *K8s) GetAutopilotRule(name string) (*apapi.AutopilotRule, error) {
	return k8sAutopilot.GetAutopilotRule(name)
}

// UpdateAutopilotRule updates the AutopilotRule
func (k *K8s) UpdateAutopilotRule(apRule *apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	return k8sAutopilot.UpdateAutopilotRule(apRule)
}

// ListAutopilotRules lists AutopilotRules
func (k *K8s) ListAutopilotRules() (*apapi.AutopilotRuleList, error) {
	return k8sAutopilot.ListAutopilotRules()
}

// DeleteAutopilotRule deletes the AutopilotRule of the given name
func (k *K8s) DeleteAutopilotRule(name string) error {
	return k8sAutopilot.DeleteAutopilotRule(name)
}

// GetActionApproval gets the ActionApproval for the provided name
func (k *K8s) GetActionApproval(namespace, name string) (*apapi.ActionApproval, error) {
	return k8sAutopilot.GetActionApproval(namespace, name)
}

// UpdateActionApproval updates the ActionApproval
func (k *K8s) UpdateActionApproval(namespace string, actionApproval *apapi.ActionApproval) (*apapi.ActionApproval, error) {
	return k8sAutopilot.UpdateActionApproval(namespace, actionApproval)
}

// DeleteActionApproval deletes the ActionApproval of the given name
func (k *K8s) DeleteActionApproval(namespace, name string) error {
	return k8sAutopilot.DeleteActionApproval(namespace, name)
}

// ListActionApprovals lists ActionApproval
func (k *K8s) ListActionApprovals(namespace string) (*apapi.ActionApprovalList, error) {
	return k8sAutopilot.ListActionApprovals(namespace)
}

func (k *K8s) isRollingDeleteStrategyEnabled(ctx *scheduler.Context) bool {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if rollDelStrategyAnnotationValue, ok := obj.Annotations[deleteStrategyAnnotationKey]; ok {
				if rollDelStrategyAnnotationValue == "rolling" {
					return true
				}
			}
		}
	}
	return false
}

// UpgradeScheduler upgrades the scheduler on the cluster to the specified version
func (k *K8s) UpgradeScheduler(version string) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpgradeScheduler()",
	}
}

// DeleteNode deletes the given in the cluster
func (k *K8s) DeleteNode(node node.Node) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteNode()",
	}
}

// DeleteSecret deletes secret with given name in given namespace
func (k *K8s) DeleteSecret(namespace, name string) error {
	return k8sCore.DeleteSecret(name, namespace)
}

// GetSecretData returns secret with given name in given namespace
func (k *K8s) GetSecretData(namespace, name, dataField string) (string, error) {
	secret, err := k8sCore.GetSecret(name, namespace)
	if err != nil {
		return "", err
	}
	return string(secret.Data[dataField]), nil
}

// CreateSecret creates new secret with given name in given namespace
func (k *K8s) CreateSecret(namespace, name, dataField, secretDataString string) error {
	meta := &metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}
	secretData := map[string]string{
		dataField: secretDataString,
	}
	secret := &corev1.Secret{
		ObjectMeta: *meta,
		StringData: secretData,
	}

	_, err := k8sCore.CreateSecret(secret)
	return err
}

// ScaleCluster scale the cluster to the given replicas
func (k *K8s) ScaleCluster(replicas int) error {
	// ScaleCluster is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ScaleCluster()",
	}
}

func (k *K8s) GetZones() ([]string, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetZones()",
	}
}

func (k *K8s) GetASGClusterSize() (int64, error) {
	// TODO: Add implementation
	return 0, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetASGClusterSize()",
	}
}

func (k *K8s) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// ScaleCluster is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetASGClusterSize()",
	}
}

// CreateCsiSnapsForVolumes create csi snapshots for Apps
func (k *K8s) CreateCsiSnapsForVolumes(ctx *scheduler.Context, snapClass string) (map[string]*volsnapv1.VolumeSnapshot, error) {
	// Only FA (pure_block) volume is supported
	volTypes := []string{PureBlock}
	var volSnapMap = make(map[string]*volsnapv1.VolumeSnapshot)

	for _, specObj := range ctx.App.SpecList {

		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvc, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			snapshotOkay, err := k.filterPureTypeVolumeIfEnabled(pvc, volTypes)
			if err != nil {
				return nil, err
			}
			if snapshotOkay {
				snapName := "snap-" + pvc.Name + "-" + strconv.Itoa(int(time.Now().Unix()))
				log.Debugf("Creating snapshot: [%s] for pvc: %s", snapName, pvc.Name)
				volSnapshot, err := k.CreateCsiSnapshot(snapName, obj.Namespace, snapClass, pvc.Name)
				if err != nil {
					return nil, err
				}
				log.Infof("Successfully created snapshot: [%s] for pvc: %s", volSnapshot.Name, pvc.Name)
				volSnapMap[pvc.Spec.VolumeName] = volSnapshot
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToCreateCsiSnapshots{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToCreateCsiSnapshots{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				snapshotOkay, err := k.filterPureTypeVolumeIfEnabled(&pvc, volTypes)
				if err != nil {
					return nil, err
				}
				if snapshotOkay {
					snapName := "snap-" + pvc.Name + "-" + strconv.Itoa(int(time.Now().Unix()))
					log.Debugf("Creating snapshot: [%s] for pvc: %s", snapName, pvc.Name)
					volSnapshot, err := k.CreateCsiSnapshot(snapName, obj.Namespace, snapClass, pvc.Name)
					if err != nil {
						return nil, err
					}
					log.Infof("Successfully created snapshot: [%s] for pvc: %s", volSnapshot.Name, pvc.Name)
					volSnapMap[pvc.Spec.VolumeName] = volSnapshot
				}
			}

		}
	}

	return volSnapMap, nil
}

// CSISnapshotTest create CSI snapshot for volumes and restore them to new PVCs, and validate the content
// (Testrail cases C58775, 58091)
func (k *K8s) CSISnapshotTest(ctx *scheduler.Context, request scheduler.CSISnapshotRequest) error {
	// This test will validate the content of the volume as opposed to just verify creation of volume.

	pvcObj, err := k8sCore.GetPersistentVolumeClaim(request.OriginalPVCName, request.Namespace)
	size := pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]
	log.Infof("Size of original PVC in snapshot test is %v", size)

	if err != nil {
		return fmt.Errorf("failed to retrieve PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}
	originalStorageClass, err := k8sCore.GetStorageClassForPVC(pvcObj)
	if err != nil {
		return fmt.Errorf("failed to retrieve storage class for PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}
	//PWX-32475:- Creating a clone of the original SC so that we can change the VolumeBindingMode to Immediate without changing the original storageclass spec
	storageClassName, err := createClonedStorageClassIfRequired(originalStorageClass)
	if err != nil {
		return fmt.Errorf("failed to create cloned storage class with the name %s : %s", originalStorageClass.Name+"-clone", err)
	}

	log.Infof("Proceeding with cloned storage class %s", storageClassName)

	podsUsingPVC, err := k8sCore.GetPodsUsingPVC(pvcObj.GetName(), pvcObj.GetNamespace())
	if err != nil {
		return fmt.Errorf("failed to retrieve pods using PVC %s/%s", pvcObj.GetName(), pvcObj.GetNamespace())
	}
	pod := podsUsingPVC[0]
	mountPath, _ := pureutils.GetAppDataDir(podsUsingPVC[0].Namespace)
	dirtyData := "thisIsJustSomeRandomText"
	snapName := request.SnapName
	for i := 0; i < 3; i++ {
		if *pvcObj.Spec.VolumeMode == corev1.PersistentVolumeBlock {
			data := "this is pure volume rawblock test data"
			err = k.writeRawBlockDataToPod(data, pod.GetName(), pod.GetNamespace(), mountPath)
			if err != nil {
				return fmt.Errorf("failed to write data to restored PVC: %s", err)
			}
			err = k.snapshotAndVerify(size, data, fmt.Sprint(snapName, i), pod.GetNamespace(), storageClassName, request.SnapshotclassName, fmt.Sprint(request.RestoredPVCName, i), request.OriginalPVCName)
			if err != nil {
				return fmt.Errorf("failed to validate restored PVC content: %s ", err)
			}
		} else {
			data := fmt.Sprint(dirtyData, strconv.Itoa(int(time.Now().Unix())))
			err = k.writeDataToPod(data, pod.GetName(), pod.GetNamespace(), mountPath)
			if err != nil {
				return fmt.Errorf("failed to write data to restored PVC: %s", err)
			}
			err = k.snapshotAndVerify(size, data, fmt.Sprint(snapName, i), pod.GetNamespace(), storageClassName, request.SnapshotclassName, fmt.Sprint(request.RestoredPVCName, i), request.OriginalPVCName)
			if err != nil {
				return fmt.Errorf("failed to validate restored PVC content: %s ", err)
			}
		}
	}

	return nil
}

// CSICloneTest create new PVC by cloning an existing PVC and make sure the contents of volume are same
// (Testrail cases C58509)
func (k *K8s) CSICloneTest(ctx *scheduler.Context, request scheduler.CSICloneRequest) error {
	// This test will validate the content of the volume as opposed to just verify creation of volume.
	pvcObj, err := k8sCore.GetPersistentVolumeClaim(request.OriginalPVCName, request.Namespace)
	if err != nil {
		return fmt.Errorf("failed to retrieve PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}
	size := pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]
	log.Infof("Size of original PVC in clone test is %v", size)
	originalStorageClass, err := k8sCore.GetStorageClassForPVC(pvcObj)
	if err != nil {
		return fmt.Errorf("failed to retrieve storage class for PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}

	//PWX-32475:- Creating a clone of the original SC so that we can change the VolumeBindingMode to Immediate without changing the original storageclass spec
	storageClassName, err := createClonedStorageClassIfRequired(originalStorageClass)
	if err != nil {
		return fmt.Errorf("failed to create cloned storage class with the name %s : %s", originalStorageClass.Name+"-clone", err)
	}
	log.Infof("Proceeding with cloned storage class %s", storageClassName)

	podsUsingPVC, err := k8sCore.GetPodsUsingPVC(pvcObj.GetName(), pvcObj.GetNamespace())
	if err != nil {
		return fmt.Errorf("failed to retrieve pods using PVC %s/%s", pvcObj.GetName(), pvcObj.GetNamespace())
	}
	pod := podsUsingPVC[0]
	mountPath, _ := pureutils.GetAppDataDir(podsUsingPVC[0].Namespace)
	dirtyData := "thisIsJustSomeRandomText"

	for i := 0; i < 3; i++ {
		if *pvcObj.Spec.VolumeMode == corev1.PersistentVolumeBlock {
			data := "this is pure volume rawblock test data"
			err = k.writeRawBlockDataToPod(data, pod.GetName(), pod.GetNamespace(), mountPath)
			if err != nil {
				return fmt.Errorf("failed to write data to cloned PVC: %s", err)
			}
			err = k.cloneAndVerify(size, data, pod.GetNamespace(), storageClassName, fmt.Sprint(request.RestoredPVCName, i), request.OriginalPVCName)
			if err != nil {
				return fmt.Errorf("failed to validate cloned PVC content: %s ", err)
			}
		} else {
			data := fmt.Sprint(dirtyData, strconv.Itoa(int(time.Now().Unix())))
			podCmd := fmt.Sprintf("touch %s/aaaa.txt", mountPath)
			cmdArgs := []string{"/bin/bash", "-c", podCmd}
			_, err := k8sCore.RunCommandInPod(cmdArgs, pod.GetName(), "", pod.GetNamespace())
			if err != nil {
				return fmt.Errorf("failed to execute command to Pod: %s", err)
			}
			err = k.writeDataToPod(data, pod.GetName(), pod.GetNamespace(), mountPath)
			if err != nil {
				return fmt.Errorf("failed to write data to cloned PVC: %s", err)
			}
			err = k.cloneAndVerify(size, data, pod.GetNamespace(), storageClassName, fmt.Sprint(request.RestoredPVCName, i), request.OriginalPVCName)
			if err != nil {
				return fmt.Errorf("failed to validate cloned PVC content: %s ", err)
			}
		}
	}

	return nil
}

// CSISnapshotAndRestoreMany create CSI snapshot and restore to many PVCs, and we validate the PVCs are up and bound
// (Testrail cases C58775, 58091)
func (k *K8s) CSISnapshotAndRestoreMany(ctx *scheduler.Context, request scheduler.CSISnapshotRequest) error {
	// This test will validate the content of the volume as opposed to just verify creation of volume.
	if !k.RunCSISnapshotAndRestoreManyTest {
		log.Info("RunCSISnapshotAndRestoreManyTest job disabled, skipping")
		return nil
	}
	pvcObj, err := k8sCore.GetPersistentVolumeClaim(request.OriginalPVCName, request.Namespace)
	size := pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]
	log.Infof("Size of original PVC in snapshot/restore test is %v", size)

	if err != nil {
		return fmt.Errorf("failed to retrieve PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}
	originalStorageClass, err := k8sCore.GetStorageClassForPVC(pvcObj)
	if err != nil {
		return fmt.Errorf("failed to retrieve storage class for PVC %s in namespace: %s : %s", request.OriginalPVCName, request.Namespace, err)
	}
	storageClassName := originalStorageClass.Name
	log.Infof("Proceeding with storage class %s", storageClassName)

	// creating the snapshot
	volSnapshot, err := k.CreateCsiSnapshot(request.SnapName, pvcObj.Namespace, request.SnapshotclassName, pvcObj.Name)
	if err != nil {
		return fmt.Errorf("failed to create snapshot %s for volume %s", request.SnapName, pvcObj.Name)
	}

	log.Infof("Successfully created snapshot: [%s] for pvc: %s", volSnapshot.Name, pvcObj.Name)
	for i := 0; i < numOfRestoredPVCForCloneManyTest; i++ {
		restoredPVCName := fmt.Sprint(request.RestoredPVCName, i)
		restoredPVCSpec, err := GeneratePVCRestoreSpec(size, pvcObj.Namespace, restoredPVCName, volSnapshot.Name, storageClassName)
		if err != nil {
			return fmt.Errorf("failed to build restored PVC Spec: %s", err)
		}
		_, err = k8sCore.CreatePersistentVolumeClaim(restoredPVCSpec)
		if err != nil {
			return fmt.Errorf("failed to restore PVC from snapshot %s: %s", volSnapshot.Name, err)
		}

	}
	log.Info("Finished issuing PVC creation request, proceed to validate")

	if err = k.waitForRestoredPVCsToBound(request.RestoredPVCName, pvcObj.Namespace, numOfRestoredPVCForCloneManyTest); err != nil {
		return fmt.Errorf("%d PVCs did not go into bound after 30 mins: %v", numOfRestoredPVCForCloneManyTest, err)
	}

	return nil
}

func (k *K8s) readRawBlockDataFromPod(podName, podNamespace, devicePath string) (string, error) {
	ddCmd := fmt.Sprintf("dd if=%s status=none bs=38 count=1 skip=0", devicePath)
	cmdArgs := []string{"/bin/sh", "-c", ddCmd}
	fileContent, err := k8sCore.RunCommandInPod(cmdArgs, podName, "", podNamespace)
	return fileContent, err
}

func (k *K8s) readDataFromPod(podName, podNamespace, mountFilePath string) (string, error) {
	cmdArgs := []string{"exec", "-it", podName, "-n", podNamespace, "--", "bin/cat", mountFilePath}
	command := exec.Command("kubectl", cmdArgs...)
	fileContent, err := command.CombinedOutput()
	return string(fileContent), err
}

func (k *K8s) writeRawBlockDataToPod(data, podName, podNamespace, devicePath string) error {
	var bsSize int = 38
	tmpFilePath := "/tmp/test.txt"

	podCmd := fmt.Sprintf("echo -n \"%s\" >> %s", data, "/tmp/test.txt")
	cmdArgs := []string{"/bin/bash", "-c", podCmd}
	_, err := k8sCore.RunCommandInPod(cmdArgs, podName, "", podNamespace)
	if err != nil {
		return fmt.Errorf("failed to execute command to Pod: %s", err)
	}
	// copy DATA_SIZE data to the device path of rawblock
	ddCopy := "dd if=%s of=%s bs=%d count=1 seek=0"
	ddCopyCmd := fmt.Sprintf(ddCopy, tmpFilePath, devicePath, bsSize)
	ddCopyCmdArgs := []string{"/bin/bash", "-c", ddCopyCmd}
	_, err = k8sCore.RunCommandInPod(ddCopyCmdArgs, podName, "", podNamespace)
	if err != nil {
		return fmt.Errorf("failed to execute command to Pod: %s", err)
	}
	// Sync the data, wait 20 secs and then proceed to snapshot the volume
	cmdArgs2 := []string{"exec", "-it", podName, "-n", podNamespace, "--", "/bin/sync"}
	command2 := exec.Command("kubectl", cmdArgs2...)
	out, err := command2.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run 'sync' in pod: %s. Output: %s", err, string(out))
	}
	fmt.Println("Sleep for 20 secs to let data write through")
	time.Sleep(time.Second * 20)
	return nil
}

func (k *K8s) writeDataToPod(data, podName, podNamespace, mountPath string) error {
	cmdArgs := []string{"exec", "-it", podName, "-n", podNamespace, "--", "/bin/sh", "-c", fmt.Sprintf("echo -n %s >>  %s/aaaa.txt", data, mountPath)}
	command := exec.Command("kubectl", cmdArgs...)
	out, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to write data to pod: %s. Output: %s", err, string(out))
	}
	// Sync the data, wait 20 secs and then proceed to snapshot the volume
	cmdArgs2 := []string{"exec", "-it", podName, "-n", podNamespace, "--", "/bin/sync"}
	command2 := exec.Command("kubectl", cmdArgs2...)
	out, err = command2.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run 'sync' in pod: %s. Output: %s", err, string(out))
	}
	fmt.Println("Sleep for 20 secs to let data write through")
	time.Sleep(time.Second * 20)
	return nil
}

// snapshotAndVerify takes an existing PVC mounted to a new pod, snapshot the PVC and mount the restored volume to a new pod, returns an error if
// the restored PVC doesn't contain the file content of the original PVC
func (k *K8s) snapshotAndVerify(size resource.Quantity, data, snapName, namespace, storageClass, snapClass, restoredPVCName, originalPVC string) error {
	clientset, err := k.getKubeClient("")
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}
	originalPVCObj, err := k8sCore.GetPersistentVolumeClaim(originalPVC, namespace)
	if err != nil {
		return fmt.Errorf("error getting original PVC: %s", originalPVC)
	}

	// creating the snapshot
	volSnapshot, err := k.CreateCsiSnapshot(snapName, namespace, snapClass, originalPVC)
	if err != nil {
		return fmt.Errorf("failed to create snapshot %s for volume %s", snapName, originalPVC)
	}

	log.Infof("Successfully created snapshot: [%s] for pvc: %s", volSnapshot.Name, originalPVC)
	restoredPVCSpec, err := GeneratePVCRestoreSpec(size, namespace, restoredPVCName, volSnapshot.Name, storageClass)
	if *originalPVCObj.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		restoredPVCSpec.Spec.VolumeMode = originalPVCObj.Spec.VolumeMode
	}
	if err != nil {
		return fmt.Errorf("failed to build restored PVC Spec: %s", err)
	}
	log.Infof("Size of restored PVC in snapshot test is %v", restoredPVCSpec.Spec.Resources.Requests[corev1.ResourceStorage])
	restoredPVC, err := k8sCore.CreatePersistentVolumeClaim(restoredPVCSpec)
	if err != nil {
		return fmt.Errorf("failed to restore PVC from snapshot %s: %s", volSnapshot.Name, err)
	}

	// Wait for PVC to be bound
	err = k.WaitForSinglePVCToBound(restoredPVCName, restoredPVC.Namespace, 0)
	if err != nil {
		return fmt.Errorf("failed to wait for cloned PVC %s to bind: %v", restoredPVCName, err)
	}

	log.Infof("Successfully restored PVC %s, proceed to mount to a new pod", restoredPVC.Name)
	restoredPodSpec := MakePod(namespace, []*v1.PersistentVolumeClaim{restoredPVC}, "ls", false)
	restoredPod, err := clientset.CoreV1().Pods(namespace).Create(context.TODO(), restoredPodSpec, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("error creating restored pod: %s", err)
	}

	if err = k.waitForPodToBeReady(restoredPod.Name, namespace); err != nil {
		return &scheduler.ErrFailedToSchedulePod{
			App:   nil,
			Cause: fmt.Sprintf("restored pod is not ready. Error: %v", err),
		}
	}
	mountPath, _ := pureutils.GetAppDataDir(restoredPod.Namespace)
	// Run a cat command from within the pod to verify the content of dirtydata
	if *restoredPVC.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		fileContent, err := k.readRawBlockDataFromPod(restoredPod.GetName(), restoredPod.GetNamespace(), mountPath)
		if err != nil {
			return fmt.Errorf("error checking content of cloned PVC: %s. Output: %s", err, string(fileContent))
		}
		if data != fileContent {
			return fmt.Errorf("compared data of text file & data copied to device path is not same")
		}
	} else {
		fileContent, err := k.readDataFromPod(restoredPod.Name, namespace, "/mnt/volume1/aaaa.txt")
		if err != nil {
			return fmt.Errorf("error checking content of restored PVC: %s. Output: %s", err, string(fileContent))
		}
		if !strings.Contains(fileContent, data) {
			return fmt.Errorf("restored volume does NOT contain data from original volume: expected to contain '%s', got '%s'", data, string(fileContent))
		}
	}

	log.Info("Validation complete, deleting restored pods")
	err = clientset.CoreV1().Pods(namespace).Delete(context.TODO(), restoredPod.Name, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("error deleting restored pod: %s", err)
	}

	// Wait for the pod to be actually gone, checking by pod UID in case it gets recreated (deployment in the future?)
	t := func() (interface{}, bool, error) {
		existingPods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return nil, true, fmt.Errorf("failed to list pods in ns %s. Error: %v", namespace, err)
		}

		for _, pod := range existingPods.Items {
			if pod.UID == restoredPod.UID {
				return nil, true, fmt.Errorf("pod %s in ns %s still exists", restoredPod.Name, restoredPod.Namespace)
			}
		}
		return nil, false, nil // Pod is now successfully gone
	}

	if _, err := task.DoRetryWithTimeout(t, time.Minute*2, DefaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// cloneAndVerify takes an existing PVC mounted to a pod, clones the PVC and mount it to a new pod, returns an error if
// the cloned PVC doesn't contain the file content of the original PVC
func (k *K8s) cloneAndVerify(size resource.Quantity, data, namespace, storageClass, clonedPVCName, originalPVC string) error {
	clientset, err := k.getKubeClient("")
	if err != nil {
		return fmt.Errorf("failed to get kube client: %s", err)
	}

	clonedPVCSpec, err := GeneratePVCCloneSpec(size, namespace, clonedPVCName, originalPVC, storageClass)
	if err != nil {
		return fmt.Errorf("failed to build cloned PVC Spec: %s", err)
	}
	log.Infof("Size of restored PVC in clone test is %v", clonedPVCSpec.Spec.Resources.Requests[corev1.ResourceStorage])
	clonedPVC, err := k8sCore.CreatePersistentVolumeClaim(clonedPVCSpec)
	if err != nil {
		return fmt.Errorf("failed to clone PVC from source PVC %s: %s", originalPVC, err)
	}

	// Wait for PVC to be bound
	err = k.WaitForSinglePVCToBound(clonedPVCName, clonedPVC.Namespace, 0)
	if err != nil {
		return fmt.Errorf("failed to wait for cloned PVC %s to bind: %v", clonedPVCName, err)
	}

	log.Infof("Successfully created cloned PVC %s, proceed to mount to a new pod", clonedPVC.Name)
	restoredPodSpec := MakePod(namespace, []*v1.PersistentVolumeClaim{clonedPVC}, "ls", false)
	restoredPod, err := clientset.CoreV1().Pods(namespace).Create(context.TODO(), restoredPodSpec, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("error creating restored pod: %s", err)
	}

	if err = k.waitForPodToBeReady(restoredPod.Name, namespace); err != nil {
		return &scheduler.ErrFailedToSchedulePod{
			App:   nil,
			Cause: fmt.Sprintf("restored pod is not ready. Error: %v", err),
		}
	}
	mountPath, _ := pureutils.GetAppDataDir(restoredPod.Namespace)
	// Run a cat command from within the pod to verify the content of dirtydata
	if *clonedPVC.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		fileContent, err := k.readRawBlockDataFromPod(restoredPod.GetName(), restoredPod.GetNamespace(), mountPath)
		if err != nil {
			return fmt.Errorf("error checking content of cloned PVC: %s. Output: %s", err, string(fileContent))
		}
		if data != fileContent {
			return fmt.Errorf("compared data of text file & data copied to device path is not same")
		}
	} else {
		fileContent, err := k.readDataFromPod(restoredPod.Name, namespace, "/mnt/volume1/aaaa.txt")
		if err != nil {
			return fmt.Errorf("error checking content of cloned PVC: %s. Output: %s", err, string(fileContent))
		}
		if !strings.Contains(fileContent, data) {
			return fmt.Errorf("cloned volume does NOT contain data from original volume: expected to contain '%s', got '%s'", data, string(fileContent))
		}
	}

	log.Info("Validation complete, deleting restored pods")
	err = clientset.CoreV1().Pods(namespace).Delete(context.TODO(), restoredPod.Name, metav1.DeleteOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("error deleting restored pod: %s", err)
	}

	// Wait for the pod to be actually gone
	t := func() (interface{}, bool, error) {
		var err error
		if _, err = clientset.CoreV1().Pods(namespace).Get(context.TODO(), restoredPod.Name, metav1.GetOptions{}); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil, false, nil // Pod is now successfully gone
			}
			return nil, true, fmt.Errorf("failed to check that pod %s in ns %s is gone. Err: %v", restoredPod.Name, restoredPod.Namespace, err)
		}
		return nil, true, fmt.Errorf("pod %s in ns %s still exists", restoredPod.Name, restoredPod.Namespace)
	}

	if _, err := task.DoRetryWithTimeout(t, time.Minute*2, DefaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// MakePod Returns a pod definition based on the namespace. The pod references the PVC's
// name.  A slice of BASH commands can be supplied as args to be run by the pod
func MakePod(ns string, pvclaims []*v1.PersistentVolumeClaim, command string, privileged bool) *v1.Pod {
	var cmd = "while true; do sleep 1; done"
	if 0 < len(command) {
		cmd = command + " && " + cmd
	}

	podSpec := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "testMountSnapshotToPod",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "testpod-",
			Namespace:    ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "test-pod",
					Image:           "alpine:3.10",
					Command:         []string{"/bin/sh", "-c"},
					Args:            []string{cmd},
					SecurityContext: &v1.SecurityContext{Privileged: &privileged},
				},
			},
			RestartPolicy: v1.RestartPolicyOnFailure,
		},
	}
	var volumeMounts = make([]v1.VolumeMount, 0)
	var volumeDevices = make([]v1.VolumeDevice, 0)
	var volumes = make([]v1.Volume, len(pvclaims))
	for index, pvclaim := range pvclaims {
		if pvclaim.Spec.VolumeMode != nil {
			fmt.Printf("mount pvc %s as %s\n", pvclaim.Name, *pvclaim.Spec.VolumeMode)
		} else {
			fmt.Printf("mount pvc %s, volume mode is nil\n", pvclaim.Name)
		}
		volumename := fmt.Sprintf("volume%v", index+1)

		if pvclaim.Spec.VolumeMode == nil || *pvclaim.Spec.VolumeMode == v1.PersistentVolumeFilesystem {
			volumeMounts = append(volumeMounts, v1.VolumeMount{Name: volumename, MountPath: "/mnt/" + volumename})
			volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
		} else {
			// Raw block device
			volumeDevices = append(volumeDevices, v1.VolumeDevice{Name: volumename, DevicePath: "/dev/xvda"})
			volumes[index] = v1.Volume{Name: volumename, VolumeSource: v1.VolumeSource{PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvclaim.Name, ReadOnly: false}}}
		}
	}
	podSpec.Spec.Containers[0].VolumeMounts = volumeMounts
	podSpec.Spec.Containers[0].VolumeDevices = volumeDevices
	podSpec.Spec.Volumes = volumes
	return podSpec
}

// MakePVC takes namespace, name and storageclass as parameter and returns a PersistentVolumeClaim spec for PVC creation
func MakePVC(size resource.Quantity, ns string, name string, storageClass string) *v1.PersistentVolumeClaim {
	PVCSpec := &v1.PersistentVolumeClaim{
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: size,
				},
			},
			StorageClassName: &storageClass,
		},
	}
	PVCSpec.ObjectMeta.Name = name
	PVCSpec.Namespace = ns
	PVCSpec.ObjectMeta.Namespace = ns
	return PVCSpec
}

// GeneratePVCRestoreSpec takes namespace, name, source snapshot name and storageclass as parameter and returns a PersistentVolumeClaim spec for PVC creation
func GeneratePVCRestoreSpec(size resource.Quantity, ns string, name string, sourceSnapshotName string, storageClass string) (*v1.PersistentVolumeClaim, error) {
	snapshotAPIGroup := "snapshot.storage.k8s.io"
	PVCSpec := MakePVC(size, ns, name, storageClass)
	PVCSpec.ObjectMeta.Name = name
	PVCSpec.Namespace = ns
	PVCSpec.ObjectMeta.Namespace = ns
	if sourceSnapshotName == "" {
		return nil, fmt.Errorf("source snapshot name is empty for PVC restoring request")
	}

	PVCSpec.Spec.DataSource = &v1.TypedLocalObjectReference{
		APIGroup: &snapshotAPIGroup,
		Kind:     "VolumeSnapshot",
		Name:     sourceSnapshotName,
	}

	return PVCSpec, nil
}

// GeneratePVCCloneSpec takes namespace, name, source snapshot name and storageclass as parameter and returns a PersistentVolumeClaim spec for PVC creation
func GeneratePVCCloneSpec(size resource.Quantity, ns string, name string, sourcePVCName string, storageClass string) (*v1.PersistentVolumeClaim, error) {
	pvcObj, err := k8sCore.GetPersistentVolumeClaim(sourcePVCName, ns)
	if err != nil {
		return nil, fmt.Errorf("error getting PVC: %s", name)
	}
	PVCSpec := MakePVC(size, ns, name, storageClass)
	PVCSpec.ObjectMeta.Name = name
	PVCSpec.Namespace = ns
	PVCSpec.ObjectMeta.Namespace = ns
	PVCSpec.Spec.VolumeMode = pvcObj.Spec.VolumeMode
	if sourcePVCName == "" {
		return nil, fmt.Errorf("source PVC name is empty for PVC cloning request")
	}

	PVCSpec.Spec.DataSource = &v1.TypedLocalObjectReference{
		Kind: "PersistentVolumeClaim",
		Name: sourcePVCName,
	}
	return PVCSpec, nil
}

// DeleteCsiSnapsForVolumes delete csi snapshots for Apps
func (k *K8s) DeleteCsiSnapsForVolumes(ctx *scheduler.Context, retainCount int) error {
	// Only FA (pure_block) volume is supported
	volTypes := []string{PureBlock}

	for _, specObj := range ctx.App.SpecList {

		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvc, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			snapshotOkay, err := k.filterPureTypeVolumeIfEnabled(pvc, volTypes)
			if err != nil {
				return err
			}
			if snapshotOkay {
				snaplistForDelete, err := k.GetCsiSnapshots(obj.Namespace, pvc.Name)
				if err != nil {
					return &scheduler.ErrFailedToGetSnapshotList{
						Name:  obj.Namespace,
						Cause: fmt.Errorf("failed to list csi snapshot in namespace: %v. Err: %v", obj.Namespace, err),
					}

				}
				log.Infof("Current [%v] snapshot exist for [%v] pvc", len(snaplistForDelete), pvc.Name)
				if len(snaplistForDelete) > retainCount {
					resVolIndex := random.Intn(len(snaplistForDelete))
					log.Infof("Deleting snapshot: [%v] for pvc: [%v]", snaplistForDelete[resVolIndex].Name, pvc.Name)
					err = k.DeleteCsiSnapshot(ctx, snaplistForDelete[resVolIndex].Name, obj.Namespace)
					if err != nil {
						return err
					}
				}
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToDeleteSnapshot{
					Name:  obj.Namespace,
					Cause: fmt.Errorf("failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return &scheduler.ErrFailedToDeleteSnapshot{
					Name:  obj.Namespace,
					Cause: fmt.Errorf("failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				snapshotOkay, err := k.filterPureTypeVolumeIfEnabled(&pvc, volTypes)
				if err != nil {
					return err
				}
				if snapshotOkay {
					snaplistFromStatefulset, err := k.GetCsiSnapshots(obj.Namespace, pvc.Name)
					if err != nil {
						return &scheduler.ErrFailedToGetSnapshotList{
							Name:  obj.Namespace,
							Cause: fmt.Errorf("failed to list csi snapshot in namespace: %v. Err: %v", obj.Namespace, err),
						}

					}
					log.Infof("Current [%v] snapshot exist for [%v] pvc", len(snaplistFromStatefulset), pvc.Name)
					if len(snaplistFromStatefulset) > retainCount {
						resVolIndex := random.Intn(len(snaplistFromStatefulset))
						log.Infof("Deleting snapshot: [%v] for pvc: [%v]", snaplistFromStatefulset[resVolIndex].Name, pvc.Name)
						err = k.DeleteCsiSnapshot(ctx, snaplistFromStatefulset[resVolIndex].Name, obj.Namespace)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (k *K8s) restoreAndValidate(
	ctx *scheduler.Context, pvc *v1.PersistentVolumeClaim,
	namespace string, sc *storageapi.StorageClass,
) (*v1.PersistentVolumeClaim, error) {
	var resPvc *v1.PersistentVolumeClaim

	snaplist, err := k.GetCsiSnapshots(namespace, pvc.Name)
	if err != nil {
		return nil, err
	}
	if len(snaplist) == 0 {
		return nil, fmt.Errorf("no snapshot found for a PVC: [%s]", pvc.Name)
	}

	resVolIndex := random.Intn(len(snaplist))
	restorePVCName := pvc.Name + "-" + "restore"
	if resPvc, err = k.restoreCsiSnapshot(restorePVCName, *pvc, snaplist[resVolIndex], sc); err != nil {
		return nil, &scheduler.ErrFailedToRestore{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to restore snapshot: [%s]", snaplist[resVolIndex].Name),
		}
	}
	// Validate restored PVC
	if err = k.ValidateCsiRestore(resPvc.Name, namespace, DefaultTimeout); err != nil {
		return nil, &scheduler.ErrFailedToValidatePvcAfterRestore{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to validate after snapshot: [%s] restore", snaplist[resVolIndex].Name),
		}
	}
	log.Infof("Successfully restored pvc [%s] from snapshot [%s]", resPvc.Name, snaplist[resVolIndex].Name)

	return resPvc, nil
}

// RestoreCsiSnapAndValidate restore the snapshot and validate the PVC
func (k *K8s) RestoreCsiSnapAndValidate(ctx *scheduler.Context, scMap map[string]*storageapi.StorageClass) (map[string]v1.PersistentVolumeClaim, error) {
	var pvcToRestorePVCMap = make(map[string]v1.PersistentVolumeClaim)
	var pureBlkType = []string{PureBlock}
	for _, specObj := range ctx.App.SpecList {
		var resPvc *v1.PersistentVolumeClaim
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvc, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			restoreOkay, err := k.filterPureTypeVolumeIfEnabled(pvc, pureBlkType)
			if err != nil {
				return nil, err
			}
			if restoreOkay {
				if resPvc, err = k.restoreAndValidate(ctx, pvc, obj.Namespace, scMap[PureBlock]); err != nil {
					return nil, err
				}
			}
			pvcToRestorePVCMap[pvc.Name] = *resPvc
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToRestore{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToRestore{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				restoreOkay, err := k.filterPureTypeVolumeIfEnabled(&pvc, pureBlkType)
				if err != nil {
					return nil, err
				}
				if restoreOkay {
					if resPvc, err = k.restoreAndValidate(ctx, &pvc, obj.Namespace, scMap[PureBlock]); err != nil {
						return nil, err
					}
				}
				pvcToRestorePVCMap[pvc.Name] = *resPvc
			}
		}
	}
	return pvcToRestorePVCMap, nil

}

// ValidateCsiRestore validates the restored PVC from snapshot
func (k *K8s) ValidateCsiRestore(pvcName string, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		var pvc *v1.PersistentVolumeClaim
		var err error
		if pvc, err = k8sCore.GetPersistentVolumeClaim(pvcName, namespace); err != nil {
			return nil, true, &scheduler.ErrFailedToValidatePvc{
				Name:  pvcName,
				Cause: fmt.Errorf("failed to get persistent volume claim.Err: %v", err),
			}
		}
		if pvc.Status.Phase == v1.ClaimPending {
			return nil, true, &scheduler.ErrFailedToValidatePvc{
				Name:  pvcName,
				Cause: fmt.Errorf("PVC [%s] is not bound", pvcName),
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, DefaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// restoreCsiSnapshot restore PVC from csiSnapshot
func (k *K8s) restoreCsiSnapshot(
	restorePvcName string, pvc corev1.PersistentVolumeClaim,
	snap *volsnapv1.VolumeSnapshot, sc *storageapi.StorageClass,
) (*v1.PersistentVolumeClaim, error) {
	var resPvc *corev1.PersistentVolumeClaim
	var dataSource v1.TypedLocalObjectReference

	var err error

	v1obj := metav1.ObjectMeta{
		Name:      restorePvcName,
		Namespace: pvc.Namespace,
	}

	resList := make(map[v1.ResourceName]resource.Quantity)
	resList["storage"] = *snap.Status.RestoreSize

	resRequirements := corev1.ResourceRequirements{
		Requests: resList,
	}

	dataSource = v1.TypedLocalObjectReference{
		Kind:     VolumeSnapshotKind,
		Name:     snap.Name,
		APIGroup: &SnapshotAPIGroup,
	}

	restorePvcSpec := corev1.PersistentVolumeClaimSpec{
		AccessModes:      pvc.Spec.AccessModes,
		Resources:        resRequirements,
		DataSource:       &dataSource,
		StorageClassName: &sc.Name,
	}
	restorePVC := corev1.PersistentVolumeClaim{
		ObjectMeta: v1obj,
		Spec:       restorePvcSpec,
	}

	log.Infof("Restoring Snapshot: %v", restorePVC.Name)
	if resPvc, err = k8sCore.CreatePersistentVolumeClaim(&restorePVC); err != nil {
		return nil, err
	}
	return resPvc, nil
}

// CreateCsiSnapshotClass creates csi volume snapshot class
func (k *K8s) CreateCsiSnapshotClass(snapClassName string, deleionPolicy string) (*volsnapv1.VolumeSnapshotClass, error) {
	var err error
	var annotation = make(map[string]string)
	var volumeSnapClass *volsnapv1.VolumeSnapshotClass
	annotation["snapshot.storage.kubernetes.io/is-default-class"] = "true"

	v1obj := metav1.ObjectMeta{
		Name:        snapClassName,
		Annotations: annotation,
	}

	snapClass := volsnapv1.VolumeSnapshotClass{
		ObjectMeta:     v1obj,
		Driver:         CsiProvisioner,
		DeletionPolicy: volsnapv1.DeletionPolicy(deleionPolicy),
	}

	log.Infof("Creating volume snapshot class: %v", snapClassName)
	if volumeSnapClass, err = k8sExternalsnap.CreateSnapshotClass(&snapClass); err != nil {
		return nil, &scheduler.ErrFailedToCreateSnapshotClass{
			Name:  snapClassName,
			Cause: err,
		}
	}
	return volumeSnapClass, nil
}

// CreateVolumeSnapshotClasses creates a volume snapshot class
func (k *K8s) CreateVolumeSnapshotClasses(snapClassName string, provisioner string, isDefault bool, deletePolicy string) (*volsnapv1.VolumeSnapshotClass, error) {
	var err error
	var annotation = make(map[string]string)
	var volumeSnapClass *volsnapv1.VolumeSnapshotClass
	if isDefault {
		annotation["snapshot.storage.kubernetes.io/is-default-class"] = "true"
	} else {
		annotation = make(map[string]string)
	}

	v1obj := metav1.ObjectMeta{
		Name:        snapClassName,
		Annotations: annotation,
	}
	if len(deletePolicy) == 0 {
		deletePolicy = "Delete"
	}
	snapClass := volsnapv1.VolumeSnapshotClass{
		ObjectMeta:     v1obj,
		Driver:         provisioner,
		DeletionPolicy: volsnapv1.DeletionPolicy(deletePolicy),
	}

	log.Infof("Creating volume snapshot class: %v with provisioner %v", snapClassName, provisioner)
	if volumeSnapClass, err = k8sExternalsnap.CreateSnapshotClass(&snapClass); err != nil {
		return nil, &scheduler.ErrFailedToCreateSnapshotClass{
			Name:  snapClassName,
			Cause: err,
		}
	}
	return volumeSnapClass, nil
}

// CreateVolumeSnapshotClassesWithParameters creates a volume snapshot class with additional parameters
func (k *K8s) CreateVolumeSnapshotClassesWithParameters(snapClassName string, provisioner string, isDefault bool, deletePolicy string, parameters map[string]string) (*volsnapv1.VolumeSnapshotClass, error) {
	var err error
	var annotation = make(map[string]string)
	var volumeSnapClass *volsnapv1.VolumeSnapshotClass
	if isDefault {
		annotation["snapshot.storage.kubernetes.io/is-default-class"] = "true"
	} else {
		annotation = make(map[string]string)
	}

	v1obj := metav1.ObjectMeta{
		Name:        snapClassName,
		Annotations: annotation,
	}
	if len(deletePolicy) == 0 {
		deletePolicy = "Delete"
	}
	snapClass := volsnapv1.VolumeSnapshotClass{
		ObjectMeta:     v1obj,
		Driver:         provisioner,
		DeletionPolicy: volsnapv1.DeletionPolicy(deletePolicy),
		Parameters:     parameters,
	}

	log.Infof("Creating volume snapshot class: %v with provisioner %v", snapClassName, provisioner)
	if volumeSnapClass, err = k8sExternalsnap.CreateSnapshotClass(&snapClass); err != nil {
		return nil, &scheduler.ErrFailedToCreateSnapshotClass{
			Name:  snapClassName,
			Cause: err,
		}
	}
	return volumeSnapClass, nil
}

// waitForCsiSnapToBeReady wait for snapshot status to be ready
func (k *K8s) waitForCsiSnapToBeReady(snapName string, namespace string) error {
	var snap *volsnapv1.VolumeSnapshot
	var err error
	log.Infof("Waiting for snapshot [%s] to be ready in namespace: %s ", snapName, namespace)
	t := func() (interface{}, bool, error) {
		if snap, err = k8sExternalsnap.GetSnapshot(snapName, namespace); err != nil {
			return "", true, err
		}
		if snap.Status == nil || !*snap.Status.ReadyToUse {
			return "", true, &scheduler.ErrFailedToValidateSnapshot{
				Name:  snapName,
				Cause: fmt.Errorf("snapshot [%s] is not ready", snapName),
			}
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, SnapshotReadyTimeout, DefaultRetryInterval); err != nil {
		return err
	}
	log.Infof("Snapshot is ready to use: %s", snap.Name)
	return nil
}

// waitForCsiSnapToBeReady wait for snapshot status to be ready
func (k *K8s) waitForPodToBeReady(podname string, namespace string) error {
	var pod *v1.Pod
	var err error
	kubeClient, err := k.getKubeClient("")
	if err != nil {
		log.Error("Failed to get Kube client")
		return err
	}
	log.Infof("Waiting for pod [%s] to be ready in namespace: %s ", podname, namespace)
	t := func() (interface{}, bool, error) {
		if pod, err = kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), podname, metav1.GetOptions{}); err != nil {
			return "", true, err
		}
		if pod.Status.Phase != v1.PodRunning {
			return "", true, fmt.Errorf("Pod %s not up yet", podname)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, k8sPodCreateTimeout, DefaultRetryInterval); err != nil {
		return err
	}
	log.Infof("Pod is up and running: %s", pod.Name)
	return nil
}

// WaitForSinglePVCToBound retries and waits up to 30 minutes for a single PVC to be bound
func (k *K8s) WaitForSinglePVCToBound(pvcName, namespace string, timeout int) error {
	var pvc *v1.PersistentVolumeClaim
	var err error
	kubeClient, err := k.getKubeClient("")
	var timeLimit int
	timeLimit = 30
	if timeout != 0 && timeout < 60 {
		timeLimit = timeout
	}
	if err != nil {
		log.Error("Failed to get Kube client")
		return err
	}
	log.Infof("Waiting for pvc [%s] to be bound in namespace: %s ", pvcName, namespace)
	t := func() (interface{}, bool, error) {
		if pvc, err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), pvcName, metav1.GetOptions{}); err != nil {
			return "", true, err
		}
		if pvc.Status.Phase != v1.ClaimBound {
			return "", true, fmt.Errorf("PVC %s not bound yet", pvcName)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, time.Duration(timeLimit)*time.Minute, 30*time.Second); err != nil {
		return err
	}
	log.Infof("PVC is in bound: %s", pvc.Name)
	return nil
}

// waitForRestoredPVCsToBound retries and waits up to 30 minutes for all PVCs to be bound
func (k *K8s) waitForRestoredPVCsToBound(pvcNamePrefix string, namespace string, numPVCs int) error {
	var pvc *v1.PersistentVolumeClaim
	var err error
	kubeClient, err := k.getKubeClient("")
	if err != nil {
		log.Error("Failed to get Kube client")
		return err
	}
	log.Infof("Waiting for pvcs [%s] to be bound in namespace: %s ", pvcNamePrefix, namespace)
	t := func() (interface{}, bool, error) {
		for j := 0; j < numPVCs; j++ {
			restoredPVCName := fmt.Sprint(pvcNamePrefix, j)
			if pvc, err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), restoredPVCName, metav1.GetOptions{}); err != nil {
				return "", true, err
			}
			if pvc.Status.Phase != v1.ClaimBound {
				return "", true, fmt.Errorf("PVC %s not bound yet", pvcNamePrefix)
			}
		}
		return "", false, nil

	}
	if _, err := task.DoRetryWithTimeout(t, 30*time.Minute, 30*time.Second); err != nil {
		return err
	}
	log.Infof("PVC is in bound: %s", pvc.Name)
	return nil
}

// CreateCsiSnapshot create snapshot for given pvc
func (k *K8s) CreateCsiSnapshot(name string, namespace string, class string, pvc string) (*volsnapv1.VolumeSnapshot, error) {
	var err error
	var snapshot *volsnapv1.VolumeSnapshot

	v1obj := metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
	}

	source := volsnapv1.VolumeSnapshotSource{
		PersistentVolumeClaimName: &pvc,
	}

	spec := volsnapv1.VolumeSnapshotSpec{
		VolumeSnapshotClassName: &class,
		Source:                  source,
	}

	snap := volsnapv1.VolumeSnapshot{
		ObjectMeta: v1obj,
		Spec:       spec,
	}
	log.Infof("Creating snapshot : %v", name)
	if snapshot, err = k8sExternalsnap.CreateSnapshot(&snap); err != nil {
		return nil, &scheduler.ErrFailedToCreateSnapshot{
			PvcName: pvc,
			Cause:   err,
		}
	}
	if err = k.waitForCsiSnapToBeReady(snapshot.Name, namespace); err != nil {
		return nil, &scheduler.ErrFailedToCreateSnapshot{
			PvcName: pvc,
			Cause:   fmt.Errorf("snapshot is not ready. Error: %v", err),
		}
	}
	return snapshot, nil
}

// GetCsiSnapshots return snapshot list for a pvc
func (k *K8s) GetCsiSnapshots(namespace string, pvcName string) ([]*volsnapv1.VolumeSnapshot, error) {
	var snaplist *volsnapv1.VolumeSnapshotList
	var snap *volsnapv1.VolumeSnapshot
	var err error
	snapshots := make([]*volsnapv1.VolumeSnapshot, 0)

	if snaplist, err = k8sExternalsnap.ListSnapshots(namespace); err != nil {
		return nil, &scheduler.ErrFailedToGetSnapshotList{
			Name:  namespace,
			Cause: err,
		}
	}

	for _, snapshot := range snaplist.Items {
		if snap, err = k8sExternalsnap.GetSnapshot(snapshot.Name, namespace); err != nil {
			// Not returning error when it failed to get snapshot as snapshot could be deleting
			log.Warnf("Unable to get snapshot: [%v]. It could be deleting", snapshot.Name)
			continue
		}
		if strings.Compare(*snap.Spec.Source.PersistentVolumeClaimName, pvcName) == 0 {
			log.Infof("[%v] snapshot source pvc: [%v] matches with: [%v] pvc", snapshot.Name, *snap.Spec.Source.PersistentVolumeClaimName, pvcName)
			snapshots = append(snapshots, snap)
		}
	}
	return snapshots, nil
}

// ValidateCsiSnapshots validate all snapshots in the context
func (k *K8s) ValidateCsiSnapshots(ctx *scheduler.Context, volSnapMap map[string]*volsnapv1.VolumeSnapshot) error {
	var pureBlkType = []string{PureBlock}

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*corev1.PersistentVolumeClaim); ok {
			pvc, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			validateOkay, err := k.filterPureTypeVolumeIfEnabled(pvc, pureBlkType)
			if err != nil {
				return err
			}
			if validateOkay {
				if _, ok := volSnapMap[pvc.Spec.VolumeName]; !ok {
					return &scheduler.ErrFailedToValidateCsiSnapshots{
						App:   ctx.App,
						Cause: fmt.Sprintf("Snapshot is empty for a pvc: %v", pvc.Name),
					}
				}

				if err = k.validateCsiSnap(pvc.Name, obj.Namespace, *volSnapMap[pvc.Spec.VolumeName]); err != nil {
					return err
				}
			}
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateCsiSnapshots{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return &scheduler.ErrFailedToValidateCsiSnapshots{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				validateOkay, err := k.filterPureTypeVolumeIfEnabled(&pvc, pureBlkType)
				if err != nil {
					return err
				}
				if validateOkay {
					if _, ok := volSnapMap[pvc.Spec.VolumeName]; !ok {
						return &scheduler.ErrFailedToValidateCsiSnapshots{
							App:   ctx.App,
							Cause: fmt.Sprintf("Snapshot is empty for a pvc: %v", pvc.Name),
						}
					}
					if err = k.validateCsiSnap(pvc.Name, obj.Namespace, *volSnapMap[pvc.Spec.VolumeName]); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// validateCsiSnapshot validates the given snapshot is successfully created or not
func (k *K8s) validateCsiSnap(pvcName string, namespace string, csiSnapshot volsnapv1.VolumeSnapshot) error {
	var snap *volsnapv1.VolumeSnapshot
	var err error

	if csiSnapshot.Name == "" {
		return &scheduler.ErrFailedToValidateSnapshot{
			Name:  pvcName,
			Cause: fmt.Errorf("failed to validate snaps.Valid snapshot not provided for volume: %s", pvcName),
		}
	}

	if snap, err = k8sExternalsnap.GetSnapshot(csiSnapshot.Name, namespace); err != nil {
		return &scheduler.ErrFailedToValidateSnapshot{
			Name:  pvcName,
			Cause: fmt.Errorf("failed to get snapshot: %s", csiSnapshot.Name),
		}
	}

	// Checking if snapshot class matches with create storage class
	log.Debugf("VolumeSnapshotClassName in snapshot: %s", *snap.Spec.VolumeSnapshotClassName)
	if *snap.Spec.VolumeSnapshotClassName != *csiSnapshot.Spec.VolumeSnapshotClassName {
		return &scheduler.ErrFailedToValidateSnapshot{
			Name:  pvcName,
			Cause: fmt.Errorf("failed to validate snap: [%s].Snapshot class is not maching", snap.Name),
		}
	}

	log.Debugf("Validating the source PVC name in snapshot: %s", *snap.Spec.Source.PersistentVolumeClaimName)
	if *snap.Spec.Source.PersistentVolumeClaimName != pvcName {
		return &scheduler.ErrFailedToValidateSnapshot{
			Name:  pvcName,
			Cause: fmt.Errorf("failed to validate source pvc name in snapshot: %s", snap.Name),
		}
	}

	log.Infof("Successfully validated the snapshot %s", csiSnapshot.Name)
	return nil
}

// GetAllSnapshotClasses returns the list of all volume snapshot classes present in the cluster
func (k *K8s) GetAllSnapshotClasses() (*volsnapv1.VolumeSnapshotClassList, error) {
	var snapshotClasses *volsnapv1.VolumeSnapshotClassList
	var err error
	if snapshotClasses, err = k8sExternalsnap.ListSnapshotClasses(); err != nil {
		return nil, err
	}
	return snapshotClasses, nil
}

// GetPodsRestartCount return map of HostIP and it restart count in given namespace
func (k *K8s) GetPodsRestartCount(namespace string, podLabelMap map[string]string) (map[*v1.Pod]int32, error) {
	podRestartCountMap := make(map[*v1.Pod]int32)
	podList, err := k8sCore.GetPods(namespace, podLabelMap)
	if err != nil {
		return nil, err
	}
	for _, pod := range podList.Items {
		actualPod, err := k8sCore.GetPodByName(pod.Name, pod.Namespace)
		if err != nil {
			return nil, err
		}
		containerStatus := actualPod.Status.ContainerStatuses
		for _, status := range containerStatus {
			podRestartCountMap[actualPod] += status.RestartCount
		}
	}
	return podRestartCountMap, nil
}

func substituteImageWithInternalRegistry(spec interface{}) {
	internalDockerRegistry := os.Getenv("INTERNAL_DOCKER_REGISTRY")
	if internalDockerRegistry != "" {
		if obj, ok := spec.(*appsapi.DaemonSet); ok {
			modifyImageInContainers(obj.Spec.Template.Spec.InitContainers, internalDockerRegistry)
			modifyImageInContainers(obj.Spec.Template.Spec.Containers, internalDockerRegistry)
		}
		if obj, ok := spec.(*appsapi.Deployment); ok {
			modifyImageInContainers(obj.Spec.Template.Spec.InitContainers, internalDockerRegistry)
			modifyImageInContainers(obj.Spec.Template.Spec.Containers, internalDockerRegistry)
		}
		if obj, ok := spec.(*appsapi.StatefulSet); ok {
			modifyImageInContainers(obj.Spec.Template.Spec.InitContainers, internalDockerRegistry)
			modifyImageInContainers(obj.Spec.Template.Spec.Containers, internalDockerRegistry)
		}
		if obj, ok := spec.(*batchv1.Job); ok {
			modifyImageInContainers(obj.Spec.Template.Spec.InitContainers, internalDockerRegistry)
			modifyImageInContainers(obj.Spec.Template.Spec.Containers, internalDockerRegistry)
		}
		if obj, ok := spec.(*corev1.Pod); ok {
			modifyImageInContainers(obj.Spec.InitContainers, internalDockerRegistry)
			modifyImageInContainers(obj.Spec.Containers, internalDockerRegistry)
		}
	}
}

func modifyImageInContainers(containers []v1.Container, imageName string) {
	if containers != nil {
		for idx := range containers {
			containers[idx].Image = fmt.Sprintf("%s/%s", imageName, containers[idx].Image)
		}
	}
}

func (k *K8s) createDockerRegistrySecret(secretName, secretNamespace string) (*v1.Secret, error) {
	var auths = struct {
		AuthConfigs map[string]docker_types.AuthConfig `json:"auths"`
	}{}

	dockerServer := os.Getenv("IMAGE_PULL_SERVER")
	dockerUsername := os.Getenv("IMAGE_PULL_USERNAME")
	dockerPassword := os.Getenv("IMAGE_PULL_PASSWORD")

	if dockerServer != "" && dockerUsername != "" && dockerPassword != "" {
		auths.AuthConfigs = map[string]docker_types.AuthConfig{
			dockerServer: {
				Auth: base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", dockerUsername, dockerPassword))),
			},
		}
		authConfigsEnc, _ := json.Marshal(auths)

		secretObj := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: secretNamespace,
			},
			Type: v1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{".dockerconfigjson": authConfigsEnc},
		}
		secret, err := k8sCore.CreateSecret(secretObj)
		if k8serrors.IsAlreadyExists(err) {
			if secret, err = k8sCore.GetSecret(secretName, secretNamespace); err == nil {
				log.Infof("Using existing Docker registry secret: %v", secret.Name)
				return secret, nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create Docker registry secret: %s in namespace: %s . Err: %v", secretName, secretNamespace, err)
		}
		log.Infof("Created Docker registry secret: %s", secret.Name)
		return secret, nil
	}
	return nil, nil
}

func insertLineBreak(note string) string {
	return fmt.Sprintf("------------------------------\n%s\n------------------------------\n", note)
}

func dumpPodStatusRecursively(pod corev1.Pod) string {
	var buf bytes.Buffer
	buf.WriteString(insertLineBreak(fmt.Sprintf("Pod: [%s] %s", pod.Namespace, pod.Name)))
	buf.WriteString(fmt.Sprintf("%v\n", pod.Status))
	for _, conStat := range pod.Status.ContainerStatuses {
		buf.WriteString(insertLineBreak(fmt.Sprintf("container: %s", conStat.Name)))
		buf.WriteString(fmt.Sprintf("%v\n", conStat))
		buf.WriteString(insertLineBreak("END container"))
	}
	buf.WriteString(dumpEvents(pod.Namespace, "Pod", pod.Name))
	buf.WriteString(insertLineBreak("END pod"))
	return buf.String()
}

func dumpEvents(namespace, resourceType, name string) string {
	var buf bytes.Buffer
	fields := fmt.Sprintf("involvedObject.kind=%s,involvedObject.name=%s", resourceType, name)
	events, err := k8sCore.ListEvents(namespace, metav1.ListOptions{FieldSelector: fields})
	if err != nil {
		buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetEvents{
			Type:  resourceType,
			Name:  name,
			Cause: err.Error(),
		}))
	}
	buf.WriteString(insertLineBreak("Events:"))
	buf.WriteString(fmt.Sprintf("%v\n", events))
	return buf.String()
}

// getAffinity return Affinity spec
func getAffinity(topologyLabels []map[string]string) corev1.Affinity {
	var matchExpressions []corev1.NodeSelectorRequirement
	var nodeSelTerms []corev1.NodeSelectorTerm
	for key, value := range topologyLabels[0] {
		var values []string
		values = append(values, value)
		nodeSelecterReq := corev1.NodeSelectorRequirement{
			Key:      key,
			Operator: "In",
			Values:   values,
		}
		matchExpressions = append(matchExpressions, nodeSelecterReq)
	}
	nodeSelTerm := corev1.NodeSelectorTerm{
		MatchExpressions: matchExpressions,
	}
	nodeSelTerms = append(nodeSelTerms, nodeSelTerm)
	nodeSelector := corev1.NodeSelector{
		NodeSelectorTerms: nodeSelTerms,
	}
	nodeAff := corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &nodeSelector,
	}
	return corev1.Affinity{
		NodeAffinity: &nodeAff,
	}
}

// getLabelsFromNodeAffinit return Topology labels from appinity specs
func getLabelsFromNodeAffinity(nodeAffSpec *v1.NodeAffinity) map[string]string {
	var nodeSelTerms []corev1.NodeSelectorTerm
	var label = make(map[string]string)

	if nodeAffSpec.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		nodeSelTerms = nodeAffSpec.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		for _, nSelTerm := range nodeSelTerms {
			matchExpressions := nSelTerm.MatchExpressions
			for _, nSelReq := range matchExpressions {
				label[nSelReq.Key] = nSelReq.Values[0]
			}
		}
	}
	return label
}

// MergeMaps merges two maps
func MergeMaps(m1 map[string]string, m2 map[string]string) map[string]string {
	for k, v := range m2 {
		m1[k] = v
	}
	return m1
}

// AddNamespaceLabel adds a label key=value on the given namespace
func (k *K8s) AddNamespaceLabel(namespace string, labelMap map[string]string) error {
	ns, err := k8sCore.GetNamespace(namespace)
	if err != nil {
		return err
	}
	newLabels := MergeMaps(ns.Labels, labelMap)
	ns.SetLabels(newLabels)
	if _, err := k8sCore.UpdateNamespace(ns); err == nil {
		return nil
	}
	return err
}

// RemoveNamespaceLabel removes the label with key on given namespace
func (k *K8s) RemoveNamespaceLabel(namespace string, labelMap map[string]string) error {
	ns, err := k8sCore.GetNamespace(namespace)
	if err != nil {
		return err
	}
	for key := range labelMap {
		delete(ns.Labels, key)
	}
	if _, err = k8sCore.UpdateNamespace(ns); err == nil {
		return nil
	}
	return err
}

// GetNamespaceLabel gets the labels on given namespace
func (k *K8s) GetNamespaceLabel(namespace string) (map[string]string, error) {
	ns, err := k8sCore.GetNamespace(namespace)
	if err != nil {
		return nil, err
	}
	return ns.Labels, nil
}

// rotateTopologyArray Rotates topology arrays by one
func rotateTopologyArray(options *scheduler.ScheduleOptions) {
	if len(options.TopologyLabels) > 1 {
		arr := options.TopologyLabels
		firstElem := arr[0]
		arr = arr[1:]
		arr = append(arr, firstElem)
		options.TopologyLabels = arr
	}
}

func init() {
	k := &K8s{}
	scheduler.Register(SchedName, k)
}

// ClusterVersion returns the cluster version of the kubernetes cluster as a string (like "1.23.0")
func ClusterVersion() (string, error) {
	ver, err := k8sCore.GetVersion()
	if err != nil {
		return "", err
	}
	return strings.TrimLeft(ver.String(), "v"), nil
}

func createClonedStorageClassIfRequired(originalStorageClass *storageapi.StorageClass) (string, error) {
	cloneStorageClass := originalStorageClass.DeepCopy()
	clonedSCName := cloneStorageClass.Name + "-clone"
	clonedSCNotFound := false

	t := func() (interface{}, bool, error) {
		_, err := k8sStorage.GetStorageClass(clonedSCName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				clonedSCNotFound = true
				return "", false, err
			}
			return "", true, err
		}
		return "", false, nil
	}

	_, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second)
	if err != nil {
		if clonedSCNotFound {
			log.Infof("Cloned SC with the name %v does not exist and will be created", clonedSCName)

			cloneStorageClass.Name = clonedSCName
			immediate := storageapi.VolumeBindingImmediate
			cloneStorageClass.VolumeBindingMode = &immediate
			cloneStorageClass.ResourceVersion = ""
			cloneStorageClass.UID = ""

			clonedSCobj, err := k8sStorage.CreateStorageClass(cloneStorageClass)
			if err != nil {
				log.Errorf("Error occurred while creating cloned storage class:- %v", err)
				return "", err
			}
			return clonedSCobj.Name, nil
		} else {
			return "", err
		}
	}
	return clonedSCName, nil
}
