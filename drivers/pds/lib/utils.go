package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	"io/ioutil"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	pdsdriver "github.com/portworx/torpedo/drivers/pds"

	"github.com/libopenstorage/openstorage/api"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/tests"
	. "github.com/portworx/torpedo/tests"

	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"

	state "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	pdscontrolplane "github.com/portworx/torpedo/drivers/pds/controlplane"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type PDS_Health_Status string

type Parameter struct {
	DataServiceToTest []struct {
		Name          string `json:"Name"`
		Version       string `json:"Version"`
		Image         string `json:"Image"`
		Replicas      int    `json:"Replicas"`
		ScaleReplicas int    `json:"ScaleReplicas"`
		OldVersion    string `json:"OldVersion"`
		OldImage      string `json:"OldImage"`
	} `json:"DataServiceToTest"`
	InfraToTest struct {
		ControlPlaneURL      string `json:"ControlPlaneURL"`
		AccountName          string `json:"AccountName"`
		TenantName           string `json:"TenantName"`
		ProjectName          string `json:"ProjectName"`
		ClusterType          string `json:"ClusterType"`
		Namespace            string `json:"Namespace"`
		PxNamespace          string `json:"PxNamespace"`
		PDSNamespace         string `json:"PDSNamespace"`
		ServiceIdentityToken bool   `json:"ServiceIdentityToken"`
	} `json:"InfraToTest"`
	PDSHelmVersions struct {
		LatestHelmVersion   string `json:"LatestHelmVersion"`
		PreviousHelmVersion string `json:"PreviousHelmVersion"`
	} `json:"PDSHelmVersions"`
	Users struct {
		AdminUsername    string `json:"AdminUsername"`
		AdminPassword    string `json:"AdminPassword"`
		NonAdminUsername string `json:"NonAdminUsername"`
		NonAdminPassword string `json:"NonAdminPassword"`
	} `json:"Users"`
	ResiliencyTest struct {
		CheckTillReplica int32 `json:"CheckTillReplica"`
	} `json:"ResiliencyTest"`
}

// ResourceSettingTemplate struct used to store template values
type ResourceSettingTemplate struct {
	Resources struct {
		Limits struct {
			CPU    string `json:"cpu"`
			Memory string `json:"memory"`
		} `json:"limits"`
		Requests struct {
			CPU     string `json:"cpu"`
			Memory  string `json:"memory"`
			Storage string `json:"storage"`
		} `json:"requests"`
	} `json:"resources"`
}

// WorkloadGenerationParams has data service creds
type WorkloadGenerationParams struct {
	Host                         string
	User                         string
	Password                     string
	DataServiceName              string
	DeploymentName               string
	DeploymentID                 string
	ScaleFactor                  string
	Iterations                   string
	Namespace                    string
	UseSSL, VerifyCerts, TimeOut string
	Replicas                     int
}

// StorageOptions struct used to store template values
type StorageOptions struct {
	Filesystem  string
	ForceSpread string
	Replicas    int32
	VolumeGroup bool
}

type DBConfig struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Annotations struct {
			AdDatadoghqComElasticsearchCheckNames  string `yaml:"ad.datadoghq.com/elasticsearch.check_names"`
			AdDatadoghqComElasticsearchInitConfigs string `yaml:"ad.datadoghq.com/elasticsearch.init_configs"`
			AdDatadoghqComElasticsearchInstances   string `yaml:"ad.datadoghq.com/elasticsearch.instances"`
			AdDatadoghqComElasticsearchLogs        string `yaml:"ad.datadoghq.com/elasticsearch.logs"`
			StorkLibopenstorageOrgSkipResource     string `yaml:"stork.libopenstorage.org/skip-resource"`
		} `yaml:"annotations"`
		CreationTimestamp time.Time `yaml:"creationTimestamp"`
		Finalizers        []string  `yaml:"finalizers"`
		Generation        int       `yaml:"generation"`
		Labels            struct {
			Name                           string `yaml:"name"`
			Namespace                      string `yaml:"namespace"`
			PdsMutatorAdmit                string `yaml:"pds.mutator/admit"`
			PdsMutatorInjectCustomRegistry string `yaml:"pds.mutator/injectCustomRegistry"`
			PdsDeploymentID                string `yaml:"pds/deployment-id"`
			PdsDeploymentName              string `yaml:"pds/deployment-name"`
			PdsEnvironment                 string `yaml:"pds/environment"`
			PdsProjectID                   string `yaml:"pds/project-id"`
		} `yaml:"labels"`
		Name            string `yaml:"name"`
		Namespace       string `yaml:"namespace"`
		OwnerReferences []struct {
			APIVersion         string `yaml:"apiVersion"`
			BlockOwnerDeletion bool   `yaml:"blockOwnerDeletion"`
			Controller         bool   `yaml:"controller"`
			Kind               string `yaml:"kind"`
			Name               string `yaml:"name"`
			UID                string `yaml:"uid"`
		} `yaml:"ownerReferences"`
		ResourceVersion string `yaml:"resourceVersion"`
		UID             string `yaml:"uid"`
	} `yaml:"metadata"`
	Spec struct {
		Application      string `yaml:"application"`
		ApplicationShort string `yaml:"applicationShort"`
		Capabilities     struct {
			ParallelPod    string `yaml:"parallel_pod"`
			PdsRestore     string `yaml:"pds_restore"`
			PdsSystemUsers string `yaml:"pds_system_users"`
		} `yaml:"capabilities"`
		ConfigMapData struct {
			CLUSTERNAME        string `yaml:"CLUSTER_NAME"`
			DESIREDREPLICAS    string `yaml:"DESIRED_REPLICAS"`
			DISCOVERYSEEDHOSTS string `yaml:"DISCOVERY_SEED_HOSTS"`
			HEAPSIZE           string `yaml:"HEAP_SIZE"`
		} `yaml:"configMapData"`
		Datastorage struct {
			Name                 string `yaml:"name"`
			NumVolumes           int    `yaml:"numVolumes"`
			PersistentVolumeSpec struct {
				Metadata struct {
					Annotations struct {
						StorkLibopenstorageOrgSkipResource string `yaml:"stork.libopenstorage.org/skip-resource"`
						XPlacementStrategy                 string `yaml:"x-placement_strategy"`
					} `yaml:"annotations"`
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				Spec struct {
					AccessModes []string `yaml:"accessModes"`
					Resources   struct {
						Requests struct {
							Storage string `yaml:"storage"`
						} `yaml:"requests"`
					} `yaml:"resources"`
				} `yaml:"spec"`
				Status struct {
				} `yaml:"status"`
			} `yaml:"persistentVolumeSpec"`
			StorageClass struct {
				AllowVolumeExpansion bool `yaml:"allowVolumeExpansion"`
				Metadata             struct {
					Annotations struct {
						StorkLibopenstorageOrgSkipResource string `yaml:"stork.libopenstorage.org/skip-resource"`
					} `yaml:"annotations"`
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				Parameters struct {
					DisableIoProfileProtection string `yaml:"disable_io_profile_protection"`
					Fg                         string `yaml:"fg"`
					Fs                         string `yaml:"fs"`
					Group                      string `yaml:"group"`
					IoProfile                  string `yaml:"io_profile"`
					PriorityIo                 string `yaml:"priority_io"`
					Repl                       string `yaml:"repl"`
				} `yaml:"parameters"`
				Provisioner       string `yaml:"provisioner"`
				ReclaimPolicy     string `yaml:"reclaimPolicy"`
				VolumeBindingMode string `yaml:"volumeBindingMode"`
			} `yaml:"storageClass"`
		} `yaml:"datastorage"`
		DisruptionBudget struct {
			MaxUnavailable int `yaml:"maxUnavailable"`
		} `yaml:"disruptionBudget"`
		Environment string `yaml:"environment"`
		Initialize  string `yaml:"initialize"`
		RoleRules   []struct {
			APIGroups     []string `yaml:"apiGroups"`
			ResourceNames []string `yaml:"resourceNames"`
			Resources     []string `yaml:"resources"`
			Verbs         []string `yaml:"verbs"`
		} `yaml:"roleRules"`
		Service  string `yaml:"service"`
		Services []struct {
			DNSZone  string `yaml:"dnsZone"`
			Metadata struct {
			} `yaml:"metadata"`
			Name    string `yaml:"name"`
			Publish string `yaml:"publish"`
			Spec    struct {
				ClusterIP string `yaml:"clusterIP"`
				Ports     []struct {
					Name       string `yaml:"name"`
					Port       int    `yaml:"port"`
					Protocol   string `yaml:"protocol"`
					TargetPort int    `yaml:"targetPort"`
				} `yaml:"ports"`
				PublishNotReadyAddresses bool   `yaml:"publishNotReadyAddresses"`
				Type                     string `yaml:"type"`
			} `yaml:"spec,omitempty"`
		} `yaml:"services"`
		SharedStorage struct {
			PersistentVolumeClaim struct {
				Metadata struct {
					Annotations struct {
						StorkLibopenstorageOrgSkipResource         string `yaml:"stork.libopenstorage.org/skip-resource"`
						StorkLibopenstorageOrgSkipSchedulerScoring string `yaml:"stork.libopenstorage.org/skipSchedulerScoring"`
					} `yaml:"annotations"`
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				Spec struct {
					AccessModes []string `yaml:"accessModes"`
					Resources   struct {
						Requests struct {
							Storage string `yaml:"storage"`
						} `yaml:"requests"`
					} `yaml:"resources"`
					StorageClassName string `yaml:"storageClassName"`
				} `yaml:"spec"`
				Status struct {
				} `yaml:"status"`
			} `yaml:"persistentVolumeClaim"`
			StorageClass struct {
				AllowVolumeExpansion bool `yaml:"allowVolumeExpansion"`
				Metadata             struct {
					Annotations struct {
						StorkLibopenstorageOrgSkipResource string `yaml:"stork.libopenstorage.org/skip-resource"`
					} `yaml:"annotations"`
					Name string `yaml:"name"`
				} `yaml:"metadata"`
				Parameters struct {
					Fs       string `yaml:"fs"`
					Repl     string `yaml:"repl"`
					Sharedv4 string `yaml:"sharedv4"`
				} `yaml:"parameters"`
				Provisioner       string `yaml:"provisioner"`
				ReclaimPolicy     string `yaml:"reclaimPolicy"`
				VolumeBindingMode string `yaml:"volumeBindingMode"`
			} `yaml:"storageClass"`
		} `yaml:"sharedStorage"`
		StatefulSet struct {
			PodManagementPolicy string `yaml:"podManagementPolicy"`
			Replicas            int    `yaml:"replicas"`
			Selector            struct {
				MatchLabels struct {
					Name                           string `yaml:"name"`
					Namespace                      string `yaml:"namespace"`
					PdsMutatorAdmit                string `yaml:"pds.mutator/admit"`
					PdsMutatorInjectCustomRegistry string `yaml:"pds.mutator/injectCustomRegistry"`
					PdsDeploymentID                string `yaml:"pds/deployment-id"`
					PdsDeploymentName              string `yaml:"pds/deployment-name"`
					PdsEnvironment                 string `yaml:"pds/environment"`
					PdsProjectID                   string `yaml:"pds/project-id"`
				} `yaml:"matchLabels"`
			} `yaml:"selector"`
			ServiceName string `yaml:"serviceName"`
			Template    struct {
				Metadata struct {
					Annotations struct {
						AdDatadoghqComElasticsearchCheckNames  string `yaml:"ad.datadoghq.com/elasticsearch.check_names"`
						AdDatadoghqComElasticsearchInitConfigs string `yaml:"ad.datadoghq.com/elasticsearch.init_configs"`
						AdDatadoghqComElasticsearchInstances   string `yaml:"ad.datadoghq.com/elasticsearch.instances"`
						AdDatadoghqComElasticsearchLogs        string `yaml:"ad.datadoghq.com/elasticsearch.logs"`
						PdsPortworxComDataService              string `yaml:"pds.portworx.com/data_service"`
						PrometheusIoPort                       string `yaml:"prometheus.io/port"`
						PrometheusIoScrape                     string `yaml:"prometheus.io/scrape"`
						StorkLibopenstorageOrgSkipResource     string `yaml:"stork.libopenstorage.org/skip-resource"`
					} `yaml:"annotations"`
					Labels struct {
						Name                           string `yaml:"name"`
						Namespace                      string `yaml:"namespace"`
						PdsMutatorAdmit                string `yaml:"pds.mutator/admit"`
						PdsMutatorInjectCustomRegistry string `yaml:"pds.mutator/injectCustomRegistry"`
						PdsDeploymentID                string `yaml:"pds/deployment-id"`
						PdsDeploymentName              string `yaml:"pds/deployment-name"`
						PdsEnvironment                 string `yaml:"pds/environment"`
						PdsProjectID                   string `yaml:"pds/project-id"`
					} `yaml:"labels"`
				} `yaml:"metadata"`
				Spec struct {
					Affinity struct {
						NodeAffinity struct {
							RequiredDuringSchedulingIgnoredDuringExecution struct {
								NodeSelectorTerms []struct {
									MatchExpressions []struct {
										Key      string   `yaml:"key"`
										Operator string   `yaml:"operator"`
										Values   []string `yaml:"values"`
									} `yaml:"matchExpressions"`
								} `yaml:"nodeSelectorTerms"`
							} `yaml:"requiredDuringSchedulingIgnoredDuringExecution"`
						} `yaml:"nodeAffinity"`
					} `yaml:"affinity"`
					Containers []struct {
						Env []struct {
							Name  string `yaml:"name"`
							Value string `yaml:"value"`
						} `yaml:"env"`
						EnvFrom []struct {
							ConfigMapRef struct {
								Name string `yaml:"name"`
							} `yaml:"configMapRef"`
						} `yaml:"envFrom,omitempty"`
						Image           string `yaml:"image"`
						ImagePullPolicy string `yaml:"imagePullPolicy,omitempty"`
						Name            string `yaml:"name"`
						Resources       struct {
							Limits struct {
								CPU              string `yaml:"cpu"`
								EphemeralStorage string `yaml:"ephemeral-storage"`
								Memory           string `yaml:"memory"`
							} `yaml:"limits"`
							Requests struct {
								CPU              string `yaml:"cpu"`
								EphemeralStorage string `yaml:"ephemeral-storage"`
								Memory           string `yaml:"memory"`
							} `yaml:"requests"`
						} `yaml:"resources"`
						SecurityContext struct {
							AllowPrivilegeEscalation bool `yaml:"allowPrivilegeEscalation"`
							Capabilities             struct {
								Drop []string `yaml:"drop"`
							} `yaml:"capabilities"`
						} `yaml:"securityContext"`
						StartupProbe struct {
							Exec struct {
								Command []string `yaml:"command"`
							} `yaml:"exec"`
							FailureThreshold int `yaml:"failureThreshold"`
							TimeoutSeconds   int `yaml:"timeoutSeconds"`
						} `yaml:"startupProbe,omitempty"`
						VolumeMounts []struct {
							MountPath string `yaml:"mountPath"`
							Name      string `yaml:"name"`
						} `yaml:"volumeMounts"`
						LivenessProbe struct {
							FailureThreshold int `yaml:"failureThreshold"`
							HTTPGet          struct {
								Path string `yaml:"path"`
								Port int    `yaml:"port"`
							} `yaml:"httpGet"`
							PeriodSeconds    int `yaml:"periodSeconds"`
							SuccessThreshold int `yaml:"successThreshold"`
							TimeoutSeconds   int `yaml:"timeoutSeconds"`
						} `yaml:"livenessProbe,omitempty"`
						Ports []struct {
							ContainerPort int    `yaml:"containerPort"`
							Protocol      string `yaml:"protocol"`
						} `yaml:"ports,omitempty"`
						ReadinessProbe struct {
							FailureThreshold int `yaml:"failureThreshold"`
							HTTPGet          struct {
								Path string `yaml:"path"`
								Port int    `yaml:"port"`
							} `yaml:"httpGet"`
							PeriodSeconds    int `yaml:"periodSeconds"`
							SuccessThreshold int `yaml:"successThreshold"`
							TimeoutSeconds   int `yaml:"timeoutSeconds"`
						} `yaml:"readinessProbe,omitempty"`
					} `yaml:"containers"`
					InitContainers []struct {
						Env []struct {
							Name  string `yaml:"name"`
							Value string `yaml:"value"`
						} `yaml:"env"`
						EnvFrom []struct {
							ConfigMapRef struct {
								Name string `yaml:"name"`
							} `yaml:"configMapRef"`
						} `yaml:"envFrom"`
						Image           string `yaml:"image"`
						ImagePullPolicy string `yaml:"imagePullPolicy"`
						Name            string `yaml:"name"`
						Resources       struct {
							Limits struct {
								CPU              string `yaml:"cpu"`
								EphemeralStorage string `yaml:"ephemeral-storage"`
								Memory           string `yaml:"memory"`
							} `yaml:"limits"`
							Requests struct {
								CPU              string `yaml:"cpu"`
								EphemeralStorage string `yaml:"ephemeral-storage"`
								Memory           string `yaml:"memory"`
							} `yaml:"requests"`
						} `yaml:"resources"`
						SecurityContext struct {
							AllowPrivilegeEscalation bool `yaml:"allowPrivilegeEscalation"`
							Capabilities             struct {
								Drop []string `yaml:"drop"`
							} `yaml:"capabilities"`
						} `yaml:"securityContext"`
						VolumeMounts []struct {
							MountPath string `yaml:"mountPath"`
							Name      string `yaml:"name"`
						} `yaml:"volumeMounts"`
					} `yaml:"initContainers"`
					SchedulerName   string `yaml:"schedulerName"`
					SecurityContext struct {
						FsGroup             int    `yaml:"fsGroup"`
						FsGroupChangePolicy string `yaml:"fsGroupChangePolicy"`
						RunAsGroup          int    `yaml:"runAsGroup"`
						RunAsNonRoot        bool   `yaml:"runAsNonRoot"`
						RunAsUser           int    `yaml:"runAsUser"`
						SeccompProfile      struct {
							Type string `yaml:"type"`
						} `yaml:"seccompProfile"`
					} `yaml:"securityContext"`
					ServiceAccountName            string `yaml:"serviceAccountName"`
					TerminationGracePeriodSeconds int    `yaml:"terminationGracePeriodSeconds"`
					Volumes                       []struct {
						EmptyDir struct {
						} `yaml:"emptyDir,omitempty"`
						Name                  string `yaml:"name"`
						PersistentVolumeClaim struct {
							ClaimName string `yaml:"claimName"`
						} `yaml:"persistentVolumeClaim,omitempty"`
						Secret struct {
							SecretName string `yaml:"secretName"`
						} `yaml:"secret,omitempty"`
					} `yaml:"volumes"`
				} `yaml:"spec"`
			} `yaml:"template"`
			UpdateStrategy struct {
				Type string `yaml:"type"`
			} `yaml:"updateStrategy"`
		} `yaml:"statefulSet"`
		Type string `yaml:"type"`
	} `yaml:"spec"`
	Status struct {
		ConnectionDetails struct {
			Nodes []string `yaml:"nodes"`
			Ports struct {
				Rest      int `yaml:"rest"`
				Transport int `yaml:"transport"`
			} `yaml:"ports"`
		} `yaml:"connectionDetails"`
		Health      string `yaml:"health"`
		Initialized string `yaml:"initialized"`
		Pods        []struct {
			IP         string `yaml:"ip"`
			Name       string `yaml:"name"`
			WorkerNode string `yaml:"workerNode"`
		} `yaml:"pods"`
		ReadyReplicas  int `yaml:"readyReplicas"`
		Replicas       int `yaml:"replicas"`
		ResourceEvents []struct {
			Resource struct {
				APIGroup string `yaml:"apiGroup"`
				Kind     string `yaml:"kind"`
				Name     string `yaml:"name"`
			} `yaml:"resource"`
		} `yaml:"resourceEvents"`
		Resources []struct {
			Conditions []struct {
				LastTransitionTime time.Time `yaml:"lastTransitionTime"`
				Message            string    `yaml:"message"`
				Reason             string    `yaml:"reason"`
				Status             string    `yaml:"status"`
				Type               string    `yaml:"type"`
			} `yaml:"conditions"`
			Resource struct {
				Kind string `yaml:"kind"`
				Name string `yaml:"name"`
			} `yaml:"resource"`
		} `yaml:"resources"`
	} `yaml:"status"`
}

type StorageClassConfig struct {
	Parameters struct {
		DisableIoProfileProtection string `yaml:"disable_io_profile_protection"`
		Fg                         string `yaml:"fg"`
		Fs                         string `yaml:"fs"`
		Group                      string `yaml:"group"`
		IoProfile                  string `yaml:"io_profile"`
		PriorityIo                 string `yaml:"priority_io"`
		Repl                       string `yaml:"repl"`
	} `yaml:"parameters"`
	Replicas  int      `yaml:"replicas"`
	Version   string   `yaml:"version"`
	Resources struct { //custom struct
		Limits struct {
			CPU              string `yaml:"cpu"`
			EphemeralStorage string `yaml:"ephemeral-storage"`
			Memory           string `yaml:"memory"`
		} `yaml:"limits"`
		Requests struct {
			CPU              string `yaml:"cpu"`
			EphemeralStorage string `yaml:"ephemeral-storage"`
			Memory           string `yaml:"memory"`
		} `yaml:"requests"`
	} `yaml:"resources"`
}

// PDS const
const (
	PDS_Health_Status_DOWN     PDS_Health_Status = "Partially Available"
	PDS_Health_Status_DEGRADED PDS_Health_Status = "Unavailable"
	PDS_Health_Status_HEALTHY  PDS_Health_Status = "Available"

	errorChannelSize             = 50
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	storageTemplateName          = "QaDefault"
	resourceTemplateName         = "Small"
	appConfigTemplateName        = "QaDefault"
	defaultRetryInterval         = 10 * time.Minute
	duration                     = 900
	timeOut                      = 30 * time.Minute
	timeInterval                 = 10 * time.Second
	maxtimeInterval              = 30 * time.Second
	resiliencyInterval           = 1 * time.Second
	defaultTestConnectionTimeout = 15 * time.Minute
	defaultWaitRebootRetry       = 10 * time.Second
	envDsVersion                 = "DS_VERSION"
	envDsBuild                   = "DS_BUILD"
	zookeeper                    = "ZooKeeper"
	redis                        = "Redis"
	consul                       = "Consul"
	cassandraStresImage          = "scylladb/scylla:4.1.11"
	postgresqlStressImage        = "portworx/torpedo-pgbench:pdsloadTest"
	consulBenchImage             = "pwxbuild/consul-bench-0.1.1"
	consulAgentImage             = "pwxbuild/consul-agent-0.1.1"
	esRallyImage                 = "elastic/rally"
	cbloadImage                  = "portworx/pds-loadtests:couchbase-0.0.2"
	pdsTpccImage                 = "portworx/torpedo-tpcc-automation:v1"
	redisStressImage             = "redis:latest"
	rmqStressImage               = "pivotalrabbitmq/perf-test:latest"
	mysqlBenchImage              = "portworx/pds-mysqlbench:v4"
	postgresql                   = "PostgreSQL"
	cassandra                    = "Cassandra"
	elasticSearch                = "Elasticsearch"
	couchbase                    = "Couchbase"
	mongodb                      = "MongoDB Enterprise"
	rabbitmq                     = "RabbitMQ"
	mysql                        = "MySQL"
	mssql                        = "MS SQL Server"
	kafka                        = "Kafka"
	pxLabel                      = "pds.portworx.com/available"
	defaultParams                = "../drivers/pds/parameters/pds_default_parameters.json"
	pdsParamsConfigmap           = "pds-params"
	configmapNamespace           = "default"
)

// K8s/PDS Instances
var (
	k8sCore       = core.Instance()
	k8sApps       = apps.Instance()
	k8sStorage    = storage.Instance()
	apiExtentions = apiextensions.Instance()
	serviceType   = "LoadBalancer"
)

// PDS vars
var (
	components    *pdsapi.Components
	deployment    *pds.ModelsDeployment
	controlplane  *pdscontrolplane.ControlPlane
	apiClient     *pds.APIClient
	ns            *corev1.Namespace
	pdsAgentpod   corev1.Pod
	ApiComponents *pdsapi.Components

	err                                   error
	isavailable                           bool
	isTemplateavailable                   bool
	isVersionAvailable                    bool
	isBuildAvailable                      bool
	currentReplicas                       int32
	deploymentTargetID, storageTemplateID string
	resourceTemplateID                    string
	appConfigTemplateID                   string
	versionID                             string
	imageID                               string
	serviceAccId                          string
	accountID                             string
	projectID                             string
	tenantID                              string
	istargetclusterAvailable              bool
	isAccountAvailable                    bool
	isStorageTemplateAvailable            bool

	dataServiceDefaultResourceTemplateIDMap = make(map[string]string)
	dataServiceNameIDMap                    = make(map[string]string)
	dataServiceNameVersionMap               = make(map[string][]string)
	dataServiceIDImagesMap                  = make(map[string][]string)
	dataServiceNameDefaultAppConfigMap      = make(map[string]string)
	deploymentsMap                          = make(map[string][]*pds.ModelsDeployment)
	namespaceNameIDMap                      = make(map[string]string)
	dataServiceVersionBuildMap              = make(map[string][]string)
	dataServiceImageMap                     = make(map[string][]string)
)

// GetAndExpectStringEnvVar parses a string from env variable.
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	return varValue
}

// GetAndExpectIntEnvVar parses an int from env variable.
func GetAndExpectIntEnvVar(varName string) (int, error) {
	varValue := GetAndExpectStringEnvVar(varName)
	varIntValue, err := strconv.Atoi(varValue)
	return varIntValue, err
}

// GetAndExpectBoolEnvVar parses a boolean from env variable.
func GetAndExpectBoolEnvVar(varName string) (bool, error) {
	varValue := GetAndExpectStringEnvVar(varName)
	varBoolValue, err := strconv.ParseBool(varValue)
	return varBoolValue, err
}

// ValidateNamespaces validates the namespace is available for pds
func ValidateNamespaces(deploymentTargetID string, ns string, status string) error {
	isavailable = false
	waitErr := wait.Poll(timeOut, timeInterval, func() (bool, error) {
		pdsNamespaces, err := components.Namespace.ListNamespaces(deploymentTargetID)
		if err != nil {
			return false, err
		}
		for _, pdsNamespace := range pdsNamespaces {
			log.Infof("namespace name %v and status %v", *pdsNamespace.Name, *pdsNamespace.Status)
			if *pdsNamespace.Name == ns && *pdsNamespace.Status == status {
				isavailable = true
			}
		}
		if isavailable {
			return true, nil
		}

		return false, nil
	})
	return waitErr
}

// DeletePDSNamespace deletes the given namespace
func DeletePDSNamespace(namespace string) error {
	err := k8sCore.DeleteNamespace(namespace)
	return err
}

// UpdatePDSNamespce updates the namespace
func UpdatePDSNamespce(name string, nsLables map[string]string) (*corev1.Namespace, error) {
	nsSpec := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nsLables,
		},
	}
	ns, err := k8sCore.UpdateNamespace(nsSpec)
	if err != nil {
		return nil, err
	}
	return ns, nil
}

// ReadParams reads the params from given or default json
func ReadParams(filename string) (*Parameter, error) {
	var jsonPara Parameter

	if filename == "" {
		filename, err = filepath.Abs(defaultParams)
		log.Infof("filename %v", filename)
		if err != nil {
			return nil, err
		}
		log.Infof("Parameter json file is not used, use initial parameters value.")
		log.InfoD("Reading params from %v ", filename)
		file, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(file, &jsonPara)
		if err != nil {
			return nil, err
		}
	} else {
		cm, err := core.Instance().GetConfigMap(pdsParamsConfigmap, configmapNamespace)
		if err != nil {
			return nil, err
		}
		if len(cm.Data) > 0 {
			configmap := &cm.Data
			for key, data := range *configmap {
				log.InfoD("key %v \n value %v", key, data)
				json_data := []byte(data)
				err = json.Unmarshal(json_data, &jsonPara)
				if err != nil {
					log.FailOnError(err, "Error while unmarshalling json:")
				}
			}
		}
	}
	return &jsonPara, nil
}

// GetPods returns the list of pods in namespace
func GetPods(namespace string) (*corev1.PodList, error) {
	k8sOps := k8sCore
	podList, err := k8sOps.GetPods(namespace, nil)
	if err != nil {
		return nil, err
	}
	return podList, err
}

// ValidatePods returns err if pods are not up
func ValidatePods(namespace string, podName string) error {

	var newPods []corev1.Pod
	newPodList, err := GetPods(namespace)
	if err != nil {
		return err
	}

	if podName != "" {
		for _, pod := range newPodList.Items {
			if strings.Contains(pod.Name, podName) {
				log.Infof("%v", pod.Name)
				newPods = append(newPods, pod)
			}
		}
	} else {
		//reinitializing the pods
		newPods = append(newPods, newPodList.Items...)
	}

	//validate deployment pods are up and running
	for _, pod := range newPods {
		log.Infof("pds system pod name %v", pod.Name)
		err = k8sCore.ValidatePod(&pod, timeOut, timeInterval)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteDeploymentPods deletes the given pods
func DeletePods(podList []corev1.Pod) error {
	err := k8sCore.DeletePods(podList, true)
	if err != nil {
		return err
	}
	return nil
}

// GetAllDataserviceResourceTemplate get the resource template id's of supported dataservices and forms supported dataserviceNameIdMap
func GetAllDataserviceResourceTemplate(tenantID string, supportedDataServices []string) (map[string]string, map[string]string, error) {
	log.Infof("Get the resource template for each data services")
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return nil, nil, err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(resourceTemplates); i++ {
		if resourceTemplates[i].GetName() == resourceTemplateName {
			isTemplateavailable = true
			dataService, err := components.DataService.GetDataService(resourceTemplates[i].GetDataServiceId())
			if err != nil {
				return nil, nil, err
			}
			for dataKey := range supportedDataServices {
				if dataService.GetName() == supportedDataServices[dataKey] {
					log.Infof("Data service name: %v", dataService.GetName())
					log.Infof("Resource template details ---> Name %v, Id : %v ,DataServiceId %v , StorageReq %v , Memoryrequest %v",
						resourceTemplates[i].GetName(),
						resourceTemplates[i].GetId(),
						resourceTemplates[i].GetDataServiceId(),
						resourceTemplates[i].GetStorageRequest(),
						resourceTemplates[i].GetMemoryRequest())

					dataServiceDefaultResourceTemplateIDMap[dataService.GetName()] =
						resourceTemplates[i].GetId()
					dataServiceNameIDMap[dataService.GetName()] = dataService.GetId()
					isavailable = true
				}
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("Template with Name %v does not exis", resourceTemplateName)
	}
	return dataServiceDefaultResourceTemplateIDMap, dataServiceNameIDMap, nil
}

// GetAllDataServiceAppConfTemplate returns the supported app config templates
func GetAllDataServiceAppConfTemplate(tenantID string, dataServiceNameIDMap map[string]string) (map[string]string, error) {
	appConfigs, err := components.AppConfigTemplate.ListTemplates(tenantID)
	if err != nil {
		return nil, err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(appConfigs); i++ {
		if appConfigs[i].GetName() == appConfigTemplateName {
			isTemplateavailable = true
			for key := range dataServiceNameIDMap {
				if dataServiceNameIDMap[key] == appConfigs[i].GetDataServiceId() {
					dataServiceNameDefaultAppConfigMap[key] = appConfigs[i].GetId()
					isavailable = true
				}
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("App Config Template with name %v does not exist", appConfigTemplateName)
	}
	return dataServiceNameDefaultAppConfigMap, nil
}

// GetVersionsImage returns the required Image of dataservice version
func GetVersionsImage(dsVersion string, dsBuild string, dataServiceID string) (string, string, map[string][]string, error) {
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage
	dsVersionBuildMap := make(map[string][]string)

	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return "", "", nil, err
	}
	isVersionAvailable = false
	isBuildAvailable = false

	for i := 0; i < len(versions); i++ {
		log.Debugf("version name %s and is enabled=%t", *versions[i].Name, *versions[i].Enabled)
		if *versions[i].Name == dsVersion {
			log.Debugf("DS Version %s is enabled in the control plane", dsVersion)
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				if *images[j].Build == dsBuild {
					versionID = versions[i].GetId()
					imageID = images[j].GetId()
					dsVersionBuildMap[versions[i].GetName()] = append(dsVersionBuildMap[versions[i].GetName()], images[j].GetBuild())
					isBuildAvailable = true
					break
				}
			}
			isVersionAvailable = true
			break
		}
	}
	if !(isVersionAvailable && isBuildAvailable) {
		return "", "", nil, fmt.Errorf("version/build passed is not available")
	}
	return versionID, imageID, dsVersionBuildMap, nil
}

// GetAllVersionsImages returns all the versions and Images of dataservice
func GetAllVersionsImages(dataServiceID string) (map[string][]string, map[string][]string, error) {
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage

	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(versions); i++ {
		if *versions[i].Enabled {
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				dataServiceIDImagesMap[versions[i].GetId()] = append(dataServiceIDImagesMap[versions[i].GetId()], images[j].GetId())
				dataServiceVersionBuildMap[versions[i].GetName()] = append(dataServiceVersionBuildMap[versions[i].GetName()], images[j].GetBuild())
			}
		}
	}

	for key := range dataServiceVersionBuildMap {
		log.Infof("Version - %v,Build - %v", key, dataServiceVersionBuildMap[key])
	}
	for key := range dataServiceIDImagesMap {
		log.Infof("DS Verion id - %v,DS Image id - %v", key, dataServiceIDImagesMap[key])
	}
	return dataServiceNameVersionMap, dataServiceIDImagesMap, nil
}

// WaitForPDSDeploymentToBeDown Checks for the deployment health status(Down/Degraded)
func WaitForPDSDeploymentToBeDown(deployment *pds.ModelsDeployment, maxtimeInterval time.Duration, timeout time.Duration) error {
	// validate the deployments in pds
	err = wait.PollImmediate(maxtimeInterval, timeOut, func() (bool, error) {
		status, res, err := components.DataServiceDeployment.GetDeploymentStatus(deployment.GetId())
		log.Infof("Health status -  %v", status.GetHealth())
		if err != nil {
			log.Infof("Deployment status %v", err)
			return false, nil
		}
		if res.StatusCode != state.StatusOK {
			log.Infof("Full HTTP response: %v\n", res)
			err = fmt.Errorf("unexpected status code")
			return false, err
		}
		if strings.Contains(status.GetHealth(), string(PDS_Health_Status_DEGRADED)) || strings.Contains(status.GetHealth(), string(PDS_Health_Status_DOWN)) {
			log.InfoD("Deployment details: Health status -  %v,Replicas - %v, Ready replicas - %v", status.GetHealth(), status.GetReplicas(), status.GetReadyReplicas())
			return true, nil
		} else {
			log.Infof("status: %v", status.GetHealth())
			return false, nil
		}
	})
	return err
}

// WaitForPDSDeploymentToBeUp Checks for the any given deployment health status
func WaitForPDSDeploymentToBeUp(deployment *pds.ModelsDeployment, maxtimeInterval time.Duration, timeout time.Duration) error {
	// validate the deployments in pds
	err = wait.PollImmediate(maxtimeInterval, timeOut, func() (bool, error) {
		status, res, err := components.DataServiceDeployment.GetDeploymentStatus(deployment.GetId())
		if err != nil {
			return false, fmt.Errorf("get deployment status is failing with error: %v", err)
		}
		log.Infof("Health status -  %v", status.GetHealth())
		if res.StatusCode != state.StatusOK {
			log.Infof("Full HTTP response: %v\n", res)
			return false, fmt.Errorf("unexpected status code: %v", err)
		}
		if strings.Contains(status.GetHealth(), string(PDS_Health_Status_HEALTHY)) {
			log.InfoD("Deployment details: Health status -  %v,Replicas - %v, Ready replicas - %v", status.GetHealth(), status.GetReplicas(), status.GetReadyReplicas())
			return true, nil
		} else {
			log.Infof("status: %v", status.GetHealth())
			return false, nil
		}

	})
	return err
}

func CheckPodIsTerminating(depName, ns string) error {
	var ss *v1.StatefulSet
	conditionError := wait.PollImmediate(resiliencyInterval, timeOut, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(depName, ns)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		log.Debugf("pods current replica %v", ss.Status.Replicas)
		pods, err := k8sApps.GetStatefulSetPods(ss)
		if err != nil {
			return false, fmt.Errorf("An error occured while getting the pods belonging to this statefulset %v", err)
		}

		for _, pod := range pods {
			if pod.DeletionTimestamp != nil {
				log.InfoD("pod %v is terminating", pod.Name)
				// Checking If this is a resiliency test case
				if ResiliencyFlag {
					ResiliencyCondition <- true
				}
				log.InfoD("Resiliency Condition Met. Will go ahead and try to induce failure now")
				return true, nil
			}
		}
		log.Infof("Resiliency Condition still not met. Will retry to see if it has met now.....")
		return false, nil
	})
	if conditionError != nil {
		if ResiliencyFlag {
			ResiliencyCondition <- false
			CapturedErrors <- conditionError
		}
	}
	return conditionError
}

// Function to check for set amount of Replica Pods
func GetPdsSs(depName string, ns string, checkTillReplica int32) error {
	var ss *v1.StatefulSet
	log.Debugf("expected replica %v", checkTillReplica)
	conditionError := wait.Poll(resiliencyInterval, timeOut, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(depName, ns)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		log.Debugf("pods current replica %v", ss.Status.Replicas)
		if ss.Status.Replicas >= checkTillReplica {
			// Checking If this is a resiliency test case
			if ResiliencyFlag {
				ResiliencyCondition <- true
			}
			log.InfoD("Resiliency Condition Met. Will go ahead and try to induce failure now")
			return true, nil
		}
		log.Infof("Resiliency Condition still not met. Will retry to see if it has met now.....")
		return false, nil
	})
	if conditionError != nil {
		if ResiliencyFlag {
			ResiliencyCondition <- false
			CapturedErrors <- conditionError
		}
	}
	return conditionError
}

// DeleteK8sPods deletes the pods in given namespace
func DeleteK8sPods(pod string, namespace string) error {
	err := k8sCore.DeletePod(pod, namespace, true)
	return err
}

// DeleteK8sDeployments deletes the deployments in given namespace
func DeleteK8sDeployments(deployment string, namespace string) error {
	err := k8sApps.DeleteDeployment(deployment, namespace)
	return err
}

// DeleteDeployment deletes the given deployment
func DeleteDeployment(deploymentID string) (*state.Response, error) {
	resp, err := components.DataServiceDeployment.DeleteDeployment(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while deleting deployment %v", err)
		return nil, err
	}
	return resp, nil
}

// GetDeploymentConnectionInfo returns the dns endpoint
func GetDeploymentConnectionInfo(deploymentID, dsName string) (string, string, error) {
	var isfound bool
	var dnsEndpoint string
	var port string

	dataServiceDeployment := components.DataServiceDeployment
	deploymentConnectionDetails, clusterDetails, err := dataServiceDeployment.GetConnectionDetails(deploymentID)
	deploymentConnectionDetails.MarshalJSON()
	if err != nil {
		log.Errorf("An Error Occured %v", err)
		return "", "", err
	}
	deploymentNodes := deploymentConnectionDetails.GetNodes()
	log.Infof("Deployment nodes %v", deploymentNodes)
	isfound = false

	if dsName == mysql {
		ports := deploymentConnectionDetails.GetPorts()
		for key, value := range ports {
			if key == "mysql-router" {
				port = fmt.Sprint(value)
			}
		}
	}

	//TODO: Validate vip endpoints as well
	for key, value := range clusterDetails {
		log.Infof("host details key: [%v] value: [%v]", key, value)
		if dsName == consul {
			if strings.Contains(key, "endpoints") {
				dnsEndpoint = fmt.Sprint(value)
				log.Infof("consul dns end point: %s", dnsEndpoint)
				isfound = true
			}
		} else if dsName == mysql {
			for _, node := range deploymentNodes {
				if strings.Contains(node, "vip") {
					dnsEndpoint = node
					isfound = true
					break
				}
			}
		} else {
			if strings.Contains(key, "host") || strings.Contains(key, "nodes") {
				dnsEndpoint = fmt.Sprint(value)
				isfound = true
			}
		}

		switch dsName {
		case cassandra:
			if strings.Contains(key, "cqlPort") {
				port = fmt.Sprint(value)
			}
		case postgresql, couchbase, mongodb, rabbitmq:
			if strings.Contains(key, "port") {
				port = fmt.Sprint(value)
			}
		case consul:
			if strings.Contains(key, "httpPort") {
				port = fmt.Sprint(value)
			}
		case elasticSearch, mssql, redis, kafka, zookeeper:
			if strings.Contains(key, "Port") {
				port = fmt.Sprint(value)
			}
		}
	}
	if !isfound {
		log.Errorf("No connection string found")
		return "", "", err
	}

	return dnsEndpoint, port, nil
}

// GetDeploymentCredentials returns the password to connect to the dataservice
func GetDeploymentCredentials(deploymentID string) (string, error) {
	dataServiceDeployment := components.DataServiceDeployment
	dataServicePassword, err := dataServiceDeployment.GetDeploymentCredentials(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured %v", err)
		return "", err
	}
	pdsPassword := dataServicePassword.GetPassword()
	return pdsPassword, nil
}

// This module sets up MySQL Database for Running TPCC. There is some specific requirement that needs to be
// done for MySQL before running MySQL.
func SetupMysqlDatabaseForTpcc(dbUser string, pdsPassword string, dnsEndpoint string, namespace string) bool {
	log.InfoD("Trying to configure Mysql deployment for TPCC Workload")
	if dbUser == "" {
		dbUser = "pds"
	}
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "configure-mysql-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "configure-mysql",
					Image:           pdsTpccImage,
					Command:         []string{"/bin/sh", "-C", "setup-mysql-for-tpcc.sh", dbUser, pdsPassword, dnsEndpoint},
					WorkingDir:      "/sysbench-tpcc",
					ImagePullPolicy: corev1.PullAlways,
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
	configureMysqlPod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return false
	}
	configureMysqlPodName := configureMysqlPod.ObjectMeta.Name
	//Static sleep to let DB changes settle in
	time.Sleep(20 * time.Second)

	err = wait.Poll(defaultCommandRetry, defaultRetryInterval, func() (bool, error) {
		pod, err := k8sCore.GetPodByName(configureMysqlPodName, namespace)
		if err != nil {
			return false, err
		}
		if k8sCore.IsPodRunning(*pod) {
			log.Infof("Looks like configure-mysql pod is running. Waiting for it to complete.")
			return false, nil
		} else {
			log.Infof("configure-mysql pod is now not running. Moving ahead.")
			return true, nil
		}
	})
	if err != nil {
		return false
	}
	var newPods []corev1.Pod
	newPodList, err := GetPods(namespace)
	if err != nil {
		return false
	}
	//reinitializing the pods
	newPods = append(newPods, newPodList.Items...)

	// Validate if MySQL pod is configured successfully or not for running TPCC
	for _, pod := range newPods {
		if strings.Contains(pod.Name, configureMysqlPodName) {
			log.InfoD("pds system pod name %v", pod.Name)
			for _, c := range pod.Status.ContainerStatuses {
				if c.State.Terminated != nil {
					if c.State.Terminated.ExitCode == 0 && c.State.Terminated.Reason == "Completed" {
						log.InfoD("Successfully Configured Mysql for TPCC Run. Exiting")
						DeleteK8sPods(pod.Name, namespace)
						return true
					} else {
						DeleteK8sPods(pod.Name, namespace)
					}
				} else {
					log.Infof("configure-mysql pod seems to be still running. This is not expected.")
					return false
				}
			}
		}
	}
	return false
}

// This module creates TPCC Schema for a given Deployment and then Runs TPCC Workload
func RunTpccWorkload(dbUser string, pdsPassword string, dnsEndpoint string, dbName string,
	timeToRun string, numOfThreads string, numOfCustomers string, numOfWarehouses string,
	deploymentName string, namespace string, dataServiceName string) bool {
	var fileToRun string
	if dataServiceName == postgresql {
		dbName = "pds"
		fileToRun = "tpcc-pg-run.sh" // file to run in case of Postgres workload
	}
	if dataServiceName == mysql {
		dbName = "tpcc"
		fileToRun = "tpcc-mysql-run.sh" // File to run in case of MySQL workload
	}
	if dbUser == "" {
		dbUser = "pds"
	}
	if timeToRun == "" {
		timeToRun = "120" // Default time to run is 2 minutes
	}
	if numOfThreads == "" {
		numOfThreads = "64" // Default threads is 64
	}
	if numOfCustomers == "" {
		numOfCustomers = "2" // Default number of customer and districts is 4
	}
	if numOfWarehouses == "" {
		numOfWarehouses = "1" // Default number of warehouses to simulate is 2
	}
	// Create a Deployment to Prepare and Run TPCC Workload
	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "tpcc-run",
							Image: pdsTpccImage,
							Command: []string{"/bin/sh", "-C", fileToRun, dbUser, pdsPassword, dnsEndpoint, dbName,
								timeToRun, numOfThreads, numOfCustomers, numOfWarehouses, "run"},
							WorkingDir: "/sysbench-tpcc",
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "tpcc-prepare",
							Image: pdsTpccImage,
							Command: []string{"/bin/sh", "-C", fileToRun, dbUser, pdsPassword, dnsEndpoint, dbName,
								timeToRun, numOfThreads, numOfCustomers, numOfWarehouses, "prepare"},
							WorkingDir:      "/sysbench-tpcc",
							ImagePullPolicy: corev1.PullAlways,
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}
	log.InfoD("Going to Trigger TPCC Workload for the Deployment")
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return false
	}

	timeAskedToRun, err := strconv.Atoi(timeToRun)
	flag := false
	// Hard sleep for 10 seconds for deployment to come up
	time.Sleep(10 * time.Second)
	var newPods []corev1.Pod
	for i := 1; i <= 200; i++ {
		newPodList, _ := GetPods(namespace)
		newPods = append(newPods, newPodList.Items...)
		for _, pod := range newPods {
			if strings.Contains(pod.Name, deployment.Name) {
				log.InfoD("Will check for status of Init Container Once......")
				for _, c := range pod.Status.InitContainerStatuses {
					if c.State.Terminated != nil {
						flag = true
					}
				}
			}
		}
		if flag {
			log.InfoD("TPCC Schema Prepared successfully. Moving ahead to run the TPCC Workload now.....")
			break
		} else {
			log.InfoD("Init Container is still running means TPCC Schema is being prepared. Will wait for further 30 Seconds.....")
			time.Sleep(30 * time.Second)
		}
	}
	if !flag {
		log.Errorf("TPCC Schema couldn't be prepared in 100 minutes. Timing Out. Please check manually.")
		return false
	}
	flag = false
	for i := 1; i <= int((timeAskedToRun+300)/60); i++ {
		newPodList, _ := GetPods(namespace)
		newPods = append(newPods, newPodList.Items...)
		for _, pod := range newPods {
			if strings.Contains(pod.Name, deployment.Name) {
				log.InfoD("Waiting for TPCC Workload Container to finish")
				for _, c := range pod.Status.ContainerStatuses {
					if int32(c.RestartCount) != 0 {
						flag = true
						if c.State.Terminated != nil && c.State.Terminated.ExitCode != 0 && c.State.Terminated.Reason != "Completed" {
							log.Errorf("Something went wrong and Run Container Exited abruptly. Leaving the TPCC deployment as is - pls check manually")
							log.InfoD("Printing TPCC Deployment Describe Status here .....")
							depStatus, err := k8sApps.DescribeDeployment(deployment.Name, namespace)
							if err != nil {
								log.Errorf("Could not print TPCC Deployment status due to some reason. Please check manually.")
								return false
							}
							log.InfoD("%+v\n", *depStatus)
							return false
						}
						break
					}
				}
			}
		}
		if flag {
			log.InfoD("TPCC Workload run finished. Finishing this Test Case")
			break
		} else {
			log.InfoD("TPCC Workload is still running. Will wait for further 1 minute to check again.....")
			time.Sleep(1 * time.Minute)
		}
	}
	log.InfoD("Will delete TPCC Worklaod Deployment now.....")
	DeleteK8sDeployments(deployment.Name, namespace)
	return flag
}

// This module runs Consul Bench workload from pre-cooked Consul Agent and Consul Bench images
func RunConsulBenchWorkload(deploymentName string, namespace string) (*v1.Deployment, error) {
	var replicas int32 = 1
	benchmarkName := deploymentName + "-bench"
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      benchmarkName,
			Namespace: namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": benchmarkName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": benchmarkName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "consul-agent",
							Image:           consulAgentImage,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name: "AGENT_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
								{
									Name:  "PDS_CLUSTER",
									Value: deploymentName,
								},
								{
									Name:  "PDS_NS",
									Value: namespace,
								},
							},
						},
						{
							Name:            "consul-kv-workload",
							Image:           consulAgentImage,
							ImagePullPolicy: corev1.PullAlways,
							Command:         []string{"/bin/kv-workload.sh"},
							Env: []corev1.EnvVar{
								{
									Name: "AGENT_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
							},
						},
						{
							Name:            "consul-bench",
							Image:           consulBenchImage,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name: "CONSUL_HTTP_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
								{
									Name: "SERVICE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "SERVICE_INSTANCES",
									Value: "10",
								},
								{
									Name:  "SERVICE_FLAP_SECONDS",
									Value: "10",
								},
								{
									Name:  "SERVICE_WATCHERS",
									Value: "10",
								},
							},
						},
					},
				},
			},
		},
	}
	log.InfoD("Going to Trigger Consul Bench Workload for the Deployment")
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		return nil, err
	}

	// Sleeping for 1 minute to let the Workload run
	time.Sleep(1 * time.Minute)

	return deployment, nil
}

// Returns a randomly generated string of given length
func GetRandomString(length int32) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	random_string := fmt.Sprintf("%x", b)[:length]
	return random_string
}

// Creates a temporary non PDS namespace of 6 letters length randomly chosen
func CreateTempNS(length int32) (string, error) {
	namespace := GetRandomString(length)
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	ns, err = k8sCore.CreateNamespace(ns)
	if err != nil {
		log.Errorf("Error while creating namespace %v", err)
		return "", err
	}
	return namespace, nil
}

func IsReachable(url string) (bool, error) {
	timeout := time.Duration(15 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	_, err := client.Get(url)
	if err != nil {
		return false, err
	}
	return true, nil
}

func InitPdsComponents(ControlPlaneURL string) error {
	components, controlplane, err = pdsdriver.InitPdsApiComponents(ControlPlaneURL)
	if err != nil {
		return err
	}
	return nil
}

func ValidatePDSDeploymentTargetHealthStatus(DeploymentTargetID, healthStatus string) (*pds.ModelsDeploymentTarget, error) {
	var deploymentTarget *pds.ModelsDeploymentTarget

	waiError := wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		deploymentTarget, err = components.DeploymentTarget.GetTarget(DeploymentTargetID)
		log.FailOnError(err, "Error occurred while getting deployment target")
		if deploymentTarget.GetStatus() == healthStatus {
			log.Infof("deployment target status %s", deploymentTarget.GetStatus())
			return true, nil
		}
		log.Infof("deployment target status %s", deploymentTarget.GetStatus())
		return false, nil
	})

	return deploymentTarget, waiError
}

func DeletePDSCRDs(pdsApiGroups []string) error {
	var isCrdsAvailable bool
	crdList, err := apiExtentions.ListCRDs()
	if err != nil {
		return fmt.Errorf("error while listing crds: %v", err)
	}
	isCrdsAvailable = false
	for index := range pdsApiGroups {
		for _, crd := range crdList.Items {
			log.Debugf("CRD NAMES %s", crd.Name)
			if strings.Contains(crd.Name, pdsApiGroups[index]) {
				crdInfo, err := apiExtentions.GetCRD(crd.Name, metav1.GetOptions{})
				if err != nil {
					return fmt.Errorf("error while getting crd information: %v", err)
				}
				log.InfoD("Deleting crd: %s", crdInfo.Name)
				err = apiExtentions.DeleteCRD(crd.Name)
				if err != nil {
					return fmt.Errorf("error while deleting crd: %v", err)
				}
				isCrdsAvailable = true
			}
		}
		if !isCrdsAvailable {
			log.InfoD("No crd's found for api groups %s", pdsApiGroups[index])
		}
		isCrdsAvailable = false
	}
	return nil
}

// Check if a deployment specific PV and associated PVC is still present. If yes then delete both of them
func DeletePvandPVCs(resourceName string, delPod bool) error {
	log.Debugf("Starting to delete the PV and PVCs for resource %v\n", resourceName)
	var claimName string
	pv_list, err := k8sCore.GetPersistentVolumes()
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("persistant volumes Not Found due to : %v", err)
		}
		return err
	}
	for _, vol := range pv_list.Items {
		if vol.Spec.ClaimRef != nil {
			claimName = vol.Spec.ClaimRef.Name
		} else {
			log.Infof("No PVC bounded to the PV - %v", vol.Name)
			continue
		}
		flag := strings.Contains(claimName, resourceName)
		if flag {
			err := CheckAndDeleteIndependentPV(vol.Name, delPod)
			if err != nil {
				return fmt.Errorf("unable to delete the associated PV and PVCS due to : %v .Please check manually", err)
			}
			log.Debugf("The PV : %v and its associated PVC : %v is deleted !", vol.Name, claimName)
		}
	}
	return nil
}

// Check if PV and associated PVC is still present. If yes then delete both of them
func CheckAndDeleteIndependentPV(name string, delPod bool) error {
	pv_check, err := k8sCore.GetPersistentVolume(name)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	log.InfoD("Stranded PV Found by the name - %s. Going ahead to delete this PV and associated entities", name)
	if pv_check.Status.Phase == corev1.VolumeBound {
		if pv_check.Spec.ClaimRef != nil && pv_check.Spec.ClaimRef.Kind == "PersistentVolumeClaim" {
			namespace := pv_check.Spec.ClaimRef.Namespace
			pvc_name := pv_check.Spec.ClaimRef.Name
			// Delete all Pods in this namespace
			var newPods []corev1.Pod
			podList, err := GetPods(namespace)
			if err != nil {
				return err
			}
			for _, pod := range podList.Items {
				newPods = append(newPods, pod)
			}
			if delPod {
				err = DeletePods(newPods)
				if err != nil {
					return err
				}
			}

			// Delete PVC from figured out namespace
			err = k8sCore.DeletePersistentVolumeClaim(pvc_name, namespace)
			if err != nil {
				return err
			}
		}
	}
	// Delete PV as it is still available from previous run
	err = k8sCore.DeletePersistentVolume(name)
	if err != nil {
		return err
	}
	return nil
}

// Create a Persistent Vol of 5G manual Storage Class
func CreateIndependentPV(name string) (*corev1.PersistentVolume, error) {
	err := CheckAndDeleteIndependentPV(name, true)
	if err != nil {
		return nil, err
	}
	pv := &corev1.PersistentVolume{

		TypeMeta: metav1.TypeMeta{Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},

		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "manual",
			AccessModes: []corev1.PersistentVolumeAccessMode{
				"ReadWriteOnce",
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceName(corev1.ResourceStorage): resource.MustParse("5Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/mnt/data",
				},
			},
		},
	}
	pv, err = k8sCore.CreatePersistentVolume(pv)
	if err != nil {
		return pv, err
	}
	return pv, nil
}

// Create a PV Claim of 5G Storage
func CreateIndependentPVC(namespace string, name string) (*corev1.PersistentVolumeClaim, error) {
	ns := namespace
	storageClass := "manual"
	createOpts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	pvc, err := k8sCore.CreatePersistentVolumeClaim(createOpts)
	if err != nil {
		log.Errorf("PVC Could not be created. Exiting. %v", err)
		return pvc, err
	}
	return pvc, nil
}

// Create an Independant MySQL non PDS App running in a namespace
func CreateIndependentMySqlApp(ns string, podName string, appImage string, pvcName string) (*corev1.Pod, string, error) {
	namespace := ns
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  podName,
					Image: appImage,
					Env:   make([]corev1.EnvVar, 1),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
	volumename := "app-persistent-storage"
	var volumes = make([]corev1.Volume, 1)
	volumes[0] = corev1.Volume{Name: volumename, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName, ReadOnly: false}}}
	podSpec.Spec.Volumes = volumes
	env := []string{"MYSQL_ROOT_PASSWORD"}
	var value []string
	value = append(value, "password")
	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return pod, "", err
	}
	return pod, podName, nil
}

// CreatePodWorkloads generate workloads as standalone pods
func CreatePodWorkloads(name string, image string, creds WorkloadGenerationParams, namespace string, count string, env []string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: name + "-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  name,
					Image: image,
					Env:   make([]corev1.EnvVar, 4),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, creds.Host)
	value = append(value, creds.User)
	value = append(value, creds.Password)
	value = append(value, count)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod [%s], Err: %v", podSpec.Name, err)
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to validate pod [%s], Err: %v", pod.Name, err)
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil

}

// CreateDeploymentWorkloads generate workloads as deployment pods
func CreateDeploymentWorkloads(command, deploymentName, stressImage, namespace string) (*v1.Deployment, error) {

	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
			Labels:       map[string]string{"app": deploymentName},
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    deploymentName,
							Image:   stressImage,
							Command: []string{"/bin/bash", "-c"},
							Args:    []string{command},
						},
					},
					RestartPolicy: "Always",
				},
			},
		},
	}
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return nil, err
	}

	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return deployment, nil
}

// CreatepostgresqlWorkload generate workloads on the pg db
func CreatepostgresqlWorkload(dnsEndpoint string, pdsPassword string, scalefactor string, iterations string, deploymentName string, namespace string) (*v1.Deployment, error) {
	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "pgbench",
							Image:   postgresqlStressImage,
							Command: []string{"/pgloadgen.sh"},
							Args:    []string{dnsEndpoint, pdsPassword, scalefactor, iterations},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return nil, err
	}
	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(2 * time.Minute)

	return deployment, err
}

// CreateRedisWorkload func runs traffic on the Redis deployments
func CreateRedisWorkload(name string, image string, dnsEndpoint string, pdsPassword string, namespace string, env []string, command string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: name + "-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    name,
					Image:   image,
					Command: []string{"/bin/sh", "-c"},
					Env:     make([]corev1.EnvVar, 3),
					Args:    []string{command},
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, dnsEndpoint)
	value = append(value, "pds")
	value = append(value, pdsPassword)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return nil, err
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil
}

// Create MySql Workload (Non-TPCC)
func RunMySqlWorkload(dnsEndpoint string, pdsPassword string, pdsPort string, namespace string, env []string, command string, deploymentName string) (*v1.Deployment, error) {
	var replicas int32 = 1
	var value []string
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    deploymentName,
							Image:   mysqlBenchImage,
							Command: []string{"/bin/sh", "-c"},
							Env:     make([]corev1.EnvVar, 4),
							Args:    []string{command},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}
	value = append(value, "pds")
	value = append(value, dnsEndpoint)
	value = append(value, pdsPassword)
	value = append(value, pdsPort)

	for index := range env {
		deploymentSpec.Spec.Template.Spec.Containers[0].Env[index].Name = env[index]
		deploymentSpec.Spec.Template.Spec.Containers[0].Env[index].Value = value[index]
	}
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(2 * time.Minute)

	return deployment, err
}

// CreateRmqWorkload generate workloads for rmq
func CreateRmqWorkload(dnsEndpoint string, pdsPassword string, namespace string, env []string, command string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "rmq-perf-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "rmqperf",
					Image:   rmqStressImage,
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{command},
					Env:     make([]corev1.EnvVar, 3),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, dnsEndpoint)
	value = append(value, "pds")
	value = append(value, pdsPassword)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return nil, err
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil
}

// This function prepares a deployment for running TPCC Workload
func CreateTpccWorkloads(dataServiceName string, deploymentID string, scalefactor string, iterations string, deploymentName string, namespace string) (bool, error) {
	var dbUser, timeToRun, numOfCustomers, numOfThreads, numOfWarehouses string

	dnsEndpoint, port, err := GetDeploymentConnectionInfo(deploymentID, dataServiceName)
	if err != nil {
		return false, err
	}
	err = controlplane.ValidateDNSEndpoint(dnsEndpoint + ":" + port)
	if err != nil {
		return false, err
	}
	log.Infof("DNS endpoints are reachable...")

	log.Infof("Dataservice DNS endpoint %s", dnsEndpoint)
	pdsPassword, err := GetDeploymentCredentials(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while getting credentials info %v", err)
		return false, err
	}

	switch dataServiceName {
	// For a Postgres workload, simply create schema and run the TPCC Workload for default time
	case postgresql:
		dbName := "pds"
		wasTpccRunSuccessful := RunTpccWorkload(dbUser, pdsPassword, dnsEndpoint, dbName,
			timeToRun, numOfThreads, numOfCustomers, numOfWarehouses,
			deploymentName, namespace, dataServiceName)
		if !wasTpccRunSuccessful {
			return wasTpccRunSuccessful, errors.New("Tpcc run failed. This could be a bug - please check manually")
		} else {
			return wasTpccRunSuccessful, nil
		}
	// For MySQL workload, first setup the deployment to run TPCC, then wait for MySQL to be available,
	// Create TPCC Schema and then run it.
	case mysql:
		dbName := "tpcc"
		var wasMysqlConfigured bool
		// Waiting for approx an hour to check if Mysql deployment comes up
		for i := 1; i <= 80; i++ {
			wasMysqlConfigured = SetupMysqlDatabaseForTpcc(dbUser, pdsPassword, dnsEndpoint, namespace)
			if wasMysqlConfigured {
				log.InfoD("MySQL Deployment is successfully configured to run for TPCC Workload. Starting TPCC Workload Now.")
				break
			} else {
				log.InfoD("MySQL deployment is not yet configured for TPCC. It may still be starting up or there could be some error")
				log.InfoD("Waiting for 30 seconds to retry if MySQL deployment can be configured or not")
				time.Sleep(30 * time.Second)
			}
		}
		if !wasMysqlConfigured {
			log.Errorf("Something went wrong and DB Couldn't be prepared for TPCC workload. Exiting.")
			return wasMysqlConfigured, errors.New("MySQL DB Couldnt be prepared for TPCC as it wasnt reachable. This could be a bug, please check manually.")
		}
		wasTpccRunSuccessful := RunTpccWorkload(dbUser, "password", dnsEndpoint, dbName,
			timeToRun, numOfThreads, numOfCustomers, numOfWarehouses,
			deploymentName, namespace, dataServiceName)
		return wasTpccRunSuccessful, errors.New("TPCC Run failed. This could be a bug - please check manually")
	}
	return false, errors.New("TPCC run failed.")
}

// CreateDataServiceWorkloads generates workloads for the given dataservices
func CreateDataServiceWorkloads(params WorkloadGenerationParams) (*corev1.Pod, *v1.Deployment, error) {
	var dep *v1.Deployment
	var pod *corev1.Pod

	dnsEndpoint, port, err := GetDeploymentConnectionInfo(params.DeploymentID, params.DataServiceName)
	if err != nil {
		return nil, nil, fmt.Errorf("error occured while getting connection info, Err: %v", err)
	}

	err = controlplane.ValidateDNSEndpoint(dnsEndpoint + ":" + port)
	if err != nil {
		return nil, nil, fmt.Errorf("error occured while validating connection info, Err: %v", err)
	}
	log.Infof("DNS endpoints are reachable...")

	pdsPassword, err := GetDeploymentCredentials(params.DeploymentID)
	if err != nil {
		return nil, nil, fmt.Errorf("error occured while getting credentials info, Err: %v", err)
	}

	switch params.DataServiceName {
	case postgresql:
		dep, err = CreatepostgresqlWorkload(dnsEndpoint, pdsPassword, params.ScaleFactor, params.Iterations, params.DeploymentName, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating postgresql workload, Err: %v", err)
		}

	case rabbitmq:
		env := []string{"AMQP_HOST", "PDS_USER", "PDS_PASS"}
		command := "while true; do java -jar perf-test.jar --uri amqp://${PDS_USER}:${PDS_PASS}@${AMQP_HOST} -jb -s 10240 -z 100 --variable-rate 100:30 --producers 10 --consumers 50; done"
		pod, err = CreateRmqWorkload(dnsEndpoint, pdsPassword, params.Namespace, env, command)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating rabbitmq workload, Err: %v", err)
		}

	case redis:
		var command string
		env := []string{"REDIS_HOST", "PDS_USER", "PDS_PASS"}
		command = fmt.Sprintf("redis-benchmark --user ${PDS_USER} -a ${PDS_PASS} -h ${REDIS_HOST} -r 10000 -c 1000 -l -q")
		if params.Replicas > 1 {
			command = fmt.Sprintf("%s %s", command, "--cluster")
		}
		pod, err = CreateRedisWorkload(params.DeploymentName, redisStressImage, dnsEndpoint, pdsPassword, params.Namespace, env, command)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating redis workload, Err: %v", err)
		}

	case cassandra:
		cassCommand := fmt.Sprintf("%s write no-warmup n=1000000 cl=ONE -mode user=pds password=%s native cql3 -col n=FIXED\\(5\\) size=FIXED\\(64\\)  -pop seq=1..1000000 -node %s -port native=9042 -rate auto -log file=/tmp/%s.load.data -schema \"replication(factor=3)\" -errors ignore; cat /tmp/%s.load.data", params.DeploymentName, pdsPassword, dnsEndpoint, params.DeploymentName, params.DeploymentName)
		dep, err = CreateDeploymentWorkloads(cassCommand, params.DeploymentName, cassandraStresImage, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating cassandra workload, Err: %v", err)
		}
	case elasticSearch:
		esCommand := fmt.Sprintf("while true; do esrally race --track=geonames --target-hosts=%s --pipeline=benchmark-only --test-mode --kill-running-processes --client-options=\"timeout:%s,use_ssl:%s,verify_certs:%s,basic_auth_user:%s,basic_auth_password:'%s'\"; done", dnsEndpoint, params.TimeOut, params.UseSSL, params.VerifyCerts, params.User, pdsPassword)
		dep, err = CreateDeploymentWorkloads(esCommand, params.DeploymentName, esRallyImage, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating elasticSearch workload, Err: %v", err)
		}

	case couchbase:
		env := []string{"HOST", "PDS_USER", "PASSWORD", "COUNT"}

		params.Host = dnsEndpoint
		params.User = "pds"
		params.Password = pdsPassword

		pod, err = CreatePodWorkloads(params.DeploymentName, cbloadImage, params, params.Namespace, "1000", env)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating couchbase workload, Err: %v", err)
		}

	case consul:
		dep, err = RunConsulBenchWorkload(params.DeploymentName, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating Consul workload, Err: %v", err)
		}
	case mysql:
		env := []string{"PDS_USER", "MYSQL_HOST", "PDS_PASS", "PDS_PORT"}
		// ToDo: Fetch the port number dynamically
		pdsPort := "6446"
		// ToDo: Move the python command to the docker container/ Part of image.
		mysqlcmd := fmt.Sprintf("python runner.py -user ${PDS_USER} -host ${MYSQL_HOST} -pwd ${PDS_PASS} -port ${PDS_PORT}")
		dep, err = RunMySqlWorkload(dnsEndpoint, pdsPassword, pdsPort, params.Namespace, env, mysqlcmd, params.DeploymentName)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating redis workload, Err: %v", err)
		}
	}
	return pod, dep, nil
}

func GetDataServiceID(ds string) string {
	var dataServiceID string
	dsModel, err := components.DataService.ListDataServices()
	if err != nil {
		log.Errorf("An Error Occured while listing dataservices %v", err)
		return ""
	}
	for _, v := range dsModel {
		if *v.Name == ds {
			dataServiceID = *v.Id
		}
	}
	return dataServiceID
}

// UpdateDataServiceVerison modifies the existing deployment version/image
func UpdateDataServiceVerison(dataServiceID, deploymentID string, appConfigID string, nodeCount int32, resourceTemplateID, dsImage, dsVersion string) (*pds.ModelsDeployment, error) {

	//Validate if the passed dsImage is available in the list of images
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage
	var dsImageID string
	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return nil, err
	}
	isBuildAvailable = false
	for i := 0; i < len(versions); i++ {
		if versions[i].GetName() == dsVersion {
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				if images[j].GetBuild() == dsImage {
					dsImageID = images[j].GetId()
					isBuildAvailable = true
					break
				}
			}
		}
	}

	if !(isBuildAvailable) {
		log.Fatalf("Version/Build passed is not available")
	}

	deployment, err = components.DataServiceDeployment.UpdateDeployment(deploymentID, appConfigID, dsImageID, nodeCount, resourceTemplateID, nil)
	if err != nil {
		log.Errorf("An Error Occured while updating the deployment %v", err)
		return nil, err
	}

	return deployment, nil

}

func GetAllDataServiceHostedNodes(deployment *pds.ModelsDeployment, namespace string) ([]node.Node, error) {
	var dsNodes []string
	var dsNodeList []node.Node
	ss, err := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
	if err != nil {
		return nil, err
	}
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		log.Infof("Node name of pod %v is %v and deletion timestamp is %v", pod.Name, pod.Spec.NodeName, pod.DeletionTimestamp)
		if pod.DeletionTimestamp != nil {
			nodeName := pod.Spec.NodeName
			dsNodes = append(dsNodes, nodeName)
		}
	}
	for _, currNode := range node.GetWorkerNodes() {
		for _, dsNode := range dsNodes {
			if currNode.Name == dsNode {
				dsNodeList = append(dsNodeList, currNode)
			}
		}
	}

	return dsNodeList, nil
}

// GetAllSupportedDataServices get the supported datasservices and returns the map
func GetAllSupportedDataServices() map[string]string {
	dataService, _ := components.DataService.ListDataServices()
	for _, ds := range dataService {
		if !*ds.ComingSoon {
			dataServiceNameIDMap[ds.GetName()] = ds.GetId()
		}
	}
	for key, value := range dataServiceNameIDMap {
		log.Infof("dsKey %v dsValue %v", key, value)
	}
	return dataServiceNameIDMap
}

// GetCRObject
func GetCRObject(namespace, group, version, resource string) (*unstructured.UnstructuredList, error) {
	_, config, err := pdsdriver.GetK8sContext()
	if err != nil {
		return nil, err
	}

	dynamicClient := dynamic.NewForConfigOrDie(config)

	// Get the GVR of the CRD.
	gvr := metav1.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: resource,
	}
	objects, err := dynamicClient.Resource(schema.GroupVersionResource(gvr)).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// ValidateDataServiceVolumes validates the storage configurations
func ValidateDataServiceVolumes(deployment *pds.ModelsDeployment, dataService string, dataServiceDefaultResourceTemplateID string, storageTemplateID string, namespace string) (ResourceSettingTemplate, StorageOptions, StorageClassConfig, error) {
	var config StorageClassConfig
	var resourceTemp ResourceSettingTemplate
	var storageOp StorageOptions
	var dbConfig DBConfig

	labelSelector := make(map[string]string)
	labelSelector["name"] = deployment.GetClusterResourceName()
	storageClasses, err := k8sStorage.GetStorageClasses(labelSelector)
	if err != nil {
		log.FailOnError(err, "An error occured while getting storage classes")
	}

	objects, err := GetCRObject(namespace, "deployments.pds.io", "v1", "databases")

	// Iterate over the CRD objects and print their names.
	for _, object := range objects.Items {
		log.Debugf("Objects created: %v", object.GetName())
		if object.GetName() == deployment.GetClusterResourceName() {
			crJsonObject, err := object.MarshalJSON()
			if err != nil {
				log.FailOnError(err, "An error occured while marshalling cr")
			}
			err = json.Unmarshal(crJsonObject, &dbConfig)
			if err != nil {
				log.FailOnError(err, "An error occured while unmarshalling cr")
			}
		}
	}

	//Get the ds version from the sts
	docImage := dbConfig.Spec.StatefulSet.Template.Spec.Containers[0].Image
	dsVersionImageTag := strings.Split(docImage, ":")
	log.Debugf("version tag %v", dsVersionImageTag[1])

	scJsonData, err := json.Marshal(storageClasses)
	if err != nil {
		log.FailOnError(err, "An error occured while marshalling statefulset")
	}
	err = json.Unmarshal(scJsonData, &config)
	if err != nil {
		log.FailOnError(err, "An error occured while unmarshalling storage class")
	}

	//Assigning values to the custom struct of storageclass config
	config.Resources.Requests.CPU = dbConfig.Spec.StatefulSet.Template.Spec.Containers[0].Resources.Requests.CPU
	config.Resources.Requests.Memory = dbConfig.Spec.StatefulSet.Template.Spec.Containers[0].Resources.Requests.Memory
	config.Resources.Requests.EphemeralStorage = dbConfig.Spec.Datastorage.PersistentVolumeSpec.Spec.Resources.Requests.Storage
	config.Resources.Limits.CPU = dbConfig.Spec.StatefulSet.Template.Spec.Containers[0].Resources.Limits.CPU
	config.Resources.Limits.Memory = dbConfig.Spec.StatefulSet.Template.Spec.Containers[0].Resources.Limits.Memory
	config.Replicas = dbConfig.Status.Replicas
	config.Version = dsVersionImageTag[1]

	config.Parameters.Fg = dbConfig.Spec.Datastorage.StorageClass.Parameters.Fg
	config.Parameters.Fs = dbConfig.Spec.Datastorage.StorageClass.Parameters.Fs
	config.Parameters.Repl = dbConfig.Spec.Datastorage.StorageClass.Parameters.Repl

	rt, err := components.ResourceSettingsTemplate.GetTemplate(dataServiceDefaultResourceTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting resource setting template %v", err)
	}
	resourceTemp.Resources.Requests.CPU = *rt.CpuRequest
	resourceTemp.Resources.Requests.Memory = *rt.MemoryRequest
	resourceTemp.Resources.Requests.Storage = *rt.StorageRequest
	resourceTemp.Resources.Limits.CPU = *rt.CpuLimit
	resourceTemp.Resources.Limits.Memory = *rt.MemoryLimit

	st, err := components.StorageSettingsTemplate.GetTemplate(storageTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting storage template %v", err)
		return resourceTemp, storageOp, config, err
	}
	storageOp.Filesystem = st.GetFs()
	storageOp.Replicas = st.GetRepl()
	storageOp.VolumeGroup = st.GetFg()

	return resourceTemp, storageOp, config, nil
}

// ValidateDataServiceVolumes validates the volumes
func ValidateAllDataServiceVolumes(deployment *pds.ModelsDeployment, dataService string, dataServiceDefaultResourceTemplateID map[string]string, storageTemplateID string) (ResourceSettingTemplate, StorageOptions, StorageClassConfig, error) {
	var config StorageClassConfig
	var resourceTemp ResourceSettingTemplate
	var storageOp StorageOptions
	ss, err := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), GetAndExpectStringEnvVar("NAMESPACE"))
	if err != nil {
		log.Warnf("An Error Occured while getting statefulsets %v", err)
	}
	err = k8sApps.ValidatePVCsForStatefulSet(ss, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An error occured while validating pvcs of statefulsets %v ", err)
	}
	pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
	if err != nil {
		log.Warnf("An Error Occured while getting pvcs of statefulsets %v", err)
	}

	for _, pvc := range pvcList.Items {
		sc, err := k8sCore.GetStorageClassForPVC(&pvc)
		if err != nil {
			log.Errorf("Error Occured while getting storage class for pvc %v", err)
		}
		scAnnotation := sc.Annotations
		for k, v := range scAnnotation {
			if k == "kubectl.kubernetes.io/last-applied-configuration" {
				log.Infof("Storage Options Values %v", v)
				data := []byte(v)
				err := json.Unmarshal(data, &config)
				if err != nil {
					log.Errorf("Error Occured while getting volume params %v", err)
				}
			}
		}
	}

	rt, err := components.ResourceSettingsTemplate.GetTemplate(dataServiceDefaultResourceTemplateIDMap[dataService])
	if err != nil {
		log.Errorf("Error Occured while getting resource setting template %v", err)
	}
	resourceTemp.Resources.Requests.CPU = *rt.CpuRequest
	resourceTemp.Resources.Requests.Memory = *rt.MemoryRequest
	resourceTemp.Resources.Requests.Storage = *rt.StorageRequest
	resourceTemp.Resources.Limits.CPU = *rt.CpuLimit
	resourceTemp.Resources.Limits.Memory = *rt.MemoryLimit

	st, err := components.StorageSettingsTemplate.GetTemplate(storageTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting storage template %v", err)
		return resourceTemp, storageOp, config, err
	}
	storageOp.Filesystem = st.GetFs()
	storageOp.Replicas = st.GetRepl()
	storageOp.VolumeGroup = st.GetFg()

	return resourceTemp, storageOp, config, nil

}

// DeleteK8sNamespace deletes the specified namespace
func DeleteK8sNamespace(namespace string) error {
	err := k8sCore.DeleteNamespace(namespace)
	if err != nil {
		log.Errorf("Could not delete the specified namespace %v because %v", namespace, err)
		return err
	}
	return nil
}

// ValidateDataServiceDeploymentNegative checks if deployment is not present
func ValidateDataServiceDeploymentNegative(deployment *pds.ModelsDeployment, namespace string) error {
	var ss *v1.StatefulSet
	err = wait.Poll(10*time.Second, 30*time.Second, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		return true, nil
	})
	if err == nil {
		log.Errorf("Validate DS Deployment negative failed, the StatefulSet still exists %v", ss)
		return fmt.Errorf("the deployment %v has not been deleted", deployment.Name)
	}
	return nil
}

func ValidateK8sNamespaceDeleted(namespace string) error {
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		_, err := k8sCore.GetNamespace(namespace)
		if err == nil {
			log.Warnf("The namespace %v has not been deleted", namespace)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("The namespace %v has not been deleted", namespace)
		return fmt.Errorf("the namespace %v has not been deleted", namespace)
	}
	log.Infof("The namespace has been successfully deleted")
	return nil

}

// TODO: Consolidate this function with CheckNamespace
func CreateK8sPDSNamespace(nname string) (*corev1.Namespace, error) {
	ns, err := k8sCore.CreateNamespace(&corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   nname,
			Labels: map[string]string{"pds.portworx.com/available": "true"},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("could not create ns %v", nname)
	}

	return ns, nil

}

// DeleteK8sPDSNamespace deletes the pdsnamespace
func DeleteK8sPDSNamespace(nname string) error {
	err := k8sCore.DeleteNamespace(nname)
	return err
}

// GetPDSPods returns Name of the pds pod
func GetPDSPods(podName string, pdsNamespace string) corev1.Pod {
	log.InfoD("Get agent pod from %v namespace", pdsNamespace)
	podList, err := GetPods(pdsNamespace)
	log.FailOnError(err, "Error while getting pods")
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, podName) {
			log.Infof("%v", pod.Name)
			pdsAgentpod = pod
			break
		}
	}
	return pdsAgentpod
}

func GetPodsFromK8sStatefulSet(deployment *pds.ModelsDeployment, namespace string) ([]corev1.Pod, error) {
	var ss *v1.StatefulSet
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("An Error Occured while getting statefulsets %v", err)
		return nil, err
	}
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		log.Errorf("An error occured while getting the pods belonging to this statefulset %v", err)
		return nil, err
	}
	return pods, nil
}

func GetK8sNodeObjectUsingPodName(nodeName string) (*corev1.Node, error) {
	nodeObject, err := k8sCore.GetNodeByName(nodeName)
	if err != nil {
		log.Errorf("Could not get the node object for node %v because %v", nodeName, err)
		return nil, err
	}
	return nodeObject, nil
}

func DrainPxPodOnK8sNode(node *corev1.Node, namespace string) error {
	labelSelector := map[string]string{"name": "portworx"}
	pod, err := k8sCore.GetPodsByNodeAndLabels(node.Name, namespace, labelSelector)
	if err != nil {
		log.Errorf("Could not fetch pods running on the given node %v", err)
		return err
	}
	log.Infof("Portworx pod to be drained %v from node %v", pod.Items[0].Name, node.Name)
	err = k8sCore.DrainPodsFromNode(node.Name, pod.Items, timeOut, maxtimeInterval)
	if err != nil {
		log.Errorf("Could not drain the node %v", err)
		return err
	}

	return nil
}

func LabelK8sNode(node *corev1.Node, label string) error {
	keyval := strings.Split(label, "=")
	err := k8sCore.AddLabelOnNode(node.Name, keyval[0], keyval[1])
	return err
}

func RemoveLabelFromK8sNode(node *corev1.Node, label string) error {
	err := k8sCore.RemoveLabelOnNode(node.Name, label)
	return err
}

func UnCordonK8sNode(node *corev1.Node) error {
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		err = k8sCore.UnCordonNode(node.Name, timeOut, maxtimeInterval)
		if err != nil {
			log.Errorf("Failed uncordon node %v due to %v", node.Name, err)
			return false, nil
		}
		return true, nil
	})
	return err
}

func VerifyPxPodOnNode(nodeName string, namespace string) (bool, error) {
	labelSelector := map[string]string{"name": "portworx"}
	var pods *corev1.PodList
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		pods, err = k8sCore.GetPodsByNodeAndLabels(nodeName, namespace, labelSelector)
		if err != nil {
			log.Errorf("Failed to get pods from node %v due to %v", nodeName, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("Could not fetch pods running on the given node %v", err)
		return false, err
	}
	pxPodName := pods.Items[0].Name
	log.Infof("The portworx pod %v from node %v", pxPodName, nodeName)
	return true, nil
}

// Returns nodes on which pods of a given statefulset are running
func GetNodesOfSS(SSName string, namespace string) ([]*corev1.Node, error) {
	var nodes []*corev1.Node
	// Get StatefulSet Object of the given Statefulset
	ss, err := k8sApps.GetStatefulSet(SSName, namespace)
	if err != nil {
		return nil, err
	}
	// Get The pods associated to this statefulset
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		return nil, err
	}
	// Create Node objects and append them to a list
	if pods != nil && len(pods) > 0 {
		for _, pod := range pods {
			if len(pod.Spec.NodeName) > 0 {
				node, err := k8sCore.GetNodeByName(pod.Spec.NodeName)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, node)
			}
		}
	}
	return nodes, nil
}

// Returns list of Pods from a given Statefulset running on a given Node
func GetPodsOfSsByNode(SSName string, nodeName string, namespace string) ([]corev1.Pod, error) {
	// Get StatefulSet Object of the given Statefulset
	ss, err := k8sApps.GetStatefulSet(SSName, namespace)
	if err != nil {
		return nil, err
	}
	// Get The pods associated to this statefulset
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		return nil, err
	}
	var podsList []corev1.Pod

	// Create Node objects and append them to a list
	if pods != nil && len(pods) > 0 {
		for _, pod := range pods {
			if pod.Spec.NodeName == nodeName {
				podsList = append(podsList, pod)
			}
		}
	}
	if podsList != nil && len(podsList) > 0 {
		return podsList, nil
	}
	return nil, errors.New(fmt.Sprintf("There is no pod of the given statefulset running on the given node name %s", nodeName))
}

func UpdateDeploymentResourceConfig(deployment *pds.ModelsDeployment, namespace string, resourceTemplate string) error {
	var resourceTemplateId string
	var cpuLimits int64
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(*deployment.TenantId)
	if err != nil {
		if ResiliencyFlag {
			CapturedErrors <- err
		}
		return err
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
		return fmt.Errorf("resource template - {%v} , not found", resourceTemplate)
	}
	log.Infof("Deployment details: Ds id- %v, appConfigTemplateID - %v, imageId - %v, Node count -%v, resourceTemplateId- %v ", deployment.GetId(),
		appConfigTemplateID, deployment.GetImageId(), deployment.GetNodeCount(), resourceTemplateId)
	_, err = components.DataServiceDeployment.UpdateDeployment(deployment.GetId(),
		appConfigTemplateID, deployment.GetImageId(), deployment.GetNodeCount(), resourceTemplateId, nil)
	if err != nil {
		if ResiliencyFlag {
			CapturedErrors <- err
		}
		return err
	}
	ss, testError := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
	if testError != nil {
		if ResiliencyFlag {
			CapturedErrors <- testError
		}
		return testError
	}
	err = wait.Poll(resiliencyInterval, timeOut, func() (bool, error) {
		// Get Pods of this StatefulSet
		pods, testError := k8sApps.GetStatefulSetPods(ss)
		if testError != nil {
			if ResiliencyFlag {
				CapturedErrors <- testError
			}
			return false, testError
		}
		for _, pod := range pods {
			for _, container := range pod.Spec.Containers {
				if container.Resources.Limits.Cpu().Value() == cpuLimits {
					if ResiliencyFlag {
						ResiliencyCondition <- true
					}
					return true, nil
				} else {
					return false, nil
				}
			}
		}
		return false, fmt.Errorf("no pods has been updated to required resource configuration")
	})
	if err != nil {
		if ResiliencyFlag {
			CapturedErrors <- err
		}
		return err
	}
	return nil
}

// ExpandAndValidatePxPool will pick a random px-pool and expand its size
func ExpandAndValidatePxPool(context []*scheduler.Context) error {
	log.InfoD("Entering into PX-POOL Expansion...")
	poolIDToResize := PickPoolToResize(context)
	poolToBeResized := GetStoragePool(poolIDToResize)
	log.InfoD("Pool to be resized is %v", poolToBeResized)
	log.InfoD("Verify that pool resize is not in progress")
	if val, err := PoolResizeIsInProgress(poolToBeResized); val {
		// wait until resize is completed and get the updated pool again
		poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
	} else {
		log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
	}
	var expectedSize uint64
	var expectedSizeWithJournal uint64
	log.InfoD("Calculate expected pool size and trigger pool resize")
	expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
	expectedSize = RoundUpValue(expectedSize)
	isjournal, err := IsJournalEnabled()
	log.FailOnError(err, "Failed to check is Journal enabled")
	expectedSizeWithJournal = expectedSize
	if isjournal {
		expectedSizeWithJournal = expectedSizeWithJournal - 3
	}
	log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
	err = tests.Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, false)
	resizeErr := WaitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
	if resizeErr != nil {
		log.FailOnError(resizeErr, fmt.Sprintf("Failed to resize pool with ID-  %s", poolIDToResize))
	}
	errorChan := make(chan error, errorChannelSize)
	for _, ctx := range context {
		log.Infof("Validating context: %v", ctx.App.Key)
		ValidateContext(ctx, &errorChan)
		for err := range errorChan {
			log.FailOnError(err, "Failed to validate Deployment")
		}
	}
	resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
	log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
	newPoolSize := resizedPool.TotalSize / units.GiB
	isExpansionSuccess := false
	if newPoolSize >= expectedSizeWithJournal {
		isExpansionSuccess = true
	} else {
		if ResiliencyFlag {
			CapturedErrors <- err
		}
		log.FailOnError(err, fmt.Sprintf("Failed to resize pool with ID-  %s", poolIDToResize))
		return err
	}
	log.InfoD("Px-Pool expansion status is - %v", isExpansionSuccess)
	log.InfoD("expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize)
	return nil
}

func GetStoragePool(poolIDToResize string) *api.StoragePool {
	pool, err := GetStoragePoolByUUID(poolIDToResize)
	log.FailOnError(err, "Failed to get pool using UUID %s", poolIDToResize)
	return pool
}

// PickPoolToResize Picks any random px-pool to resize if I/0s are not running
func PickPoolToResize(contexts []*scheduler.Context) string {
	poolWithIO, err := GetPoolIDWithIOs(contexts)
	if poolWithIO == "" || err != nil {
		log.Warnf("No pool with IO found, picking a random pool in use to resize")
	}
	poolIDsInUseByTestingApp, err := GetPoolsInUse()
	log.FailOnError(err, "Error identifying pool to run test")
	poolIDToResize := poolIDsInUseByTestingApp[0]
	return poolIDToResize
}

// RoundUpValue func to round up fetched values to 10
func RoundUpValue(toRound uint64) uint64 {
	if toRound%10 == 0 {
		return toRound
	}
	rs := (10 - toRound%10) + toRound
	return rs
}

// PoolResizeIsInProgress checks the status of PX-Pool resize
func PoolResizeIsInProgress(poolToBeResized *api.StoragePool) (bool, error) {
	if poolToBeResized.LastOperation != nil {
		f := func() (interface{}, bool, error) {
			pools, err := tests.Inst().V.ListStoragePools(metav1.LabelSelector{})
			if err != nil || len(pools) == 0 {
				return nil, true, fmt.Errorf("error getting pools list, err %v", err)
			}
			updatedPoolToBeResized := pools[poolToBeResized.Uuid]
			if updatedPoolToBeResized == nil {
				return nil, false, fmt.Errorf("error getting pool with given pool id %s", poolToBeResized.Uuid)
			}
			if updatedPoolToBeResized.LastOperation.Status != api.SdkStoragePool_OPERATION_SUCCESSFUL {
				log.Infof("Current pool status : %v", updatedPoolToBeResized.LastOperation)
				if updatedPoolToBeResized.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
					return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation)
				}
				log.Infof("Pool Resize is already in progress: %v", updatedPoolToBeResized.LastOperation)
				return nil, true, nil
			}
			return nil, false, nil
		}
		_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
		if err != nil {
			return false, err
		}
	}
	stNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
	if err != nil {
		return false, err
	}
	t := func() (interface{}, bool, error) {
		status, err := tests.Inst().V.GetNodePoolsStatus(*stNode)
		if err != nil {
			return "", false, err
		}
		currStatus := status[poolToBeResized.Uuid]
		if currStatus == "Offline" {
			return "", true, fmt.Errorf("pool [%s] has current status [%s].Waiting rebalance to complete if in-progress", poolToBeResized.Uuid, currStatus)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(t, 120*time.Minute, 2*time.Second)
	if err != nil {
		return false, err
	}
	return true, nil
}

// WaitForPoolToBeResized will wait for PX-Pool resize and report the status post expansion
func WaitForPoolToBeResized(expectedSize uint64, poolIDToResize string, isJournalEnabled bool) error {
	currentLastMsg := ""
	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
		}
		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			log.Infof("Pool Resize Status : %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("pool %s expansion has failed. Error: %s", poolIDToResize, expandedPool.LastOperation)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_PENDING {
				return nil, true, fmt.Errorf("pool %s is in pending state, waiting to start", poolIDToResize)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS {
				if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
					if currentLastMsg == expandedPool.LastOperation.Msg {
						return nil, false, fmt.Errorf("pool reblance is not progressing")
					}
					currentLastMsg = expandedPool.LastOperation.Msg
					return nil, true, fmt.Errorf("wait for pool rebalance to complete")
				}
				if strings.Contains(expandedPool.LastOperation.Msg, "No pending operation pool status: Maintenance") ||
					strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance complete pool status: Maintenance") {
					return nil, false, nil
				}
				return nil, true, fmt.Errorf("waiting for pool status to update")
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB
		expectedSizeWithJournal := expectedSize
		if isJournalEnabled {
			expectedSizeWithJournal = expectedSizeWithJournal - 3
		}
		if newPoolSize >= expectedSizeWithJournal {
			// storage pool resize has been completed
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("pool has not been resized to %d or %d yet. Waiting...Current size is %d", expectedSize, expectedSizeWithJournal, newPoolSize)
	}
	_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
	return err
}

func CreatePdsLabeledNamespaces() (string, error) {
	nname := "nsi" + strconv.Itoa(rand.Int())
	_, err := CreateK8sPDSNamespace(nname)
	log.FailOnError(err, "error while creating pds namespace")
	log.InfoD("Created namespace: %v", nname)
	log.InfoD("Waiting for created namespaces to be available in PDS")
	time.Sleep(10 * time.Second)
	return nname, nil
}

func CreateSiAndIamRoleBindings(accountID string, nsRoles []pds.ModelsBinding) (string, string, error) {
	actorId, siToken, err := components.ServiceIdentity.CreateAndGetServiceIdentityToken(accountID)
	if err != nil {
		return "", "", fmt.Errorf("error while creating and fetching service identity token for actorID %v", actorId)
	}
	log.InfoD("Successfully created serviceIdentity- %v", actorId)
	iamModels, err := components.ServiceIdentity.CreateIAMRoleBindingsWithSi(actorId, accountID, nsRoles, siToken)
	if err != nil {
		return "", "", fmt.Errorf("error generating service identity token for serviceId- %v", iamModels.Id)
	}
	iamId := *iamModels.Id
	log.InfoD("Successfully created for IAM Roles- %v", iamId)
	return actorId, iamId, nil
}
