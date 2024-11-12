package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	version "github.com/hashicorp/go-version"
	storkversion "github.com/libopenstorage/stork/pkg/version"
	coreops "github.com/portworx/sched-ops/k8s/core"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"github.com/openshift/api/security/v1"  // Provides SecurityContextConstraints type

)

const (
	// BackupJobPrefix job prefix for backup jobs
	BackupJobPrefix = "b"
	// RestoreJobPrefix job prefix for restore jobs
	RestoreJobPrefix = "r"
	// SkipResourceAnnotation skipping kopia secret to be backed up
	SkipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	// BackupObjectNameKey - label key to store backup object name
	BackupObjectNameKey = "backup-object-name"
	// BackupObjectUIDKey - label key to store backup object uid
	BackupObjectUIDKey = "backup-object-uid"
	// TLSCertMountVol mount vol name for tls certificate secret
	TLSCertMountVol = "tls-secret"
	// NfsVolumeName is the Volume spec's name to be used in kopia Job Spec
	NfsVolumeName = "nfs-target"
	// DefaultTimeout default timeout for tasks retry
	DefaultTimeout = 1 * time.Minute
	// ProgressCheckInterval regular interval at which task does a retry
	ProgressCheckInterval = 5 * time.Second
	// KdmpConfigmapName kdmp config map name
	KdmpConfigmapName = "kdmp-config"
	// KdmpConfigmapNamespace kdmp config map ns
	KdmpConfigmapNamespace = "kube-system"
	// DefaultCompresion default compression type
	DefaultCompresion = "s2-parallel-8"
	// DefaultQPS - default qps value for k8s apis
	DefaultQPS = 100
	// DefaultBurst - default burst value for k8s apis
	DefaultBurst = 100
	// QPSKey - configmap QPS key name
	QPSKey = "K8S_QPS"
	// BurstKey - configmap burst key name
	BurstKey                             = "K8S_BURST"
	k8sMinVersionSASecretTokenNotSupport = "1.24"
	dacClusterRoleName                   = "dac"
	SccRoleBindingNameSuffix             = "-scc"
)

var (
	// JobPodBackOffLimit backofflimit for the job
	JobPodBackOffLimit = int32(10)
)

// GetCsiRestoreJobName returns a CSI restore Job name based on name of the CR
func GetCsiRestoreJobName(jobPrefix string, crName string) string {
	return jobPrefix + strings.SplitN(crName, "restore", 2)[1]
}

// isServiceAccountSecretMissing returns true, if the K8s version does not support secret token for the service account.
func isServiceAccountSecretMissing() (bool, error) {
	k8sVersion, _, err := storkversion.GetFullVersion()
	if err != nil {
		return false, err
	}
	VersionTokenNotSupported, err := version.NewVersion(k8sMinVersionSASecretTokenNotSupport)
	if err != nil {
		return false, err

	}
	if k8sVersion.GreaterThanOrEqual(VersionTokenNotSupported) {
		return true, nil
	}
	return false, nil
}

// SetupServiceAccount create a service account and bind it to a provided role.
func SetupServiceAccount(name, namespace string, role *rbacv1.Role) error {
	if role != nil {
		role.Name, role.Namespace = name, namespace
		role.Annotations = map[string]string{
			SkipResourceAnnotation: "true",
		}
		if _, err := rbacops.Instance().CreateRole(role); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s role: %s", namespace, name, err)
		}
		if _, err := rbacops.Instance().CreateRoleBinding(roleBindingFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s rolebinding: %s", namespace, name, err)
		}
		logrus.Debug("kartik Entering the function which creates scc and stuff")
		addRoleBindingForScc(name, namespace, dacClusterRoleName)
	}
	var sa *corev1.ServiceAccount
	var err error
	if sa, err = coreops.Instance().CreateServiceAccount(serviceAccountFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s serviceaccount: %s", namespace, name, err)
	}
	// From 1.24.0 onwards service token does not support default secret token
	tokenSupported, err := isServiceAccountSecretMissing()
	if !tokenSupported {
		t := func() (interface{}, bool, error) {
			sa, err = coreops.Instance().GetServiceAccount(name, namespace)
			if err != nil {
				errMsg := fmt.Sprintf("failed fetching sa [%v/%v]: %v", name, namespace, err)
				logrus.Tracef("%v", errMsg)
				return "", true, fmt.Errorf("%v", errMsg)
			}
			if sa.Secrets == nil {
				errMsg := fmt.Sprintf("secret token is missing in sa [%v/%v]", name, namespace)
				return "", true, fmt.Errorf("%v", errMsg)
			}
			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(t, DefaultTimeout, ProgressCheckInterval); err != nil {
			errMsg := fmt.Sprintf("max retries done, failed in fetching secret token of sa [%v/%v]: %v ", name, namespace, err)
			logrus.Errorf("%v", errMsg)
			// Exhausted all retries
			return err
		}

		tokenName := sa.Secrets[0].Name
		secretToken, err := coreops.Instance().GetSecret(tokenName, namespace)
		if err != nil {
			return fmt.Errorf("failed in getting secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
		}
		secretToken.Annotations[SkipResourceAnnotation] = "true"
		_, err = coreops.Instance().UpdateSecret(secretToken)
		if err != nil {
			return fmt.Errorf("failed in updating the secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
		}
	}
	return nil
}

// Check if corresponding SCC cluster role exists, then only create rolebinding for it.
// This way we will avoid creating rolebinding in non-ocp cluster.
func addRoleBindingForScc(name string, namespace string, sccClusterRoleName string) (bool, error) {
	// read the kdmp-config configmap to read a key named KDMP_JOB_WITH_ANYUID
	// If  it is true  only then exercise below code else return without doing anything
	// kdmpConfigMap, err := coreops.Instance().GetConfigMap(KdmpConfigmapName, KdmpConfigmapNamespace)
	// if err != nil {
	// 	if errors.IsNotFound(err) {
	// 		return false, nil
	// 	}
	// 	return true, fmt.Errorf("get %s/%s configmap: %s", KdmpConfigmapNamespace, KdmpConfigmapName, err)
	// }
	// value, ok := kdmpConfigMap.Data[runJobPodWithDacOverride]
	// if !ok || value != "true" {
	// 	return false, nil
	// }
	
	sccName := "allow-dac-override-scc"
	logrus.Debugf("kartik scc which is to be created is: %v", sccName)
	if err := ensureDACOverrideSCC(sccName); err != nil {
		return true, fmt.Errorf("failed to ensure DAC override SCC: %v", err)
	}

	// Step 2: Check if the ClusterRole for this SCC exists, create it if it doesn't
    _, err := rbacops.Instance().GetClusterRole(sccClusterRoleName)
    if err != nil {
        if errors.IsNotFound(err) {
            // Define the ClusterRole to allow use of the custom SCC
            _, err = rbacops.Instance().CreateClusterRole(&rbacv1.ClusterRole{
                ObjectMeta: metav1.ObjectMeta{
                    Name: sccClusterRoleName,
                },
                Rules: []rbacv1.PolicyRule{
                    {
                        APIGroups:     []string{"security.openshift.io"},
                        Resources:     []string{"securitycontextconstraints"},
                        ResourceNames: []string{sccName},
                        Verbs:         []string{"use"},
                    },
                },
            })
            if err != nil {
                return true, fmt.Errorf("failed to create ClusterRole %s: %v", sccClusterRoleName, err)
            }
            fmt.Printf("ClusterRole %s created successfully\n", sccClusterRoleName)
        } else {
            return true, fmt.Errorf("failed to check ClusterRole %s: %v", sccClusterRoleName, err)
        }
    } else {
        fmt.Printf("ClusterRole %s already exists\n", sccClusterRoleName)
    }

    // Step 3: Create ClusterRoleBinding for the service account
    clusterRoleBinding := &rbacv1.ClusterRoleBinding{
        ObjectMeta: metav1.ObjectMeta{
            Name: name + "-dac-override-binding",
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
        },
        RoleRef: rbacv1.RoleRef{
            APIGroup: rbacv1.GroupName,
            Kind:     "ClusterRole",
            Name:     sccClusterRoleName,
        },
        Subjects: []rbacv1.Subject{
            {
                Kind:      rbacv1.ServiceAccountKind,
                Name:      name,       // Service account name
                Namespace: namespace,  // Namespace where service account is located
            },
        },
    }
    _, err = rbacops.Instance().CreateClusterRoleBinding(clusterRoleBinding)
    if err != nil && !errors.IsAlreadyExists(err) {
        return true, fmt.Errorf("failed to create ClusterRoleBinding for %s/%s: %s", namespace, name, err)
    }

    fmt.Println("ClusterRoleBinding created successfully for DAC_OVERRIDE SCC")
    return false, nil

}

func getDynamicClient() (dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}
	return dynamic.NewForConfig(config)
}

var sccGVR = schema.GroupVersionResource{
	Group:    "security.openshift.io",
	Version:  "v1",
	Resource: "securitycontextconstraints",
}

func getSCC(sccName string) (*unstructured.Unstructured, error) {
	client, err := getDynamicClient()
	if err != nil {
		return nil, err
	}
	return client.Resource(sccGVR).Get(context.TODO(), sccName, metav1.GetOptions{})
}

func createSCC(sccName string) error {
	logrus.Debug("kartik Entering create scc with dynmaic client")
    client, err := getDynamicClient()
    if err != nil {
        return err
    }

    // Define the SCC using the security/v1 API types
    scc := &v1.SecurityContextConstraints{
        TypeMeta: metav1.TypeMeta{
			APIVersion: "security.openshift.io/v1",
			Kind:       "SecurityContextConstraints",
		},
		ObjectMeta: metav1.ObjectMeta{
            Name: sccName,
        },
		AllowedCapabilities: []corev1.Capability{
			"DAC_OVERRIDE",
		},
        RunAsUser: v1.RunAsUserStrategyOptions{
            Type: v1.RunAsUserStrategyRunAsAny,
        },
        SELinuxContext: v1.SELinuxContextStrategyOptions{
            Type: v1.SELinuxStrategyRunAsAny,
        },
        FSGroup: v1.FSGroupStrategyOptions{
            Type: v1.FSGroupStrategyRunAsAny,
        },
        SupplementalGroups: v1.SupplementalGroupsStrategyOptions{
            Type: v1.SupplementalGroupsStrategyRunAsAny,
        },
    }

    // Convert the SCC object to unstructured JSON for dynamic client
    sccUnstructured, err := toUnstructured(scc)
    if err != nil {
        return fmt.Errorf("failed to convert SCC to unstructured: %v", err)
    }

    // Create the SCC
    _, err = client.Resource(sccGVR).Create(context.TODO(), sccUnstructured, metav1.CreateOptions{})
    if err != nil {
        return fmt.Errorf("failed to create SCC %s: %v", sccName, err)
    }
    fmt.Printf("SCC %s created successfully\n", sccName)
    return nil
}

func toUnstructured(obj interface{}) (*unstructured.Unstructured, error) {
    data, err := json.Marshal(obj)
    if err != nil {
		logrus.Debugf("kartik error in marsshaling scc: %v", err)
        return nil, err
    }
    unstructuredObj := &unstructured.Unstructured{}
    if err := unstructuredObj.UnmarshalJSON(data); err != nil {
		logrus.Debugf("kartik error in unmarshaling scc: %v", err)
        return nil, err
    }
    return unstructuredObj, nil
}

func ensureDACOverrideSCC(sccName string) error {
    _, err := getSCC(sccName)
    if err != nil {
		logrus.Debugf("kartik error in getting scc: %v", err)
        if errors.IsNotFound(err) {
            return createSCC(sccName)
        }
        return fmt.Errorf("failed to get SCC %s: %v", sccName, err)
    }
    fmt.Printf("SCC %s already exists\n", sccName)
    return nil
}

// CleanServiceAccount removes a service account with a corresponding role and rolebinding.
func CleanServiceAccount(name, namespace string) error {
	if err := rbacops.Instance().DeleteRole(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s role: %s", namespace, name, err)
	}
	if err := rbacops.Instance().DeleteRoleBinding(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s rolebinding: %s", namespace, name, err)
	}
	if err := coreops.Instance().DeleteServiceAccount(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s serviceaccount: %s", namespace, name, err)
	}
	if err := rbacops.Instance().DeleteClusterRole(name); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s clusterrole: %s", namespace, name, err)
	}
	if err := rbacops.Instance().DeleteClusterRoleBinding(name); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s clusterrolebinding: %s", namespace, name, err)
	}
	return nil
}

// SetupNFSServiceAccount create a service account and bind it to a provided role.
func SetupNFSServiceAccount(name, namespace string, role *rbacv1.ClusterRole) error {
	if role != nil {
		role.Name, role.Namespace = name, namespace
		role.Annotations = map[string]string{
			SkipResourceAnnotation: "true",
		}
		if _, err := rbacops.Instance().CreateClusterRole(role); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s cluster role: %s", namespace, name, err)
		}
		if _, err := rbacops.Instance().CreateClusterRoleBinding(clusterRoleBindingFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s/%s cluster rolebinding: %s", namespace, name, err)
		}
	}
	var sa *corev1.ServiceAccount
	var err error
	if sa, err = coreops.Instance().CreateServiceAccount(serviceAccountFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s serviceaccount: %s", namespace, name, err)
	}
	var errMsg error
	// From 1.24.0 onwards service token does not support default secret token
	tokenSupported, err := isServiceAccountSecretMissing()
	if !tokenSupported {
		t := func() (interface{}, bool, error) {
			sa, err = coreops.Instance().GetServiceAccount(name, namespace)
			if err != nil {
				errMsg = fmt.Errorf("failed fetching sa [%v/%v]: %v", name, namespace, err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf("%v", errMsg)
			}
			if sa.Secrets == nil {
				logrus.Infof("Returned sa-secret null")
				errMsg = fmt.Errorf("secret token is missing in sa [%v/%v]", name, namespace)
				return "", true, fmt.Errorf("%v", errMsg)
			}
			return "", false, nil
		}
		if _, err := task.DoRetryWithTimeout(t, DefaultTimeout, ProgressCheckInterval); err != nil {
			eMsg := fmt.Errorf("max retries done, failed in fetching secret token of sa [%v/%v]: %v ", name, namespace, errMsg)
			logrus.Errorf("%v", eMsg)
			// Exhausted all retries
			return eMsg
		}

		tokenName := sa.Secrets[0].Name
		secretToken, err := coreops.Instance().GetSecret(tokenName, namespace)
		if err != nil {
			errMsg := fmt.Errorf("failed in getting secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
			logrus.Errorf("%v", errMsg)
			return errMsg
		}
		secretToken.Annotations[SkipResourceAnnotation] = "true"
		_, err = coreops.Instance().UpdateSecret(secretToken)
		if err != nil {
			errMsg := fmt.Errorf("failed in updating the secretToken [%v] of service account [%v/%v]: %v", tokenName, name, namespace, err)
			logrus.Errorf("%v", errMsg)
			return errMsg
		}
	}
	return nil
}

func roleBindingFor(name, namespace string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Name:     name,
			Kind:     "Role",
			APIGroup: rbacv1.GroupName,
		},
	}
}

func clusterRoleBindingFor(name, namespace string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Name:     name,
			Kind:     "ClusterRole",
			APIGroup: rbacv1.GroupName,
		},
	}
}

func serviceAccountFor(name, namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
	}
}

// In OCP standard scc cluster role name are predefined and one pod can adhere to one SCC at a time.
func roleBindingForScc(name, namespace string, sccClusterRoleName string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + SccRoleBindingNameSuffix,
			Namespace: namespace,
			Annotations: map[string]string{
				SkipResourceAnnotation: "true",
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Name:     sccClusterRoleName,
			Kind:     "ClusterRole",
			APIGroup: rbacv1.GroupName,
		},
	}
}
