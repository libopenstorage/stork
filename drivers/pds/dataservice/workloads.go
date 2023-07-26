package dataservice

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/pkg/log"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"strings"
	"time"
)

func createServiceAccount(namespace string) (*corev1.ServiceAccount, error) {
	serviceAccountName := "pds-loadgen"
	serviceAccount := &corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{
			Kind: "ServiceAccount",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceAccountName,
			Namespace: namespace,
		},
	}

	_, err = k8sCore.GetServiceAccount(serviceAccountName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Creating Service account..")
			serviceAccount, err = k8sCore.CreateServiceAccount(serviceAccount)
			if err != nil {
				return nil, fmt.Errorf("Error while creating service account %v", err)
			}
			log.InfoD("Service Account %s Created..", serviceAccount.Name)
		} else {
			return nil, fmt.Errorf("Error while creating service account %v", err)
		}
	} else {
		log.Info("Service account already exists")
	}
	return serviceAccount, err
}

func createRole(namespace string, account *corev1.ServiceAccount) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      account.Name,
			Namespace: namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{"policy"},
				Resources:     []string{"podsecuritypolicies"},
				ResourceNames: []string{"pds-restricted"},
				Verbs:         []string{"use"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"services"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/exec"},
				Verbs:     []string{"create"},
			},
		},
	}
	_, err := k8sRbac.GetRole(account.Name, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Creating Roles..")
			_, err = k8sRbac.CreateRole(role)
			if err != nil {
				return fmt.Errorf("Error while creating role %v", err)
			}
			log.InfoD("Role Created..")
		} else {
			return fmt.Errorf("Error while creating roles %v", err)
		}
	} else {
		log.Info("Roles already exists")
	}
	return err
}

func createRoleBinding(namespace string, account *corev1.ServiceAccount) error {
	roleBindingName := "pds-loadgen:pds-loadgen"
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pds-loadgen:pds-loadgen",
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      account.Name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "Role",
			Name:     "pds-loadgen",
			APIGroup: "rbac.authorization.k8s.io",
		},
	}
	_, err = k8sRbac.GetRoleBinding(roleBindingName, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Creating Role Bindings..")
			_, err = k8sRbac.CreateRoleBinding(roleBinding)
			if err != nil {
				return fmt.Errorf("Error while creating role bindings %v", err)
			}
			log.InfoD("RoleBindings Created..")
		} else {
			return fmt.Errorf("Error while creating role bindings %v", err)
		}
	} else {
		log.Info("RoleBindings already exists")
	}

	return err
}

func createClusterRole() (*rbacv1.ClusterRole, error) {
	clusterRoleName := "pds-loadgen-cluster"
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"deployments.pds.io"},
				Resources: []string{"databases"},
				Verbs:     []string{"get", "list"},
			},
		},
	}
	clusterrole, err := k8sRbac.GetClusterRole(clusterRoleName)
	if err != nil {
		if errors.IsNotFound(err) {
			log.InfoD("Creating ClusterRoles..")
			clusterrole, err = k8sRbac.CreateClusterRole(clusterRole)
			if err != nil {
				return nil, fmt.Errorf("Error while creating cluster role %v", err)
			}
			log.InfoD("ClusterRole %s created successfully", clusterrole.Name)
		} else {
			return nil, fmt.Errorf("Error while creating cluster role %v", err)
		}
	} else {
		log.InfoD("ClusterRoles already exists")
	}
	return clusterrole, err
}

func createClusterRoleBinding(namespace string, account *corev1.ServiceAccount, clusterRole *rbacv1.ClusterRole) error {
	clusterRoleBindingName := "pds-loadgen:pds-loadgen-c"

	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterRoleBindingName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      account.Name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			Name:     clusterRole.Name,
			APIGroup: "rbac.authorization.k8s.io",
		},
	}

	_, err := k8sRbac.GetClusterRoleBinding(clusterRoleBindingName)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Creating ClusterRole Bindings..")
			_, err = k8sRbac.CreateClusterRoleBinding(clusterRoleBinding)
			if err != nil {
				return fmt.Errorf("Error while creating cluster role bindings %v", err)
			}
			log.InfoD("ClusterRole Binding Created..")
		} else {
			return fmt.Errorf("Error while creating cluster role bindings %v", err)
		}
	} else {
		log.Info("ClusterRoleBindings already exists")
	}
	return err
}

func createPolicies(namespace string) (*corev1.ServiceAccount, error) {
	serviceAccount, err := createServiceAccount(namespace)
	if err != nil {
		return nil, fmt.Errorf("error while creating service Account %v", err)
	}

	err = createRole(namespace, serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("error while creating role %v", err)
	}

	err = createRoleBinding(namespace, serviceAccount)
	if err != nil {
		return nil, fmt.Errorf("error while creating rolebinding %v", err)
	}

	clusterRole, err := createClusterRole()
	if err != nil {
		return nil, fmt.Errorf("error while creating cluster role %v", err)
	}

	err = createClusterRoleBinding(namespace, serviceAccount, clusterRole)
	if err != nil {
		return nil, fmt.Errorf("error while creating cluster rolebinding %v", err)
	}
	return serviceAccount, nil
}

// InsertDataAndReturnChecksum Inserts Data into the db and returns the checksum
func (ds *DataserviceType) InsertDataAndReturnChecksum(pdsDeployment *pds.ModelsDeployment, wkloadGenParams pdsdriver.LoadGenParams) (string, *v1.Deployment, error) {
	wkloadGenParams.Mode = "write"
	ckSum, wlDep, err := ds.GenerateWorkload(pdsDeployment, wkloadGenParams)
	return ckSum, wlDep, err
}

// ReadDataAndReturnChecksum Reads Data from the db and returns the checksum
func (ds *DataserviceType) ReadDataAndReturnChecksum(pdsDeployment *pds.ModelsDeployment, wkloadGenParams pdsdriver.LoadGenParams) (string, *v1.Deployment, error) {
	wkloadGenParams.Mode = "read"
	ckSum, wlDep, err := ds.GenerateWorkload(pdsDeployment, wkloadGenParams)
	return ckSum, wlDep, err
}

// ValidateDataMd5Hash validates the hash of the data service deployments
func (ds *DataserviceType) ValidateDataMd5Hash(deploymentHash, restoredDepHash map[string]string) bool {
	count := 0

	//Debug block to print hash of the database table
	for depName, hash := range deploymentHash {
		log.Debugf("Dep name %s and hash %s", depName, hash)
	}
	for depName, hash := range restoredDepHash {
		log.Debugf("Restored Dep name %s and hash %s", depName, hash)
	}

	for key, depHash := range deploymentHash {
		depName, _, _ := strings.Cut(key, "-")
		for key1, resDepHash := range restoredDepHash {
			resDepName, _, _ := strings.Cut(key1, "-")
			if depName == resDepName && depHash == resDepHash {
				log.InfoD("data is consistent for restored deployment %s", key1)
				count += 1
			}
		}
	}
	if count != len(restoredDepHash) {
		return false
	}
	return true
}

// GenerateWorkload creates a deployment using the given params(perform read/write) and returns the checksum
func (ds *DataserviceType) GenerateWorkload(pdsDeployment *pds.ModelsDeployment,
	wkloadGenParams pdsdriver.LoadGenParams) (string, *v1.Deployment, error) {

	var checksum string
	dsName := pdsDeployment.GetClusterResourceName()
	workloadDepName := wkloadGenParams.LoadGenDepName
	namespace := wkloadGenParams.Namespace
	failOnError := wkloadGenParams.FailOnError
	mode := wkloadGenParams.Mode
	seed := wkloadGenParams.TableName
	counts := wkloadGenParams.NumOfRows
	iterations := wkloadGenParams.Iterations
	timeout := wkloadGenParams.Timeout
	replicas := wkloadGenParams.Replicas
	replacePassword := wkloadGenParams.ReplacePassword
	clusterMode := wkloadGenParams.ClusterMode

	serviceAccount, err := createPolicies(namespace)
	if err != nil {
		return "", nil, fmt.Errorf("error while creating policies")
	}

	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: workloadDepName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": workloadDepName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": workloadDepName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "main",
							Image:           pdsWorkloadImage,
							ImagePullPolicy: "Always",
							Env: []corev1.EnvVar{
								{Name: "PDS_DEPLOYMENT", Value: dsName},
								{Name: "NAMESPACE", Value: namespace},
								{Name: "FAIL_ON_ERROR", Value: failOnError},
								{Name: "MODE", Value: mode},
								{Name: "SEED", Value: seed},
								{Name: "COUNTS", Value: counts},
								{Name: "ITERATIONS", Value: iterations},
								{Name: "TIMEOUT", Value: timeout},
								{Name: "REPLACE_PASSWORD", Value: replacePassword},
								{Name: "CLUSTER_MODE", Value: clusterMode},
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								RunAsNonRoot:             boolPtr(true),
								RunAsUser:                int64Ptr(1000),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{
										"ALL",
									},
								},
							},
						},
					},
					ServiceAccountName: serviceAccount.Name,
				},
			},
		},
	}
	log.Debugf("Deployment spec %+v", deploymentSpec)
	wlDeployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("error Occured while creating deployment %v", err)
	}
	err = k8sApps.ValidateDeployment(wlDeployment, timeOut, 10*time.Second)
	if err != nil {
		return "", nil, fmt.Errorf("error Occured while validating the pod %v", err)
	}
	podList, err := k8sCore.GetPods(wlDeployment.Namespace, nil)
	if err != nil {
		return "", nil, fmt.Errorf("error Occured while getting the podlist %v", err)
	}
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, wlDeployment.Name) {
			log.Debugf("workload pod name %s", pod.Name)
			checksum, err = ReadChecksum(pod.Name, wlDeployment.Namespace, mode)
			if err != nil {
				return "", nil, fmt.Errorf("error Occured while fetching checksum %v", err)
			}
		}
	}
	return checksum, wlDeployment, nil
}

func ReadChecksum(podName, namespace, mode string) (string, error) {
	var checksum string

	log.InfoD("%s operation started...", mode)
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		logs, err := k8sCore.GetPodLog(podName, namespace, &corev1.PodLogOptions{})
		if err != nil {
			return false, fmt.Errorf("error while fetching the pod logs: %v", err)
		}
		log.Infof("%s operation is in progress...", mode)
		if strings.Contains(logs, "Checksum") {
			for _, line := range strings.Split(strings.TrimRight(logs, "\n"), "\n") {
				if strings.Contains(line, "Checksum") {
					words := strings.Split(line, ":")
					checksum = words[1]
					return true, nil
				}
			}
		}
		return false, nil
	})
	log.InfoD("%s operation completed...", mode)
	log.InfoD("Checksum of the table is %s", checksum)
	return checksum, err
}

func int64Ptr(i int64) *int64 {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}
