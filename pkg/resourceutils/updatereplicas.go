package resourceutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-openapi/inflect"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/dynamic"
	"github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/util"
)

func ScaleReplicas(namespace string, activate bool, printFunc func(string, string), config *rest.Config) {
	updateStatefulSets(namespace, activate, printFunc)
	updateDeployments(namespace, activate, printFunc)
	updateDeploymentConfigs(namespace, activate, printFunc)
	updateIBPObjects("IBPPeer", namespace, activate, printFunc)
	updateIBPObjects("IBPCA", namespace, activate, printFunc)
	updateIBPObjects("IBPOrderer", namespace, activate, printFunc)
	updateIBPObjects("IBPConsole", namespace, activate, printFunc)
	updateVMObjects("VirtualMachine", namespace, true, printFunc)
	updateCRDObjects(namespace, activate, printFunc, config)
	updateCronJobObjects(namespace, activate, printFunc)
}

func updateStatefulSets(namespace string, activate bool, printFunc func(string, string)) {
	statefulSets, err := apps.Instance().ListStatefulSets(namespace, metav1.ListOptions{})
	if err != nil {
		util.CheckErr(err)
		return
	}
	for _, statefulSet := range statefulSets.Items {
		if replicas, update := getUpdatedReplicaCount(statefulSet.Annotations, activate, printFunc); update {
			statefulSet.Spec.Replicas = &replicas
			_, err := apps.Instance().UpdateStatefulSet(&statefulSet)
			if err != nil {
				printFunc(fmt.Sprintf("Error updating replicas for statefulset %v/%v : %v", statefulSet.Namespace, statefulSet.Name, err), "err")
				continue
			}
			printFunc(fmt.Sprintf("Updated replicas for statefulset %v/%v to %v", statefulSet.Namespace, statefulSet.Name, replicas), "out")
		}

	}
}

func updateDeployments(namespace string, activate bool, printFunc func(string, string)) {
	deployments, err := apps.Instance().ListDeployments(namespace, metav1.ListOptions{})
	if err != nil {
		util.CheckErr(err)
		return
	}
	for _, deployment := range deployments.Items {
		if replicas, update := getUpdatedReplicaCount(deployment.Annotations, activate, printFunc); update {
			deployment.Spec.Replicas = &replicas
			_, err := apps.Instance().UpdateDeployment(&deployment)
			if err != nil {
				printFunc(fmt.Sprintf("Error updating replicas for deployment %v/%v : %v", deployment.Namespace, deployment.Name, err), "err")
				continue
			}
			printFunc(fmt.Sprintf("Updated replicas for deployment %v/%v to %v", deployment.Namespace, deployment.Name, replicas), "out")
		}
	}
}

func updateDeploymentConfigs(namespace string, activate bool, printFunc func(string, string)) {
	deployments, err := openshift.Instance().ListDeploymentConfigs(namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			util.CheckErr(err)
		}
		return
	}
	for _, deployment := range deployments.Items {
		if replicas, update := getUpdatedReplicaCount(deployment.Annotations, activate, printFunc); update {
			deployment.Spec.Replicas = replicas
			_, err := openshift.Instance().UpdateDeploymentConfig(&deployment)
			if err != nil {
				printFunc(fmt.Sprintf("Error updating replicas for deploymentconfig %v/%v : %v", deployment.Namespace, deployment.Name, err), "err")
				continue
			}
			printFunc(fmt.Sprintf("Updated replicas for deploymentconfig %v/%v to %v", deployment.Namespace, deployment.Name, replicas), "out")
		}
	}
}

func updateCRDObjects(ns string, activate bool, printFunc func(string, string), config *rest.Config) {
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		util.CheckErr(err)
		return
	}
	configClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		util.CheckErr(err)
		return
	}

	ruleset := resourcecollector.GetDefaultRuleSet()

	for _, res := range crdList.Items {
		for _, crd := range res.Resources {
			var client k8sdynamic.ResourceInterface
			opts := &metav1.ListOptions{
				TypeMeta: metav1.TypeMeta{
					Kind:       crd.Kind,
					APIVersion: crd.Group + "/" + crd.Version},
			}
			gvk := schema.FromAPIVersionAndKind(opts.APIVersion, opts.Kind)
			resourceInterface := configClient.Resource(gvk.GroupVersion().WithResource(ruleset.Pluralize(strings.ToLower(gvk.Kind))))
			client = resourceInterface.Namespace(ns)
			objects, err := client.List(context.TODO(), *opts)
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				util.CheckErr(err)
				return
			}
			for _, o := range objects.Items {
				annotations := o.GetAnnotations()
				if annotations == nil {
					printFunc(fmt.Sprintf("Warn: Skipping CR update %s-%s/%s, annotations not found", strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName()), "err")
					continue
				}
				if crd.SuspendOptions.Path != "" {
					crd.NestedSuspendOptions = append(crd.NestedSuspendOptions, crd.SuspendOptions)
				}
				if len(crd.NestedSuspendOptions) == 0 {
					continue
				}
				for _, suspend := range crd.NestedSuspendOptions {
					specPath := strings.Split(suspend.Path, ".")
					if len(specPath) > 1 {
						var disableVersion interface{}
						if suspend.Type == "bool" {
							if val, err := strconv.ParseBool(suspend.Value); err != nil {
								disableVersion = !activate
							} else {
								disableVersion = val
								if activate {
									disableVersion = !val
								}
							}
						} else if suspend.Type == "int" {
							replicas, _ := getSuspendIntOpts(o.GetAnnotations(), activate, suspend.Path, printFunc)
							disableVersion = replicas
						} else if suspend.Type == "string" {
							suspend, err := getSuspendStringOpts(o.GetAnnotations(), activate, suspend.Path, printFunc)
							if err != nil {
								util.CheckErr(err)
								return
							}
							disableVersion = suspend
						} else {
							util.CheckErr(fmt.Errorf("invalid type %v to suspend cr", crd.SuspendOptions.Type))
							return
						}
						err := unstructured.SetNestedField(o.Object, disableVersion, specPath...)
						if err != nil {
							printFunc(fmt.Sprintf("Error updating \"%v\" for %v %v/%v to %v : %v", suspend.Path, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), disableVersion, err), "err")
							continue
						}
					}
				}
				_, err = client.Update(context.TODO(), &o, metav1.UpdateOptions{}, "")
				if err != nil {
					printFunc(fmt.Sprintf("Error updating CR %v %v/%v: %v", strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), err), "err")
					continue
				}
				printFunc(fmt.Sprintf("Updated CR for %v %v/%v", strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName()), "out")
				if !activate {
					if crd.PodsPath == "" {
						continue
					}
					podpath := strings.Split(crd.PodsPath, ".")
					pods, found, err := unstructured.NestedStringSlice(o.Object, podpath...)
					if err != nil {
						printFunc(fmt.Sprintf("Error getting pods for %v %v/%v : %v", strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), err), "err")
						continue
					}
					if !found {
						continue
					}
					for _, pod := range pods {
						err = core.Instance().DeletePod(o.GetNamespace(), pod, true)
						printFunc(fmt.Sprintf("Error deleting pod %v for %v %v/%v : %v", pod, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), err), "err")
						continue
					}
				}
			}

		}
	}
}

func updateIBPObjects(kind string, namespace string, activate bool, printFunc func(string, string)) {
	objects, err := dynamic.Instance().ListObjects(
		&metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       kind,
				APIVersion: "ibp.com/v1alpha1"},
		},
		namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			util.CheckErr(err)
		}
		return
	}
	for _, o := range objects.Items {
		if replicas, update := getUpdatedReplicaCount(o.GetAnnotations(), activate, printFunc); update {
			err := unstructured.SetNestedField(o.Object, int64(replicas), "spec", "replicas")
			if err != nil {
				printFunc(fmt.Sprintf("Error updating replicas for %v %v/%v : %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), err), "err")
				continue
			}
			_, err = dynamic.Instance().UpdateObject(&o)
			if err != nil {
				printFunc(fmt.Sprintf("Error updating replicas for %v %v/%v : %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), err), "err")
				continue
			}
			printFunc(fmt.Sprintf("Updated replicas for %v %v/%v to %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), replicas), "out")
		}
	}
}

func updateVMObjects(kind string, namespace string, activate bool, printFunc func(string, string)) {
	objects, err := dynamic.Instance().ListObjects(
		&metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       kind,
				APIVersion: "kubevirt.io/v1"},
		},
		namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			util.CheckErr(err)
		}
		return
	}
	for _, o := range objects.Items {
		path := []string{"spec", "running"}
		unstructured.RemoveNestedField(o.Object, path...)
		printFunc(fmt.Sprintf("Removed field for %v %v/%v", strings.ToLower(kind), o.GetNamespace(), o.GetName()), "out")
	}
}

func updateCronJobObjects(namespace string, activate bool, printFunc func(string, string)) {
	cronJobs, err := batch.Instance().ListCronJobs(namespace, metav1.ListOptions{})
	if err != nil {
		util.CheckErr(err)
		return
	}

	for _, cronJob := range cronJobs.Items {
		*cronJob.Spec.Suspend = !activate
		_, err = batch.Instance().UpdateCronJob(&cronJob)
		if err != nil {
			printFunc(fmt.Sprintf("Error updating suspend option for cronJob %v/%v : %v", cronJob.Namespace, cronJob.Name, err), "err")
			continue
		}
		printFunc(fmt.Sprintf("Updated suspend option for cronjob %v/%v to %v", cronJob.Namespace, cronJob.Name, !activate), "out")
	}

}
func getSuspendStringOpts(annotations map[string]string, activate bool, path string, printFunc func(string, string)) (string, error) {
	if val, present := annotations[migration.StorkAnnotationPrefix+path]; present {
		suspend := strings.Split(val, ",")
		if len(suspend) != 2 {
			return "", fmt.Errorf("migrated annotation does not have proper values %s/%s", migration.StorkAnnotationPrefix+path, val)
		}
		if activate {
			return suspend[0], nil
		}
		return suspend[1], nil
	}
	// for backward compatibility of old migrated cr's
	crdOpts := migration.StorkMigrationCRDActivateAnnotation
	if !activate {
		crdOpts = migration.StorkMigrationCRDDeactivateAnnotation
	}
	suspend, present := annotations[crdOpts]
	if !present {
		return "", fmt.Errorf("required migration annotation not found %s", crdOpts)
	}
	return suspend, nil
}

func getSuspendIntOpts(annotations map[string]string, activate bool, path string, printFunc func(string, string)) (int64, bool) {
	intOpts := ""
	if val, present := annotations[migration.StorkAnnotationPrefix+path]; present {
		intOpts = strings.Split(val, ",")[0]
	} else if val, present := annotations[migration.StorkMigrationCRDActivateAnnotation]; present {
		// for old migrated cr compatibility
		intOpts = val
	} else {
		return 0, false
	}
	var replicas int64
	if activate {
		parsedReplicas, err := strconv.Atoi(intOpts)
		if err != nil {
			printFunc(fmt.Sprintf("Error parsing replicas for app : %v", err), "err")
			return 0, false
		}
		replicas = int64(parsedReplicas)
	} else {
		replicas = 0
	}
	return replicas, true
}

func getUpdatedReplicaCount(annotations map[string]string, activate bool, printFunc func(string, string)) (int32, bool) {
	if replicas, present := annotations[migration.StorkMigrationReplicasAnnotation]; present {
		var updatedReplicas int32
		if activate {
			parsedReplicas, err := strconv.Atoi(replicas)
			if err != nil {
				printFunc(fmt.Sprintf("Error parsing replicas for app : %v", err), "err")
				return 0, false
			}
			updatedReplicas = int32(parsedReplicas)
		} else {
			updatedReplicas = 0
		}
		return updatedReplicas, true
	}

	return 0, false
}
