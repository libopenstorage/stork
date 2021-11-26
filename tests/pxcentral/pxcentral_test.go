package tests

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/osutils"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultTimeout       = 1 * time.Minute
	defaultRetryInterval = 1 * time.Second
	appReadinessTimeout  = 20 * time.Minute
)

// To run tests here, k8s ConfigMaps need to be created for each app before starting torpedo
// ConfigMap names are: px-central, px-license-server and px-monitor
// the Date field should contains helm chart url, helm values (new url, repo and chart name for upgrade tests)
// for simple installation, only install-url and values are needed, while for upgrade test here is an example:
// install-url: http://charts.portworx.io/
// upgrade-url: https://raw.githubusercontent.com/portworx/helm/master/repo/staging (staging chart to upgrade to)
// values: persistentStorage.storageClassName=central-sc,persistentStorage.enabled=true
// repo-name: portworx-staging (create a new staging repo for upgrade)
// chart-name: px-central (if the chart name for release installed is changed, e.g. px-backup to px-central)
func TestPxcentral(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : px-central", specReporters)
}

// install px-central 1.2.3+
func installPxcentral(centralOptions, lsOptions, monitorOptions *scheduler.ScheduleOptions, backupEnable, configMutable bool) *scheduler.Context {
	var context *scheduler.Context
	Step("Install px-central using the px-backup helm chart then validate", func() {
		if !backupEnable {
			appName := centralOptions.AppKeys[0]
			configMap, err := core.Instance().GetConfigMap(appName, "default")
			configMap.Data[k8s.HelmExtraValues] = "pxbackup.enabled=false"
			_, err = core.Instance().UpdateConfigMap(configMap)
			Expect(err).NotTo(HaveOccurred())

			// cleanup extra values after installation
			defer func() {
				if !configMutable {
					configMap, err := core.Instance().GetConfigMap(appName, "default")
					configMap.Data[k8s.HelmExtraValues] = ""
					_, err = core.Instance().UpdateConfigMap(configMap)
					Expect(err).NotTo(HaveOccurred())
				}
			}()
		}

		contexts, err := Inst().S.Schedule(Inst().InstanceID, *centralOptions)
		Expect(err).NotTo(HaveOccurred())
		Expect(contexts).NotTo(BeEmpty())

		// Skipping volume validation until other volume providers are implemented.
		// Also change the app readinessTimeout to 20 mins
		context = contexts[0]
		context.SkipVolumeValidation = true
		context.ReadinessTimeout = appReadinessTimeout

		ValidateContext(context)
		logrus.Infof("Successfully validated specs for px-central")
	})

	if lsOptions != nil {
		Step("Install px-license-server then validate", func() {
			// label px/ls=true on 2 worker nodes
			for i, node := range node.GetWorkerNodes() {
				if i == 2 {
					break
				}
				err := Inst().S.AddLabelOnNode(node, "px/ls", "true")
				Expect(err).NotTo(HaveOccurred())
			}

			err := Inst().S.AddTasks(context, *lsOptions)
			Expect(err).NotTo(HaveOccurred())

			ValidateContext(context)
			logrus.Infof("Successfully validated specs for px-license-server")
		})
	}

	if monitorOptions != nil {
		appName := monitorOptions.AppKeys[0]
		Step("Install px-monitor then validate", func() {
			var endpoint, oidcSecret string
			Step("Getting px-backup UI endpoint IP:PORT", func() {
				endpointIP := node.GetNodes()[0].GetMgmtIp()

				serviceObj, err := core.Instance().GetService("px-backup-ui", context.ScheduleOptions.Namespace)
				Expect(err).NotTo(HaveOccurred())
				endpointPort := serviceObj.Spec.Ports[0].NodePort

				endpoint = fmt.Sprintf("%s:%v", endpointIP, endpointPort)
				logrus.Infof("Got px-backup-ui endpoint: %s", endpoint)
			})

			Step("Getting OIDC client secret", func() {
				secretObj, err := core.Instance().GetSecret("pxc-backup-secret", context.ScheduleOptions.Namespace)
				Expect(err).NotTo(HaveOccurred())

				secretData, exist := secretObj.Data["OIDC_CLIENT_SECRET"]
				Expect(exist).To(Equal(true))
				oidcSecret = base64.StdEncoding.EncodeToString(secretData)
				logrus.Infof("Got OIDC client secret: %s", oidcSecret)
			})

			Step("Adding extra values to helm ConfigMap", func() {
				configMap, err := core.Instance().GetConfigMap(appName, "default")
				Expect(err).NotTo(HaveOccurred())

				configMap.Data[k8s.HelmExtraValues] = fmt.Sprintf("pxmonitor.pxCentralEndpoint=%s,pxmonitor.oidcClientSecret=%s",
					endpoint,
					oidcSecret)
				configMap, err = core.Instance().UpdateConfigMap(configMap)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("Got extra helm values for px-monitor: %s", configMap.Data[k8s.HelmExtraValues])
			})

			Step("Install px-monitor", func() {
				err := Inst().S.AddTasks(context, *monitorOptions)
				Expect(err).NotTo(HaveOccurred())

				ValidateContext(context)
				logrus.Infof("Successfully validated specs for px-monitor")

				// remove extra values in config map
				if !configMutable {
					configMap, err := core.Instance().GetConfigMap(appName, "default")
					configMap.Data[k8s.HelmExtraValues] = ""
					configMap, err = core.Instance().UpdateConfigMap(configMap)
					Expect(err).NotTo(HaveOccurred())
				}
			})
		})
	}

	return context
}

// deleteAndWait waiting for resources to be deleted
func deleteAndWait(resource interface{}) error {
	if obj, ok := resource.(*corev1.Namespace); ok {
		t := func() (interface{}, bool, error) {
			err := core.Instance().DeleteNamespace(obj.Name)

			if err != nil && strings.Contains(err.Error(), "not found") {
				return nil, false, err
			}
			return nil, true, fmt.Errorf("namespace %s not deleted", obj.Name)
		}

		_, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not found"))
		logrus.Infof("namespace %s deleted", obj.Name)
	} else if obj, ok := resource.(*batchv1.Job); ok {
		t := func() (interface{}, bool, error) {
			err := batch.Instance().DeleteJob(obj.Name, obj.Namespace)

			if err != nil && strings.Contains(err.Error(), "not found") {
				return nil, false, err
			}
			return nil, true, fmt.Errorf("job %s not deleted", obj.Name)
		}

		_, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("not found"))
		logrus.Infof("job %s deleted", obj.Name)
	} else {
		return errors.New("resource type not implemented")
	}

	return nil
}

func destroyPxcentral(context *scheduler.Context) {
	opts := make(map[string]bool)
	opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

	TearDownContext(context, opts)
	ns, err := core.Instance().GetNamespace(context.ScheduleOptions.Namespace)
	Expect(err).NotTo(HaveOccurred())

	if ns.Name != "default" {
		err = deleteAndWait(ns)
		Expect(err).NotTo(HaveOccurred())
	}

	logrus.Infof("Successfully destroyed px-central")
}

// All specs got deleted that would be validated during upgrade from 1.2.3+ to 1.3.0+
func getSpecsToDeleteForCentralUpgrade(lsEnabled, monitorEnabled bool) []interface{} {
	var specs []interface{}
	specs = append(specs,
		&corev1.Service{
			ObjectMeta: v1.ObjectMeta{Name: "px-backup"},
		},
		&corev1.Service{
			ObjectMeta: v1.ObjectMeta{Name: "pxc-backup-etcd"},
		},
		&corev1.Service{
			ObjectMeta: v1.ObjectMeta{Name: "pxc-backup-etcd-headless"},
		},
		&corev1.Service{
			ObjectMeta: v1.ObjectMeta{Name: "px-backup-ui"},
		},
		&appsapi.Deployment{
			ObjectMeta: v1.ObjectMeta{Name: "px-backup"},
		},
		&appsapi.StatefulSet{
			ObjectMeta: v1.ObjectMeta{Name: "pxc-backup-etcd"},
		})

	if lsEnabled {
		specs = append(specs,
			&appsapi.Deployment{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-license-server"},
			},
			&batchv1.Job{ObjectMeta: v1.ObjectMeta{Name: "pxcentral-license-ha-setup"}})
	}

	if monitorEnabled {
		specs = append(specs,
			&appsapi.Deployment{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-prometheus-operator"},
			},
			&batchv1.Job{ObjectMeta: v1.ObjectMeta{Name: "pxcentral-monitor-post-install-setup"}},
			&appsapi.StatefulSet{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-memcached-index-read"},
			},
			&appsapi.StatefulSet{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-memcached-index-write"},
			},
			&appsapi.StatefulSet{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-memcached"},
			},
			&appsapi.StatefulSet{
				ObjectMeta: v1.ObjectMeta{Name: "pxcentral-cortex-consul"},
			})
	}

	return specs
}

func kubectlAnnotate(resourceType, namespace, release string, resourceList []string) {
	for _, name := range resourceList {
		annotation := fmt.Sprintf("meta.helm.sh/release-name=%s", release)
		err := osutils.Kubectl([]string{"-n", namespace, "annotate", resourceType, name, annotation, "--overwrite"})
		Expect(err).NotTo(HaveOccurred())
		// this annotation is missing during installation by helm sdk and not needed in the actual migration script
		// for torpedo automation test only
		annotation = fmt.Sprintf("meta.helm.sh/release-namespace=%s", namespace)
		err = osutils.Kubectl([]string{"-n", namespace, "annotate", resourceType, name, annotation, "--overwrite"})
		Expect(err).NotTo(HaveOccurred())
	}
}

func kubectlDelete(resourceType, namespace string, resourceList []string) {
	for _, name := range resourceList {
		err := osutils.Kubectl([]string{"-n", namespace, "delete", resourceType, name})
		Expect(err).NotTo(HaveOccurred())
	}
}

func annotateLS(context *scheduler.Context, releaseName string) {
	ns := context.ScheduleOptions.Namespace

	Step("Changing the annotations of existing px-license-server components", func() {
		secretListLS := []string{"px-license-secret"}
		cmListLS := []string{"pxcentral-ls-configmap"}
		svcListLS := []string{"pxcentral-license"}
		deploymentListLS := []string{"pxcentral-license-server"}
		deploymentListLSDelete := []string{"pxcentral-license-server"}

		kubectlAnnotate("secret", ns, releaseName, secretListLS)
		kubectlAnnotate("configmap", ns, releaseName, cmListLS)
		kubectlAnnotate("service", ns, releaseName, svcListLS)
		kubectlAnnotate("deployment", ns, releaseName, deploymentListLS)
		kubectlDelete("deployment", ns, deploymentListLSDelete)
	})
}

func annotateMonitor(context *scheduler.Context, releaseName string) {
	ns := context.ScheduleOptions.Namespace

	Step("Changing annotations of px-monitor components", func() {
		saListMonitor := []string{"px-monitor", "pxcentral-prometheus", "pxcentral-prometheus-operator"}
		secretListMonitor := []string{"pxcentral-cortex", "pxcentral-cortex-cassandra"}
		cmListMonitor := []string{"pxcentral-cortex-nginx", "pxcentral-grafana-dashboard-config", "pxcentral-grafana-dashboards", "pxcentral-grafana-ini-config", "pxcentral-grafana-source-config", "pxcentral-monitor-configmap"}
		pvcListMonitor := []string{"pxcentral-grafana-dashboards", "pxcentral-grafana-datasource", "pxcentral-mysql-pvc"}
		clusterroleListMonitor := []string{"pxcentral-prometheus", "pxcentral-prometheus-operator"}
		clusterrolebindingListMonitor := []string{"pxcentral-prometheus", "pxcentral-prometheus-operator"}
		roleListMonitor := []string{"px-monitor", "pxcentral-cortex"}
		rolebindingListMonitor := []string{"px-monitor", "pxcentral-cortex"}
		svcListMonitor := []string{"pxcentral-cortex-cassandra-headless", "pxcentral-cortex-cassandra", "pxcentral-memcached-index-read", "pxcentral-memcached-index-write", "pxcentral-memcached", "pxcentral-cortex-alertmanager-headless", "pxcentral-cortex-alertmanager", "pxcentral-cortex-configs", "pxcentral-cortex-distributor", "pxcentral-cortex-ingester", "pxcentral-cortex-querier", "pxcentral-cortex-query-frontend-headless", "pxcentral-cortex-consul", "pxcentral-cortex-query-frontend", "pxcentral-cortex-ruler", "pxcentral-cortex-table-manager", "pxcentral-prometheus"}
		stsListMonitor := []string{"pxcentral-cortex-cassandra", "pxcentral-memcached-index-read", "pxcentral-memcached-index-write", "pxcentral-memcached", "pxcentral-cortex-consul", "pxcentral-cortex-alertmanager", "pxcentral-cortex-ingester"}
		deploymentListMonitor := []string{"pxcentral-cortex-configs", "pxcentral-cortex-distributor", "pxcentral-cortex-nginx", "pxcentral-cortex-querier", "pxcentral-cortex-query-frontend", "pxcentral-cortex-ruler", "pxcentral-cortex-table-manager", "pxcentral-grafana", "pxcentral-prometheus-operator"}
		prometheusMonitor := []string{"pxcentral-prometheus"}
		prometheusruleMonitor := []string{"prometheus-portworx-rules-portworx.rules.yaml"}
		servicemonitorListMonitor := []string{"pxcentral-portworx", "px-backup"}
		statefulsetListMonitorDelete := []string{"pxcentral-memcached-index-read", "pxcentral-memcached-index-write", "pxcentral-memcached", "pxcentral-cortex-consul"}
		deploymentListMonitorDelete := []string{"pxcentral-prometheus-operator"}

		kubectlAnnotate("serviceaccount", ns, releaseName, saListMonitor)
		kubectlAnnotate("secret", ns, releaseName, secretListMonitor)
		kubectlAnnotate("configmap", ns, releaseName, cmListMonitor)
		kubectlAnnotate("pvc", ns, releaseName, pvcListMonitor)
		kubectlAnnotate("clusterrole", ns, releaseName, clusterroleListMonitor)
		kubectlAnnotate("clusterrolebinding", ns, releaseName, clusterrolebindingListMonitor)
		kubectlAnnotate("role", ns, releaseName, roleListMonitor)
		kubectlAnnotate("rolebinding", ns, releaseName, rolebindingListMonitor)
		kubectlAnnotate("service", ns, releaseName, svcListMonitor)
		kubectlAnnotate("statefulset", ns, releaseName, stsListMonitor)
		kubectlAnnotate("deployment", ns, releaseName, deploymentListMonitor)
		kubectlAnnotate("prometheus", ns, releaseName, prometheusMonitor)
		kubectlAnnotate("prometheusrule", ns, releaseName, prometheusruleMonitor)
		kubectlAnnotate("servicemonitor", ns, releaseName, servicemonitorListMonitor)
		kubectlDelete("statefulset", ns, statefulsetListMonitorDelete)
		kubectlDelete("deployment", ns, deploymentListMonitorDelete)
	})
}

var _ = BeforeSuite(func() {
	logrus.Infof("Init instance")
	InitInstance()
})

// This test performs basic test of installing px-central with helm
// px-license-server and px-minotor will be installed after px-central is validated
var _ = Describe("{InstallCentral}", func() {
	It("has to setup, validate and teardown apps", func() {
		var context *scheduler.Context

		centralApp := "px-central"
		centralOptions := scheduler.ScheduleOptions{
			AppKeys:            []string{centralApp},
			StorageProvisioner: Inst().Provisioner,
		}

		//lsApp := "px-license-server"
		//lsOptions := scheduler.ScheduleOptions{
		//	AppKeys:            []string{lsApp},
		//	StorageProvisioner: Inst().Provisioner,
		//}

		//monitorApp := "px-monitor"
		//monitorOptions := scheduler.ScheduleOptions{
		//	AppKeys:            []string{monitorApp},
		//	StorageProvisioner: Inst().Provisioner,
		//}

		// Setting license server and monitor options to empty for now

		Step("Install px-central, px-license-server and px-monitor", func() {
			context = installPxcentral(&centralOptions, nil, nil, true, true)
		})

		//Step("Uninstall license server and monitoring", func() {
		//	err := Inst().S.ScheduleUninstall(context, scheduler.ScheduleOptions{})
		//	Expect(err).NotTo(HaveOccurred())
		//	err = Inst().S.ScheduleUninstall(context, scheduler.ScheduleOptions{})
		//	Expect(err).NotTo(HaveOccurred())

		//	ValidateContext(context)
		//	logrus.Infof("Successfully uninstalled px-license-server and px-monitor")
		//})

		Step("destroy apps", func() {
			destroyPxcentral(context)
		})
	})
})

// This test installs px-central from release repo then upgrade using staging repo
// testing 1.2.3 -> 1.3.0 multi charts to single chart upgrade
var _ = Describe("{UpgradeCentralSingle}", func() {
	It("has to setup, upgrade, validate and teardown apps", func() {
		var context *scheduler.Context

		centralApp := "px-central"
		centralOptions := scheduler.ScheduleOptions{
			AppKeys:            []string{centralApp},
			StorageProvisioner: Inst().Provisioner,
			// Due to a known helm SDK bug, resources could be created in default namespace
			// During helm upgrade k8s resources could be installed into default ns due to a helm SDK known bug:
			// https://github.com/helm/helm/issues/8780
			// Current workaround is to install everything in default namespace, helm cli is not affected
			Namespace: "default",
		}

		//lsApp := "px-license-server"
		//lsOptions := scheduler.ScheduleOptions{
		//	AppKeys:            []string{lsApp},
		//	StorageProvisioner: Inst().Provisioner,
		//}

		//monitorApp := "px-monitor"
		//monitorOptions := scheduler.ScheduleOptions{
		//	AppKeys:            []string{monitorApp},
		//	StorageProvisioner: Inst().Provisioner,
		//}

		// Setting license server and monitor options to empty for now

		Step("Install px-central using 1.2.x helm repo then validate", func() {
			context = installPxcentral(&centralOptions, nil, nil, true, true)
		})

		Step("Prepare for helm upgrade", func() {
			// set a different helm chart repo name for staging chart
			// and update the chart name from px-backup to px-central
			centralConfigMap, err := core.Instance().GetConfigMap(centralApp, "default")
			Expect(err).NotTo(HaveOccurred())

			centralConfigMap.Data[k8s.HelmRepoName] = "portworx-staging"
			centralConfigMap.Data[k8s.HelmChartName] = centralApp

			// default px-license-server is the same as px-backup
			// extra values are endpoint and oidc for px-monitor
			//monitorConfigMap, err := core.Instance().GetConfigMap(monitorApp, "default")
			//Expect(err).NotTo(HaveOccurred())

			//extraValues := monitorConfigMap.Data[k8s.HelmExtraValues]
			enableFlags := "pxbackup.enabled=true,pxmonitor.enabled=true,pxlicenseserver.enabled=true"
			backupDBFlags := "pxbackup.mongoMigration=incomplete,pxbackup.datastore=mongodb,pxbackup.retainEtcd=true"
			extraValues := fmt.Sprintf("%s,%s", enableFlags, backupDBFlags)
			centralConfigMap.Data[k8s.HelmExtraValues] = extraValues
			_, err = core.Instance().UpdateConfigMap(centralConfigMap)
			Expect(err).NotTo(HaveOccurred())

			// changing annotations according to the migration script
			// https://github.com/portworx/helm/blob/1.3.0/single_chart_migration/migration.sh
			//annotateLS(context, centralApp)
			//annotateMonitor(context, centralApp)

			job, err := batch.Instance().GetJob("pxcentral-post-install-hook", context.ScheduleOptions.Namespace)
			Expect(err).NotTo(HaveOccurred())
			err = deleteAndWait(job)
			Expect(err).NotTo(HaveOccurred())

			// objects in spec list will be deleted during upgrade
			deleteSpecs := getSpecsToDeleteForCentralUpgrade(true, true)
			err = Inst().S.RemoveAppSpecsByName(context, deleteSpecs)
		})

		Step("Upgrade px-central using single chart helm repo then validate", func() {
			centralOptions.Upgrade = true
			err := Inst().S.AddTasks(context, centralOptions)
			Expect(err).NotTo(HaveOccurred())

			// delete old stale jobs
			job, err := batch.Instance().GetJob("pxcentral-monitor-post-install-setup", context.ScheduleOptions.Namespace)
			Expect(err).NotTo(HaveOccurred())
			err = deleteAndWait(job)
			Expect(err).NotTo(HaveOccurred())
			job, err = batch.Instance().GetJob("pxcentral-license-ha-setup", context.ScheduleOptions.Namespace)
			Expect(err).NotTo(HaveOccurred())
			err = deleteAndWait(job)
			Expect(err).NotTo(HaveOccurred())

			ValidateContext(context)

			// reset configmap
			configMap, err := core.Instance().GetConfigMap(centralApp, "default")
			configMap.Data[k8s.HelmExtraValues] = ""
			configMap, err = core.Instance().UpdateConfigMap(configMap)
			Expect(err).NotTo(HaveOccurred())

			logrus.Infof("Successfully upgraded px-central to staging version")
		})

		Step("destroy apps", func() {
			destroyPxcentral(context)
		})
	})
})

var _ = Describe("{InstallCentralWithoutBackup}", func() {
	It("has to setup, validate and teardown apps", func() {
		var context *scheduler.Context

		centralApp := "px-central"
		centralOptions := scheduler.ScheduleOptions{
			AppKeys:            []string{centralApp},
			StorageProvisioner: Inst().Provisioner,
		}

		Step("Install px-central with px-backup disabled then validate", func() {
			context = installPxcentral(&centralOptions, nil, nil, false, false)
		})

		Step("destroy apps", func() {
			destroyPxcentral(context)
		})
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
