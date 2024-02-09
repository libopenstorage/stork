package tests

import (
	"fmt"
	ops_v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/openshift"
	"github.com/portworx/torpedo/pkg/log"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/url"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{UpgradeCluster}", func() {
	var contexts []*scheduler.Context

	JustBeforeEach(func() {
		tags := map[string]string{
			"upgradeCluster": "true",
		}
		StartTorpedoTest("UpgradeCluster", "Upgrade cluster test", tags, 0)
	})
	It("upgrade scheduler and ensure everything is running fine", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradecluster-%d", i))...)
		}

		ValidateApplications(contexts)

		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		Expect(versions).NotTo(BeEmpty())
		stopSignal := make(chan struct{})
		var mError error
		go getClusterNodesInfo(stopSignal, &mError)

		defer func() {
			close(stopSignal)
		}()

		for _, version := range versions {
			if Inst().S.String() == openshift.SchedName && strings.Contains(version, "4.14") {
				err = ocp414Prereq()
				log.FailOnError(err, fmt.Sprintf("error running OCP pre-requisites for version:%s", version))
			}
			Step("start scheduler upgrade", func() {
				err := Inst().S.UpgradeScheduler(version)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify upgrade to %s is successful", version))
			})

			Step("validate storage components", func() {
				urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				u, err := url.Parse(urlToParse)
				log.FailOnError(err, fmt.Sprintf("error parsing the url: %s", urlToParse))
				err = Inst().V.ValidateDriver(u.String(), true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))
			})
			dash.VerifyFatal(mError, nil, "validate no parallel upgrade of nodes")

			Step("validate all apps after upgrade", func() {
				ValidateApplications(contexts)
			})
		}

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func getClusterNodesInfo(stopSignal <-chan struct{}, mError *error) {
	stNodes := node.GetStorageNodes()

	nodeSchedulableStatus := make(map[string]string)
	stNodeNames := make(map[string]bool)

	for _, stNode := range stNodes {
		stNodeNames[stNode.Name] = true
	}

	//Handling case where we have storageless node as kvdb node with dedicated kvdb device attached.
	kvdbNodes, _ := GetAllKvdbNodes()
	for _, kvdbNode := range kvdbNodes {
		sNode, err := node.GetNodeDetailsByNodeID(kvdbNode.ID)
		if err == nil {
			stNodeNames[sNode.Name] = true
		} else {
			log.Errorf("got error while getting with id [%s]", kvdbNode.ID)
		}
	}

	log.Infof("stnodes are %#v", stNodeNames)
	itr := 1
	for {
		log.Infof("K8s node validation. iteration: #%d", itr)
		select {
		case <-stopSignal:
			log.Infof("Exiting node validations routine")
			return
		default:
			nodeList, err := core.Instance().GetNodes()
			if err != nil {
				log.Errorf("Got error : %s", err.Error())
				*mError = err
				return
			}

			nodeNotReadyeCount := 0
			for _, k8sNode := range nodeList.Items {
				for _, status := range k8sNode.Status.Conditions {
					if status.Type == v1.NodeReady {
						nodeSchedulableStatus[k8sNode.Name] = string(status.Status)
						if status.Status != v1.ConditionTrue && stNodeNames[k8sNode.Name] {
							nodeNotReadyeCount += 1
						}
						break
					}
				}

			}
			if nodeNotReadyeCount > 1 {
				err = fmt.Errorf("multiple  nodes are Unschedulable at same time,"+
					"node status:%#v", nodeSchedulableStatus)
				log.Errorf("Got error : %s", err.Error())
				log.Infof("Node Details: %#v", nodeList.Items)
				output, err := Inst().N.RunCommand(stNodes[0], "pxctl status", node.ConnectionOpts{
					IgnoreError:     false,
					TimeBeforeRetry: defaultRetryInterval,
					Timeout:         defaultTimeout,
					Sudo:            true,
				})
				if err != nil {
					log.Errorf("failed to get pxctl status, Err: %v", err)
				}
				log.Infof(output)
				*mError = err
				return
			}
		}
		itr++
		time.Sleep(30 * time.Second)
	}
}

func ocp414Prereq() error {

	stc, err := Inst().V.GetDriver()
	if err != nil {
		return err
	}

	log.Infof("is autopilot enabled?: %t", stc.Spec.Autopilot.Enabled)
	if stc.Spec.Autopilot.Enabled {
		if err = createClusterMonitoringConfig(); err != nil {
			return err
		}

		if err = updatePrometheusAndAutopilot(stc); err != nil {
			return err
		}
	}

	return nil

}

func createClusterMonitoringConfig() error {
	// Create configmap
	ocpConfigmap := &v1.ConfigMap{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      "cluster-monitoring-config",
			Namespace: "openshift-monitoring",
		},
		Data: map[string]string{
			"config.yaml": "enableUserWorkload: true",
		},
	}

	_, err = core.Instance().CreateConfigMap(ocpConfigmap)

	return err

}

func updatePrometheusAndAutopilot(stc *ops_v1.StorageCluster) error {
	thanosQuerierHostCmd := `kubectl get route thanos-querier -n openshift-monitoring -o json | jq -r '.spec.host'`

	var output []byte

	output, err = exec.Command("sh", "-c", thanosQuerierHostCmd).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get thanos querier host , Err: %v", err)
	}
	thanosQuerierHost := strings.TrimSpace(string(output))
	log.Infof("Thanos Querier Host:%s", thanosQuerierHost)

	if stc.Spec.Monitoring.Prometheus.Enabled {
		stc.Spec.Monitoring.Prometheus.Enabled = false
	}

	dataProviders := stc.Spec.Autopilot.Providers

	for _, dataProvider := range dataProviders {
		if dataProvider.Type == "prometheus" {
			dataProvider.Params["url"] = fmt.Sprintf("https://%s", thanosQuerierHost)
		}

	}
	pxOperator := operator.Instance()
	_, err = pxOperator.UpdateStorageCluster(stc)

	return err

}
