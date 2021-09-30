package tests

import (
	"fmt"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
)

const (
	defaultCommandRetry   = 5 * time.Second
	defaultCommandTimeout = 1 * time.Minute
)

var _ = Describe("{NFSServerFailover}", func() {
	var contexts []*scheduler.Context
	It("has to setup, validate, failover, make sure pods on old and new server got restarted, and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nfsserverfailover-%d", i))...)
		}

		ValidateApplications(contexts)

		for _, ctx := range contexts {
			nodeReplicaMap := make(map[string]bool)
			var volume *volume.Volume
			Step("disable scheduling on non replica nodes", func() {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				volume = vols[0]
				replicaSets, err := Inst().V.GetReplicaSets(volume)
				Expect(err).NotTo(HaveOccurred())
				for _, replicaSet := range replicaSets {
					for _, node := range replicaSet.Nodes {
						nodeReplicaMap[node] = true
					}
				}
				// make sure there are 2 replicas
				Expect(len(nodeReplicaMap)).To(Equal(2))
				allNodes := node.GetWorkerNodes()
				for _, node := range allNodes {
					if !nodeReplicaMap[node.VolDriverNodeID] {
						Inst().S.DisableSchedulingOnNode(node)
					}
				}
			})

			// scale down and then scale up the app, so that pods are only scheduled on replica nodes
			Step(fmt.Sprintf("scale down app: %s to 0 ", ctx.App.Key), func() {
				applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
				Expect(err).NotTo(HaveOccurred())
				for name := range applicationScaleUpMap {
					applicationScaleUpMap[name] = int32(0)
				}
				err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
				Expect(err).NotTo(HaveOccurred())
			})

			Step(fmt.Sprintf("scale up app: %s to 2, and re-enable scheduling on all nodes", ctx.App.Key), func() {
				applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
				Expect(err).NotTo(HaveOccurred())
				for name := range applicationScaleUpMap {
					applicationScaleUpMap[name] = int32(2)
				}
				err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
				Expect(err).NotTo(HaveOccurred())
				ValidateApplications(contexts)
			})

			Step("fail over nfs server, and make sure the pod on server gets restarted", func() {
				oldServer, err := Inst().V.GetNodeForVolume(volume, defaultCommandTimeout, defaultCommandRetry)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("old nfs server %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])
				pods, err := core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				var oldPodOnOldServer corev1.Pod
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						oldPodOnOldServer = pod
					}
				}
				// make sure there is a pod running on the old nfs server
				Expect(oldPodOnOldServer.Name).NotTo(Equal(""))
				logrus.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)

				timestampBeforeFailOver := time.Now()
				err = Inst().V.StopDriver([]node.Node{*oldServer}, false, nil)
				Expect(err).NotTo(HaveOccurred())
				err = Inst().V.WaitDriverDownOnNode(*oldServer)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("stopped px on nfs server node %v [%v]", oldServer.SchedulerNodeName, oldServer.Addresses[0])

				var newServer *node.Node

				for i := 0; i < 60; i++ {
					server, err := Inst().V.GetNodeForVolume(volume, defaultCommandTimeout, defaultCommandRetry)
					// there could be intermittent error here
					if err != nil {
						logrus.Infof("Failed to get node for volume. Error: %v", err)
					} else {
						if server.Id != oldServer.Id {
							logrus.Infof("nfs server failed over, new nfs server is %s [%s]", server.SchedulerNodeName, server.Addresses[0])
							newServer = server
							break
						}
					}
					time.Sleep(10 * time.Second)
				}
				// make sure nfs server failed over
				Expect(newServer).NotTo(BeNil())
				logrus.Infof("new nfs server is %v [%v]", newServer.SchedulerNodeName, newServer.Addresses[0])

				logrus.Infof("start px on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)
				Inst().V.StartDriver(*oldServer)
				err = Inst().V.WaitDriverUpOnNode(*oldServer, Inst().DriverStartTimeout)
				Expect(err).NotTo(HaveOccurred())
				logrus.Infof("px is up on old nfs server Id %v, Name %v", oldServer.Id, oldServer.Name)

				ValidateApplications(contexts)

				// make sure the pods on both old and new server are restarted
				pods, err = core.Instance().GetPodsUsingPV(volume.ID)
				Expect(err).NotTo(HaveOccurred())
				podRestartedOnOldServer := false
				podRestartedOnNewServer := false
				for _, pod := range pods {
					if pod.Spec.NodeName == oldServer.Name {
						logrus.Infof("pod on old server %v, creation time %v", oldPodOnOldServer.Name, oldPodOnOldServer.CreationTimestamp)
						logrus.Infof("After failover, pod on old server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnOldServer = true
					}
					if pod.Spec.NodeName == newServer.Name {
						logrus.Infof("After failover, pod on new server %v, creation time %v", pod.Name, pod.CreationTimestamp)
						Expect(pod.CreationTimestamp.After(timestampBeforeFailOver)).To(BeTrue())
						podRestartedOnNewServer = true
					}
				}

				Expect(podRestartedOnOldServer).To(BeTrue())
				Expect(podRestartedOnNewServer).To(BeTrue())

				// re-enable scheduling on non replica nodes
				for _, node := range node.GetWorkerNodes() {
					if !nodeReplicaMap[node.VolDriverNodeID] {
						Inst().S.EnableSchedulingOnNode(node)
					}
				}
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}
