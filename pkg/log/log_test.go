// +build unittest

package log

import (
	"testing"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestLog(t *testing.T) {
	t.Run("podLogTest", podLogTest)
	t.Run("deploymentLogTest", deploymentLogTest)
	t.Run("statefulsetLogTest", statefulsetLogTest)
	t.Run("snapshotLogTest", snapshotLogTest)
	t.Run("snapshotScheduleLogTest", snapshotScheduleLogTest)
	t.Run("migrationLogTest", migrationLogTest)
	t.Run("migrationScheduleLogTest", migrationScheduleLogTest)
	t.Run("ruleLogTest", ruleLogTest)
}

func podLogTest(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testpod",
			Namespace: "testnamespace",
		},
	}
	PodLog(pod).Infof("valid pod info")
	pod = nil
	PodLog(pod).Infof("nil pod info")

	pod = &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "testpod",
			Namespace:       "testnamespace",
			OwnerReferences: []metav1.OwnerReference{},
		},
	}
	PodLog(pod).Infof("empty pod owner")
	pod.OwnerReferences = append(pod.OwnerReferences,
		metav1.OwnerReference{
			Kind:       "testkind",
			Name:       "testname",
			Controller: nil,
		})
	PodLog(pod).Infof("pod owner nil controller")
	controller := true
	pod.OwnerReferences[0].Controller = &controller
	PodLog(pod).Infof("pod owner controller")
}

func deploymentLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testdeployment",
		Namespace: "testnamespace",
	}
	deploymentv1 := &appv1.Deployment{
		ObjectMeta: metadata}
	DeploymentV1Log(deploymentv1).Infof("deploymentv1 log")
	DeploymentV1Log(nil).Infof("deploymentv1 nil log")
	deploymentv1beta1 := &appv1beta1.Deployment{
		ObjectMeta: metadata}
	DeploymentV1Beta1Log(deploymentv1beta1).Infof("deploymentv1beta1 log")
	DeploymentV1Beta1Log(nil).Infof("deploymentv1beta1 nil log")
	deploymentv1beta2 := &appv1beta2.Deployment{
		ObjectMeta: metadata}
	DeploymentV1Beta2Log(deploymentv1beta2).Infof("deploymentv1beta2 log")
	DeploymentV1Beta2Log(nil).Infof("deploymentv1beta2 nil log")
}
func statefulsetLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "teststatefulset",
		Namespace: "testnamespace",
	}
	statefulsetv1 := &appv1.StatefulSet{
		ObjectMeta: metadata}
	StatefulSetV1Log(statefulsetv1).Infof("statefulsetv1 log")
	StatefulSetV1Log(nil).Infof("statefulsetv1 nil log")
	statefulsetv1beta1 := &appv1beta1.StatefulSet{
		ObjectMeta: metadata}
	StatefulSetV1Beta1Log(statefulsetv1beta1).Infof("statefulsetv1beta1 log")
	StatefulSetV1Beta1Log(nil).Infof("statefulsetv1beta1 nil log")
	statefulsetv1beta2 := &appv1beta2.StatefulSet{
		ObjectMeta: metadata}
	StatefulSetV1Beta2Log(statefulsetv1beta2).Infof("statefulsetv1beta2 log")
	StatefulSetV1Beta2Log(nil).Infof("statefulsetv1beta2 nil log")
}

func snapshotLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testsnapshot",
		Namespace: "testnamespace",
	}
	snapshot := &crdv1.VolumeSnapshot{
		Metadata: metadata,
	}
	SnapshotLog(snapshot).Infof("snapshot log")
	SnapshotLog(nil).Infof("snapshot nil log")
}

func snapshotScheduleLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testsnapshotschedule",
		Namespace: "testnamespace",
	}
	snapshotSchedule := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metadata,
	}
	VolumeSnapshotScheduleLog(snapshotSchedule).Infof("snapshot schedule log")
	VolumeSnapshotScheduleLog(nil).Infof("snapshot schedule nil log")
}

func migrationLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testmigration",
		Namespace: "testnamespace",
	}
	migration := &storkv1.Migration{
		ObjectMeta: metadata,
	}
	MigrationLog(migration).Infof("migration log")
	MigrationLog(nil).Infof("migration nil log")
}

func migrationScheduleLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testmigrationschedule",
		Namespace: "testnamespace",
	}
	migrationSchedule := &storkv1.MigrationSchedule{
		ObjectMeta: metadata,
	}
	MigrationScheduleLog(migrationSchedule).Infof("migrationschedule log")
	MigrationScheduleLog(nil).Infof("migrationschedule nil log")
}

func ruleLogTest(t *testing.T) {
	metadata := metav1.ObjectMeta{
		Name:      "testrule",
		Namespace: "testnamespace",
	}
	rule := &storkv1.Rule{
		ObjectMeta: metadata,
	}
	migration := &storkv1.Migration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testmigration",
			Namespace: "testnamespace",
		},
	}
	snapshot := &crdv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      "testsnapshot",
			Namespace: "testnamespace",
		},
	}

	RuleLog(rule, snapshot).Infof("rule with snapshot log")
	RuleLog(rule, migration).Infof("rule with migration log")
	RuleLog(rule, nil).Infof("rule with nil object log")
}
