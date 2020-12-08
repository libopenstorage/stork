package metrics

import (
	"fmt"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	// CloneStatusCounter for application clone CR status on server
	cloneStatusCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_clone_status",
		Help: "Status of application clones",
	}, []string{MetricName, MetricNamespace})
	// CloneStageCounter for application clone CR stages on server
	cloneStageCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_clone_stage",
		Help: "Stage of application clones",
	}, []string{MetricName, MetricNamespace})
	// CloneDurationCounter for time taken by application clone to complete
	cloneDurationCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "application_clone_duration",
		Help: "Duration of application clones",
	}, []string{MetricName, MetricNamespace})
)

var (
	// CloneStatus map of application clone status to enum
	CloneStatus = map[stork_api.ApplicationCloneStatusType]float64{
		stork_api.ApplicationCloneStatusInitial:        0,
		stork_api.ApplicationCloneStatusPending:        1,
		stork_api.ApplicationCloneStatusInProgress:     2,
		stork_api.ApplicationCloneStatusFailed:         3,
		stork_api.ApplicationCloneStatusSuccessful:     4,
		stork_api.ApplicationCloneStatusRetained:       5,
		stork_api.ApplicationCloneStatusPartialSuccess: 6,
	}

	// CloneStage map of application clone stage to enum
	CloneStage = map[stork_api.ApplicationCloneStageType]float64{
		stork_api.ApplicationCloneStageInitial:      0,
		stork_api.ApplicationCloneStagePreExecRule:  1,
		stork_api.ApplicationCloneStagePostExecRule: 2,
		stork_api.ApplicationCloneStageVolumes:      3,
		stork_api.ApplicationCloneStageApplications: 4,
		stork_api.ApplicationCloneStageFinal:        5,
	}
)

func watchCloneCR(object runtime.Object) error {
	clone, ok := object.(*stork_api.ApplicationClone)
	if !ok {
		err := fmt.Errorf("invalid object type on clone watch: %v", object)
		return err
	}
	labels := make(prometheus.Labels)
	labels[MetricName] = clone.Name
	labels[MetricNamespace] = clone.Namespace
	if clone.DeletionTimestamp != nil {
		cloneStatusCounter.Delete(labels)
		cloneStageCounter.Delete(labels)
		cloneDurationCounter.Delete(labels)
		return nil
	}
	// Set Clone Status counter
	cloneStatusCounter.With(labels).Set(CloneStatus[clone.Status.Status])
	// Set Clone Stage Counter
	cloneStageCounter.With(labels).Set(CloneStage[clone.Status.Stage])
	if clone.Status.Stage == stork_api.ApplicationCloneStageFinal && (clone.Status.Status == stork_api.ApplicationCloneStatusSuccessful ||
		clone.Status.Status == stork_api.ApplicationCloneStatusPartialSuccess ||
		clone.Status.Status == stork_api.ApplicationCloneStatusFailed) {
		st := clone.CreationTimestamp.Unix()
		et := clone.Status.FinishTimestamp.Unix()
		// Set Clone Duration
		cloneDurationCounter.With(labels).Set(float64(et - st))
	}

	return nil
}

func init() {
	prometheus.MustRegister(cloneStatusCounter)
	prometheus.MustRegister(cloneStageCounter)
	prometheus.MustRegister(cloneDurationCounter)
}
