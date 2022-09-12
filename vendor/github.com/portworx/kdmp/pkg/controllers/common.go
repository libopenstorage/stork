package controllers

import (
	"os"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	"k8s.io/apimachinery/pkg/util/yaml"
)

var (
	// ResyncPeriod controller resync period
	ResyncPeriod = 10 * time.Second
	// RequeuePeriod controller requeue period
	RequeuePeriod = 5 * time.Second
	// ValidateCRDInterval CRD validation interval
	ValidateCRDInterval time.Duration = 10 * time.Second
	// ValidateCRDTimeout CRD validation timeout
	ValidateCRDTimeout time.Duration = 2 * time.Minute
	// CleanupFinalizer cleanup finalizer
	CleanupFinalizer = "kdmp.portworx.com/finalizer-cleanup"
	// TaskDefaultTimeout timeout for retry task
	TaskDefaultTimeout = 1 * time.Minute
	// TaskProgressCheckInterval to check task progress at specified interval
	TaskProgressCheckInterval = 5 * time.Second
)

// ReadBackupLocation fetching backuplocation CR
func ReadBackupLocation(name, namespace, filePath string) (*storkapi.BackupLocation, error) {
	if name != "" {
		if namespace == "" {
			namespace = "default"
		}
		return stork.Instance().GetBackupLocation(name, namespace)
	}

	// TODO: This is needed for restic, we can think of removing it later
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	out := &storkapi.BackupLocation{}
	if err = yaml.NewYAMLOrJSONDecoder(f, 1024).Decode(out); err != nil {
		return nil, err
	}

	return out, nil
}
