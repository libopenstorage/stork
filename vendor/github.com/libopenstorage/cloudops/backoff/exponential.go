package backoff

import (
	"fmt"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
)

// ExponentialBackoffErrorCheck is a callback function implemented by cloud providers
// which process the input error and indicate if an exponential backoff is required
type ExponentialBackoffErrorCheck func(err error) bool

// NewExponentialBackoffOps return wrapper for CloudOps interface for all cloud providers.
// It provides exponential backoff retries on cloud APIs for specific error codes
// It uses k8s.io/apimachinery's wait.ExponentialBackoff
//
// ExponentialBackoff repeats a condition check with exponential backoff.
//
// It checks the condition up to Steps times, increasing the wait by multiplying
// the previous duration by Factor.
//
// If Jitter is greater than zero, a random amount of each duration is added
// (between duration and duration*(1+jitter)).
//
// If the condition never returns true, ErrWaitTimeout is returned. All other
// errors terminate immediately.
func NewExponentialBackoffOps(
	cloudOps cloudops.Ops,
	errorCheck ExponentialBackoffErrorCheck,
	backoff wait.Backoff,
) cloudops.Ops {
	return &exponentialBackoff{cloudOps, errorCheck, backoff}
}

// DefaultExponentialBackoff is the default backoff strategy that is used for doing
// an exponential backoff. This configuration results into a total wait time of 20 minutes
var DefaultExponentialBackoff = wait.Backoff{
	Duration: 5 * time.Second, // the base duration
	Factor:   1.5,             // Duration is multiplied by factor each iteration
	Jitter:   1.0,             // The amount of jitter applied each iteration
	Steps:    12,              // Exit with error after this many steps
}

type exponentialBackoff struct {
	cloudOps           cloudops.Ops
	isExponentialError ExponentialBackoffErrorCheck
	backoff            wait.Backoff
}

func (e *exponentialBackoff) InstanceID() string {
	return e.cloudOps.InstanceID()
}

func (e *exponentialBackoff) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {
	var (
		instanceInfo *cloudops.InstanceInfo
		origErr      error
	)
	conditionFn := func() (bool, error) {
		instanceInfo, origErr = e.cloudOps.InspectInstance(instanceID)
		msg := fmt.Sprintf("Failed to inspect instance: %v.", instanceID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return instanceInfo, origErr

}

func (e *exponentialBackoff) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {
	var (
		instanceGroupInfo *cloudops.InstanceGroupInfo
		origErr           error
	)
	conditionFn := func() (bool, error) {
		instanceGroupInfo, origErr = e.cloudOps.InspectInstanceGroupForInstance(instanceID)
		msg := fmt.Sprintf("Failed to inspect instance-group for instance: %v.", instanceID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return instanceGroupInfo, origErr
}

func (e *exponentialBackoff) SetInstanceGroupSize(instanceGroupID string,
	count int64,
	timeout time.Duration) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.SetInstanceGroupSize(instanceGroupID, count, timeout)
		return e.handleError(origErr, fmt.Sprintf("Failed to set cluster size"))
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr

}

func (e *exponentialBackoff) SetClusterVersion(version string, timeout time.Duration) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.SetClusterVersion(version, timeout)
		return e.handleError(origErr, fmt.Sprintf("Failed to set cluster version"))
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr

}

func (e *exponentialBackoff) SetInstanceGroupVersion(instanceGroupID string,
	version string,
	timeout time.Duration) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.SetInstanceGroupVersion(instanceGroupID, version, timeout)
		return e.handleError(origErr, fmt.Sprintf("Failed to set instance group version"))
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr

}

func (e *exponentialBackoff) GetInstanceGroupSize(instanceGroupID string) (int64, error) {
	var (
		count   int64
		origErr error
	)
	conditionFn := func() (bool, error) {
		count, origErr = e.cloudOps.GetInstanceGroupSize(instanceGroupID)
		return e.handleError(origErr, fmt.Sprintf("Failed to get instance group size"))
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return 0, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return count, origErr
}

func (e *exponentialBackoff) GetClusterSizeForInstance(instanceID string) (int64, error) {
	var (
		count   int64
		origErr error
	)
	conditionFn := func() (bool, error) {
		count, origErr = e.cloudOps.GetClusterSizeForInstance(instanceID)
		return e.handleError(origErr, fmt.Sprintf("Failed to get cluster size for instance: %v.", instanceID))
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return 0, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return count, origErr

}

func (e *exponentialBackoff) DeleteInstance(instanceID string, zone string, timeout time.Duration) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.DeleteInstance(instanceID, zone, timeout)
		msg := fmt.Sprintf("Failed to delete instance: %v.", instanceID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr

}

// Create volume based on input template volume and also apply given labels.
func (e *exponentialBackoff) Create(template interface{}, labels map[string]string) (interface{}, error) {
	var (
		drive   interface{}
		origErr error
	)
	conditionFn := func() (bool, error) {
		drive, origErr = e.cloudOps.Create(template, labels)
		msg := fmt.Sprintf("Failed to create drive.")
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return drive, origErr

}

// GetDeviceID returns ID/Name of the given device/disk or snapshot
func (e *exponentialBackoff) GetDeviceID(template interface{}) (string, error) {
	return e.cloudOps.GetDeviceID(template)
}

// Attach volumeID.
// Return attach path.
func (e *exponentialBackoff) Attach(volumeID string, options map[string]string) (string, error) {
	var (
		devPath string
		origErr error
	)
	conditionFn := func() (bool, error) {
		devPath, origErr = e.cloudOps.Attach(volumeID, options)
		msg := fmt.Sprintf("Failed to attach drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return "", cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return devPath, origErr
}

// Detach volumeID.
func (e *exponentialBackoff) Detach(volumeID string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.Detach(volumeID)
		msg := fmt.Sprintf("Failed to detach drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// DetachFrom detaches the disk/volume with given ID from the given instance ID
func (e *exponentialBackoff) DetachFrom(volumeID, instanceID string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.DetachFrom(volumeID, instanceID)
		msg := fmt.Sprintf("Failed to detach drive (%v) from instance (%v).", volumeID, instanceID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// Delete volumeID.
func (e *exponentialBackoff) Delete(volumeID string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.Delete(volumeID)
		msg := fmt.Sprintf("Failed to delete drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// DeleteFrom deletes the given volume/disk from the given instanceID
func (e *exponentialBackoff) DeleteFrom(volumeID, instanceID string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.DeleteFrom(volumeID, instanceID)
		msg := fmt.Sprintf("Failed to delete drive (%v) from instance %v.", volumeID, instanceID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// Desribe an instance
func (e *exponentialBackoff) Describe() (interface{}, error) {
	var (
		instance interface{}
		origErr  error
	)
	conditionFn := func() (bool, error) {
		instance, origErr = e.cloudOps.Describe()
		msg := fmt.Sprintf("Failed to describe instance.")
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return instance, origErr
}

// FreeDevices returns free block devices on the instance.
// blockDeviceMappings is a data structure that contains all block devices on
// the instance and where they are mapped to
func (e *exponentialBackoff) FreeDevices(blockDeviceMappings []interface{}, rootDeviceName string) ([]string, error) {
	return e.cloudOps.FreeDevices(blockDeviceMappings, rootDeviceName)
}

// Inspect volumes specified by volumeID
func (e *exponentialBackoff) Inspect(volumeIds []*string) ([]interface{}, error) {
	var (
		volumes []interface{}
		origErr error
	)
	conditionFn := func() (bool, error) {
		volumes, origErr = e.cloudOps.Inspect(volumeIds)
		msg := fmt.Sprintf("Failed to inspect drives (%v).", volumeIds)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return volumes, origErr
}

// DeviceMappings returns map[local_attached_volume_path]->volume ID/NAME
func (e *exponentialBackoff) DeviceMappings() (map[string]string, error) {
	var (
		mappings map[string]string
		origErr  error
	)
	conditionFn := func() (bool, error) {
		mappings, origErr = e.cloudOps.DeviceMappings()
		msg := fmt.Sprintf("Failed to get device mappings.")
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return mappings, origErr

}

// Enumerate volumes that match given filters. Organize them into
// sets identified by setIdentifier.
// labels can be nil, setIdentifier can be empty string.
func (e *exponentialBackoff) Enumerate(volumeIds []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	var (
		enumerateResponse map[string][]interface{}
		origErr           error
	)
	conditionFn := func() (bool, error) {
		enumerateResponse, origErr = e.cloudOps.Enumerate(volumeIds, labels, setIdentifier)
		msg := fmt.Sprintf("Failed to enumerate drives (%v).", volumeIds)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return enumerateResponse, origErr
}

// DevicePath for the given volume i.e path where it's attached
func (e *exponentialBackoff) DevicePath(volumeID string) (string, error) {
	var (
		devicePath string
		origErr    error
	)
	conditionFn := func() (bool, error) {
		devicePath, origErr = e.cloudOps.DevicePath(volumeID)
		msg := fmt.Sprintf("Failed to get device path for drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return "", cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return devicePath, origErr
}

func (e *exponentialBackoff) Expand(volumeID string, targetSize uint64) (uint64, error) {
	var (
		actualSize uint64
		origErr    error
	)
	conditionFn := func() (bool, error) {
		actualSize, origErr = e.cloudOps.Expand(volumeID, targetSize)
		msg := fmt.Sprintf("Failed to get device path for drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return 0, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return actualSize, origErr
}

// Snapshot the volume with given volumeID
func (e *exponentialBackoff) Snapshot(volumeID string, readonly bool) (interface{}, error) {
	var (
		snapshot interface{}
		origErr  error
	)
	conditionFn := func() (bool, error) {
		snapshot, origErr = e.cloudOps.Snapshot(volumeID, readonly)
		msg := fmt.Sprintf("Failed to snapshot drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return snapshot, origErr
}

// SnapshotDelete deletes the snapshot with given ID
func (e *exponentialBackoff) SnapshotDelete(snapID string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.SnapshotDelete(snapID)
		msg := fmt.Sprintf("Failed to delete snapshot (%v).", snapID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// ApplyTags will apply given labels/tags on the given volume
func (e *exponentialBackoff) ApplyTags(volumeID string, labels map[string]string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.ApplyTags(volumeID, labels)
		msg := fmt.Sprintf("Failed to apply tags on drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// RemoveTags removes labels/tags from the given volume
func (e *exponentialBackoff) RemoveTags(volumeID string, labels map[string]string) error {
	var (
		origErr error
	)
	conditionFn := func() (bool, error) {
		origErr = e.cloudOps.RemoveTags(volumeID, labels)
		msg := fmt.Sprintf("Failed to remove tags from drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return origErr
}

// Tags will list the existing labels/tags on the given volume
func (e *exponentialBackoff) Tags(volumeID string) (map[string]string, error) {
	var (
		origErr error
		labels  map[string]string
	)
	conditionFn := func() (bool, error) {
		labels, origErr = e.cloudOps.Tags(volumeID)
		msg := fmt.Sprintf("Failed to get tags of drive (%v).", volumeID)
		return e.handleError(origErr, msg)
	}
	expErr := wait.ExponentialBackoff(e.backoff, conditionFn)
	if expErr == wait.ErrWaitTimeout {
		return nil, cloudops.NewStorageError(cloudops.ErrExponentialTimeout, origErr.Error(), "")
	}
	return labels, origErr
}

func (e *exponentialBackoff) Name() string {
	return "exponential-backoff"
}

func (e *exponentialBackoff) handleError(origErr error, msg string) (bool, error) {
	if origErr != nil {
		if e.isExponentialError(origErr) {
			// do an exponential backoff
			logrus.WithFields(logrus.Fields{
				e.cloudOps.Name() + "-error": origErr,
			}).Errorf("%v Retrying after a backoff.", msg)
			return false, nil
		} // for all other errors return immediately
		return true, origErr
	}
	return true, nil
}
