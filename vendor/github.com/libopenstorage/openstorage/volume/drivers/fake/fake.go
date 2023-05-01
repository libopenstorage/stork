/*
Package fake provides an in-memory fake driver implementation
Copyright 2018 Portworx

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package fake

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/cluster"
	clustermanager "github.com/libopenstorage/openstorage/cluster/manager"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/libopenstorage/openstorage/volume/drivers/common"
	"github.com/pborman/uuid"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/mem"
	"github.com/sirupsen/logrus"
)

const (
	Name             = "fake"
	credsKeyPrefix   = "/fake/credentials"
	backupsKeyPrefix = "/fake/backups"
	schedPrefix      = "/fake/schedules"
	Type             = api.DriverType_DRIVER_TYPE_BLOCK
)

// Implements the open storage volume interface.
type driver struct {
	volume.IODriver
	volume.StoreEnumerator
	volume.StatsDriver
	volume.QuiesceDriver
	volume.CredsDriver
	volume.CloudBackupDriver
	volume.CloudMigrateDriver
	volume.FilesystemTrimDriver
	volume.FilesystemCheckDriver
	kv          kvdb.Kvdb
	thisCluster cluster.Cluster
}

type fakeCred struct {
	Id     string
	Params map[string]interface{}
}

type fakeBackups struct {
	Volume    api.Volume
	Info      api.CloudBackupInfo
	Status    api.CloudBackupStatus
	ClusterId string
}

type fakeSchedules struct {
	Id   string
	Info api.CloudBackupScheduleInfo
}

func Init(params map[string]string) (volume.VolumeDriver, error) {
	return newFakeDriver(params)
}

func newFakeDriver(params map[string]string) (*driver, error) {

	// This instance of the KVDB is Always in memory and created for each instance of the fake driver
	// It is not necessary to run a single instance, and it helps tests create a new kvdb on each test
	kv, err := kvdb.New(mem.Name, "fake_test", []string{}, nil, kvdb.LogFatalErrorCB)
	if err != nil {
		return nil, err
	}
	inst := &driver{
		IODriver:              volume.IONotSupported,
		StoreEnumerator:       common.NewDefaultStoreEnumerator(Name, kv),
		StatsDriver:           volume.StatsNotSupported,
		QuiesceDriver:         volume.QuiesceNotSupported,
		CloudMigrateDriver:    volume.CloudMigrateNotSupported,
		FilesystemTrimDriver:  volume.FilesystemTrimNotSupported,
		FilesystemCheckDriver: volume.FilesystemCheckNotSupported,
		kv:                    kv,
	}

	inst.thisCluster, err = clustermanager.Inst()
	if err != nil {
		return nil, err
	}

	volumeInfo, err := inst.StoreEnumerator.Enumerate(&api.VolumeLocator{}, nil)
	if err == nil {
		for _, info := range volumeInfo {
			if info.Status == api.VolumeStatus_VOLUME_STATUS_NONE {
				info.Status = api.VolumeStatus_VOLUME_STATUS_UP
				inst.UpdateVol(info)
			}
		}
	}

	logrus.Println("Fake driver initialized")
	return inst, nil
}

func (d *driver) Name() string {
	return Name
}

func (d *driver) Type() api.DriverType {
	return Type
}

func (d *driver) Version() (*api.StorageVersion, error) {
	return &api.StorageVersion{
		Driver:  d.Name(),
		Version: "1.0.0-fake",
		Details: map[string]string{
			"example": "data",
		},
	}, nil
}

// Status diagnostic information
func (d *driver) Status() [][2]string {
	return [][2]string{}
}

func (d *driver) Inspect(volumeIDs []string) ([]*api.Volume, error) {
	volumes, err := d.StoreEnumerator.Inspect(volumeIDs)
	if err != nil {
		return nil, err
	} else if err == nil && len(volumes) == 0 {
		return nil, kvdb.ErrNotFound
	}

	return volumes, err
}

//
// These functions below implement the volume driver interface.
//

func (d *driver) Create(
	ctx context.Context,
	locator *api.VolumeLocator,
	source *api.Source,
	spec *api.VolumeSpec) (string, error) {

	if spec.Size == 0 {
		return "", fmt.Errorf("Volume size cannot be zero")
	} else if spec.GetHaLevel() == 0 {
		return "", fmt.Errorf("HA level cannot be zero")
	}

	volumeID := strings.TrimSuffix(uuid.New(), "\n")

	if _, err := d.GetVol(volumeID); err == nil {
		return "", fmt.Errorf("volume with that id already exists")
	}

	// snapshot passes nil volumelabels
	if locator.VolumeLabels == nil {
		locator.VolumeLabels = make(map[string]string)
	}

	v := common.NewVolume(
		volumeID,
		api.FSType_FS_TYPE_XFS,
		locator,
		source,
		spec,
	)

	if err := d.CreateVol(v); err != nil {
		return "", err
	}
	return v.Id, nil
}

func (d *driver) Delete(ctx context.Context, volumeID string) error {
	_, err := d.GetVol(volumeID)
	if err != nil {
		logrus.Println(err)
		return err
	}

	err = d.DeleteVol(volumeID)
	if err != nil {
		logrus.Println(err)
		return err
	}

	return nil
}

func (d *driver) MountedAt(ctx context.Context, mountpath string) string {
	return ""
}

func (d *driver) Mount(ctx context.Context, volumeID string, mountpath string, options map[string]string) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		logrus.Println(err)
		return err
	}

	v.AttachPath = append(v.AttachPath, mountpath)
	return d.UpdateVol(v)
}

func (d *driver) Unmount(ctx context.Context, volumeID string, mountpath string, options map[string]string) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		return err
	}

	v.AttachPath = nil
	return d.UpdateVol(v)
}

func (d *driver) Snapshot(volumeID string, readonly bool, locator *api.VolumeLocator, noRetry bool) (string, error) {

	if len(locator.GetName()) == 0 {
		return "", fmt.Errorf("Name for snapshot must be provided")
	}

	volIDs := []string{volumeID}
	vols, err := d.Inspect(volIDs)
	if err != nil {
		return "", nil
	}
	source := &api.Source{Parent: volumeID}
	logrus.Infof("Creating snap %s for vol %s", locator.Name, volumeID)
	newVolumeID, err := d.Create(context.TODO(), locator, source, vols[0].Spec)
	if err != nil {
		return "", nil
	}

	return newVolumeID, nil
}

func (d *driver) Restore(volumeID string, snapID string) error {
	if _, err := d.Inspect([]string{volumeID, snapID}); err != nil {
		return err
	}

	return nil
}

func (d *driver) SnapshotGroup(groupID string, labels map[string]string, volumeIDs []string, deleteOnFailure bool) (*api.GroupSnapCreateResponse, error) {

	// We can return something here.
	return nil, volume.ErrNotSupported
}

func (d *driver) Attach(ctx context.Context, volumeID string, attachOptions map[string]string) (string, error) {
	return "/dev/fake/" + volumeID, nil
}

func (d *driver) Detach(ctx context.Context, volumeID string, options map[string]string) error {
	return nil
}

func (d *driver) CloudMigrateStart(request *api.CloudMigrateStartRequest) (*api.CloudMigrateStartResponse, error) {
	return &api.CloudMigrateStartResponse{TaskId: request.TaskId}, nil
}

func (d *driver) CloudMigrateCancel(request *api.CloudMigrateCancelRequest) error {
	return nil
}

func (d *driver) CloudMigrateStatus(request *api.CloudMigrateStatusRequest) (*api.CloudMigrateStatusResponse, error) {
	cml := make(map[string]*api.CloudMigrateInfoList, 0)
	cml["result"] = &api.CloudMigrateInfoList{}
	return &api.CloudMigrateStatusResponse{
		Info: cml,
	}, nil
}

func (d *driver) Set(volumeID string, locator *api.VolumeLocator, spec *api.VolumeSpec) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		return err
	}

	// Set locator
	if locator != nil {
		if len(locator.GetName()) != 0 {
			v.Locator.Name = locator.GetName()
		}

		if len(locator.GetVolumeLabels()) != 0 {
			volumeLabels := v.GetLocator().GetVolumeLabels()
			if volumeLabels == nil {
				volumeLabels = locator.GetVolumeLabels()
			} else {
				for key, val := range locator.GetVolumeLabels() {
					if len(val) == 0 {
						delete(volumeLabels, key)
					} else {
						volumeLabels[key] = val
					}
				}
			}
			v.Locator.VolumeLabels = volumeLabels
		}
	}

	// Set Spec
	if spec != nil {
		if spec.Size != 0 {
			v.Spec.Size = spec.Size
		}
		if spec.HaLevel > 0 && spec.HaLevel < 4 {
			v.Spec.HaLevel = spec.HaLevel
		}
		if spec.GetReplicaSet() != nil {
			v.Spec.ReplicaSet = spec.GetReplicaSet()
		}
		v.Spec.Scale = spec.Scale
		v.Spec.Sticky = spec.Sticky
		v.Spec.Shared = spec.Shared
		v.Spec.Sharedv4 = spec.Sharedv4
		v.Spec.Journal = spec.Journal
		if spec.SnapshotInterval != math.MaxUint32 {
			v.Spec.SnapshotInterval = spec.SnapshotInterval
		}
		v.Spec.IoProfile = spec.IoProfile
		v.Spec.SnapshotSchedule = spec.SnapshotSchedule
		v.Spec.Ownership = spec.Ownership
	}

	return d.UpdateVol(v)
}

func (d *driver) Shutdown() {}

func (d *driver) UsedSize(volumeID string) (uint64, error) {
	vols, err := d.Inspect([]string{volumeID})
	if err == kvdb.ErrNotFound {
		return 0, fmt.Errorf("Volume not found")
	} else if err != nil {
		return 0, err
	} else if len(vols) == 0 {
		return 0, fmt.Errorf("Volume not found")
	}

	return uint64(12345), nil
}

func (d *driver) Stats(volumeID string, cumulative bool) (*api.Stats, error) {

	vols, err := d.Inspect([]string{volumeID})
	if err == kvdb.ErrNotFound {
		return nil, fmt.Errorf("Volume not found")
	} else if err != nil {
		return nil, err
	} else if len(vols) == 0 {
		return nil, fmt.Errorf("Volume not found")
	}

	return &api.Stats{
		Reads:      uint64(12345),
		ReadMs:     uint64(1),
		ReadBytes:  uint64(1234567),
		Writes:     uint64(9876),
		WriteMs:    uint64(2),
		WriteBytes: uint64(7654321),
		IoProgress: uint64(987),
		IoMs:       uint64(3),
		BytesUsed:  uint64(1234567890),
		IntervalMs: uint64(4),
	}, nil
}

func (d *driver) CapacityUsage(
	volumeID string,
) (*api.CapacityUsageResponse, error) {
	vols, err := d.Inspect([]string{volumeID})
	if err == kvdb.ErrNotFound {
		return nil, fmt.Errorf("Volume not found")
	} else if err != nil {
		return nil, err
	} else if len(vols) == 0 {
		return nil, fmt.Errorf("Volume not found")
	}

	return &api.CapacityUsageResponse{CapacityUsageInfo: &api.CapacityUsageInfo{
		ExclusiveBytes: int64(123456),
		SharedBytes:    int64(654321),
		TotalBytes:     int64(653421),
	}}, nil

}

func (d *driver) CredsCreate(
	params map[string]string,
) (string, error) {

	// Convert types
	converted := make(map[string]interface{})
	for k, v := range params {
		converted[k] = v
	}
	id := uuid.New()
	_, err := d.kv.Put(credsKeyPrefix+"/"+id, &fakeCred{
		Id:     id,
		Params: converted,
	}, 0)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (d *driver) CredsDelete(
	uuid string,
) error {
	d.kv.Delete(credsKeyPrefix + "/" + uuid)
	return nil
}

func (d *driver) CredsEnumerate() (map[string]interface{}, error) {

	kvp, err := d.kv.Enumerate(credsKeyPrefix)
	if err != nil {
		return nil, err
	}
	creds := make(map[string]interface{}, len(kvp))
	for _, v := range kvp {
		elem := &fakeCred{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		creds[elem.Id] = elem.Params
	}

	return creds, nil
}

func (d *driver) CredsValidate(uuid string) error {

	// All we can do here is just to check if it exists
	_, err := d.kv.Get(credsKeyPrefix + "/" + uuid)
	if err != nil {
		return fmt.Errorf("Credential id %s not found", uuid)
	}
	return nil
}

// CloudBackupCreate uploads snapshot of a volume to the cloud
func (d *driver) CloudBackupCreate(
	input *api.CloudBackupCreateRequest,
) (*api.CloudBackupCreateResponse, error) {
	name, _, err := d.cloudBackupCreate(input)
	if err == nil {
		resp := &api.CloudBackupCreateResponse{Name: name}
		return resp, err
	}
	return nil, err
}

// cloudBackupCreate uploads snapshot of a volume to the cloud and returns the
// backup task id
func (d *driver) cloudBackupCreate(input *api.CloudBackupCreateRequest) (string, string, error) {

	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return "", "", err
	}

	// Get volume info
	vols, err := d.Inspect([]string{input.VolumeID})
	if err != nil {
		return "", "", fmt.Errorf("Volume id not found")
	}
	if len(vols) < 1 {
		return "", "", fmt.Errorf("Internal error. Volume found but no data returned")
	}
	vol := vols[0]
	if vol.GetSpec() == nil {
		return "", "", fmt.Errorf("Internal error. Volume has no specificiation")
	}

	taskId := uuid.New()
	// Save cloud backup
	cloudId := uuid.New()
	clusterInfo, err := d.thisCluster.Enumerate()
	if err != nil {
		return "", "", err
	}
	_, err = d.kv.Put(backupsKeyPrefix+"/"+taskId, &fakeBackups{
		Volume:    *vol,
		ClusterId: clusterInfo.Id,
		Status: api.CloudBackupStatus{
			ID:             cloudId,
			OpType:         api.CloudBackupOp,
			Status:         api.CloudBackupStatusDone,
			BytesDone:      vol.GetSpec().GetSize(),
			StartTime:      time.Now(),
			CompletedTime:  time.Now().Local().Add(1 * time.Second),
			NodeID:         clusterInfo.NodeId,
			CredentialUUID: input.CredentialUUID,
			SrcVolumeID:    input.VolumeID,
		},
		Info: api.CloudBackupInfo{
			ID:            cloudId,
			SrcVolumeID:   input.VolumeID,
			SrcVolumeName: vol.GetLocator().GetName(),
			Timestamp:     time.Now(),
			Metadata: map[string]string{
				"fake": "backup",
			},
			Status: string(api.CloudBackupStatusDone),
		},
	}, 0)
	if err != nil {
		return "", "", err
	}
	return taskId, cloudId, nil
}

func (d *driver) backupEntry(Id string, op api.CloudBackupOpType) (*fakeBackups, string, error) {
	var backup *fakeBackups
	kvp, err := d.kv.Enumerate(backupsKeyPrefix)
	if err != nil {
		return nil, "", err
	}
	found := false
	id := ""
	for _, v := range kvp {
		if err := json.Unmarshal(v.Value, &backup); err != nil {
			return nil, "", err
		}
		if backup.Status.OpType != op {
			continue
		}
		if backup.Status.ID == Id {
			found = true
			id = v.Key
			break
		}
	}
	if !found {
		return nil, "", fmt.Errorf("Failed to find backup")
	}
	return backup, id, nil

}

// CloudBackupRestore downloads a cloud backup and restores it to a volume
func (d *driver) CloudBackupRestore(
	input *api.CloudBackupRestoreRequest,
) (*api.CloudBackupRestoreResponse, error) {

	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return nil, err
	}
	backup, _, err := d.backupEntry(input.ID, api.CloudBackupOp)
	if err != nil {
		return nil, err
	}

	volid, err := d.Create(context.TODO(), &api.VolumeLocator{Name: input.RestoreVolumeName}, &api.Source{}, backup.Volume.GetSpec())
	if err != nil {
		return nil, err
	}
	vols, err := d.Inspect([]string{volid})
	if err != nil {
		return nil, fmt.Errorf("Volume id not found")
	}
	if len(vols) < 1 {
		return nil, fmt.Errorf("Internal error. Volume found but no data returned")
	}
	vol := vols[0]
	if vol.GetSpec() == nil {
		return nil, fmt.Errorf("Internal error. Volume has no specificiation")
	}

	cloudId := uuid.New()
	clusterInfo, err := d.thisCluster.Enumerate()
	if err != nil {
		return nil, err
	}
	_, err = d.kv.Put(backupsKeyPrefix+"/"+cloudId, &fakeBackups{
		Volume:    *vol,
		ClusterId: clusterInfo.Id,
		Status: api.CloudBackupStatus{
			ID:             cloudId,
			OpType:         api.CloudRestoreOp,
			Status:         api.CloudBackupStatusDone,
			BytesDone:      vol.GetSpec().GetSize(),
			StartTime:      time.Now(),
			CompletedTime:  time.Now().Local().Add(1 * time.Second),
			NodeID:         clusterInfo.NodeId,
			CredentialUUID: input.CredentialUUID,
			SrcVolumeID:    volid,
		},
	}, 0)
	if err != nil {
		return nil, err
	}

	return &api.CloudBackupRestoreResponse{
		RestoreVolumeID: volid,
	}, nil

}

// CloudBackupDelete deletes the specified backup in cloud
func (d *driver) CloudBackupDelete(input *api.CloudBackupDeleteRequest) error {

	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return err
	}

	_, id, err := d.backupEntry(input.ID, api.CloudBackupOp)
	if err != nil {
		return err
	}
	//_, err := d.kv.Delete(backupsKeyPrefix + "/" + id)
	_, err = d.kv.Delete(id)
	return err
}

// CloudBackupEnumerate enumerates the backups for a given cluster/credential/volumeID
func (d *driver) CloudBackupEnumerate(input *api.CloudBackupEnumerateRequest) (*api.CloudBackupEnumerateResponse, error) {

	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return nil, err
	}

	backups := make([]api.CloudBackupInfo, 0)
	kvp, err := d.kv.Enumerate(backupsKeyPrefix)
	if err != nil {
		return nil, err
	}
	for _, v := range kvp {
		elem := &fakeBackups{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		if elem.Status.OpType == api.CloudRestoreOp {
			continue
		}

		if len(input.SrcVolumeID) == 0 && len(input.ClusterID) == 0 {
			backups = append(backups, elem.Info)
		} else if input.SrcVolumeID == elem.Info.SrcVolumeID {
			backups = append(backups, elem.Info)
		} else if input.ClusterID == elem.ClusterId {
			backups = append(backups, elem.Info)
		}
	}

	return &api.CloudBackupEnumerateResponse{
		Backups: backups,
	}, nil
}

// CloudBackupDelete deletes all the backups for a given volume in cloud
func (d *driver) CloudBackupDeleteAll(input *api.CloudBackupDeleteAllRequest) error {
	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return err
	}

	// Get volume info
	if len(input.SrcVolumeID) != 0 {
		vols, err := d.Inspect([]string{input.SrcVolumeID})
		if err != nil {
			return fmt.Errorf("Volume id not found")
		}
		if len(vols) < 1 {
			return fmt.Errorf("Internal error. Volume found but no data returned")
		}
		vol := vols[0]
		if vol.GetSpec() == nil {
			return fmt.Errorf("Internal error. Volume has no specificiation")
		}
	}

	kvp, err := d.kv.Enumerate(backupsKeyPrefix)
	if err != nil {
		return err
	}
	for _, v := range kvp {
		elem := &fakeBackups{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return err
		}
		if elem.Status.OpType == api.CloudRestoreOp {
			continue
		}
		if len(input.SrcVolumeID) == 0 && len(input.ClusterID) == 0 {
			_, err = d.kv.Delete(v.Key)
		} else if input.SrcVolumeID == elem.Volume.GetId() {
			_, err = d.kv.Delete(v.Key)
		} else if input.ClusterID == elem.ClusterId {
			_, err = d.kv.Delete(v.Key)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// CloudBackupStatus indicates the most recent status of backup/restores
func (d *driver) CloudBackupStatus(input *api.CloudBackupStatusRequest) (*api.CloudBackupStatusResponse, error) {

	clusterInfo, err := d.thisCluster.Enumerate()
	if err != nil {
		return nil, fmt.Errorf("Failed to get cluster information: %v", err)
	}

	statuses := make(map[string]api.CloudBackupStatus)

	kvps, err := d.kv.Enumerate(backupsKeyPrefix)
	if err != nil {
		return nil, err
	}

	for _, v := range kvps {
		elem := &fakeBackups{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		splitKey := strings.Split(v.Key, "/")
		id := splitKey[len(splitKey)-1]
		if input.ID != "" && id == input.ID {
			statuses[id] = elem.Status
			break
		}
		if len(input.SrcVolumeID) == 0 && !input.Local {
			statuses[id] = elem.Status
		} else if input.SrcVolumeID == elem.Volume.GetId() {
			statuses[id] = elem.Status
		} else if input.Local && clusterInfo.NodeId == elem.Status.NodeID {
			statuses[id] = elem.Status
		}
	}

	return &api.CloudBackupStatusResponse{
		Statuses: statuses,
	}, nil
}

// CloudBackupCatalog displays listing of backup content
func (d *driver) CloudBackupCatalog(input *api.CloudBackupCatalogRequest) (*api.CloudBackupCatalogResponse, error) {
	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return nil, err
	}

	// Get the cloud data
	_, _, err := d.backupEntry(input.ID, api.CloudBackupOp)
	if err != nil {
		return nil, err
	}
	return &api.CloudBackupCatalogResponse{
		Contents: []string{
			"/one/two/three.gz",
			"/fake.img",
		},
	}, nil

}

// CloudBackupHistory displays past backup/restore operations on a volume
func (d *driver) CloudBackupHistory(input *api.CloudBackupHistoryRequest) (*api.CloudBackupHistoryResponse, error) {

	kvps, err := d.kv.Enumerate(backupsKeyPrefix)
	if err != nil {
		return nil, err
	}
	items := make([]api.CloudBackupHistoryItem, 0)
	for _, v := range kvps {

		elem := &fakeBackups{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}

		if elem.Status.OpType == api.CloudRestoreOp {
			continue
		}

		if len(input.SrcVolumeID) == 0 {
			items = append(items, api.CloudBackupHistoryItem{
				SrcVolumeID: elem.Info.SrcVolumeID,
				Timestamp:   elem.Status.CompletedTime,
				Status:      string(elem.Status.Status),
			})
		} else if input.SrcVolumeID == elem.Info.SrcVolumeID {
			items = append(items, api.CloudBackupHistoryItem{
				SrcVolumeID: elem.Info.SrcVolumeID,
				Timestamp:   elem.Status.CompletedTime,
				Status:      string(elem.Status.Status),
			})
		}
	}

	return &api.CloudBackupHistoryResponse{
		HistoryList: items,
	}, nil
}

// CloudBackupStateChange allows a current backup state transitions(pause/resume/stop)
func (d *driver) CloudBackupStateChange(input *api.CloudBackupStateChangeRequest) error {

	if len(input.Name) == 0 {
		return fmt.Errorf("Name of the task must be provided")
	}

	resp, err := d.CloudBackupStatus(&api.CloudBackupStatusRequest{
		ID: input.Name,
	})
	if err != nil {
		return err
	}

	for _, status := range resp.Statuses {
		save := false
		if status.Status == api.CloudBackupStatusPaused {
			save = true
			if input.RequestedState == api.CloudBackupRequestedStateResume {
				status.Status = api.CloudBackupStatusActive
			} else if input.RequestedState == api.CloudBackupRequestedStateStop {
				status.Status = api.CloudBackupStatusStopped
			}
		} else if status.Status == api.CloudBackupStatusActive {
			save = true
			if input.RequestedState == api.CloudBackupRequestedStatePause {
				status.Status = api.CloudBackupStatusPaused
			} else if input.RequestedState == api.CloudBackupRequestedStateStop {
				status.Status = api.CloudBackupStatusStopped
			}
		}

		if save {
			var elem *fakeBackups
			_, err := d.kv.GetVal(backupsKeyPrefix+"/"+input.Name, &elem)
			if err != nil {
				return err
			}
			elem.Status = status
			_, err = d.kv.Update(backupsKeyPrefix+"/"+input.Name, elem, 0)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// CloudBackupSchedCreate creates a schedule backup volume to cloud
func (d *driver) CloudBackupSchedCreate(
	input *api.CloudBackupSchedCreateRequest,
) (*api.CloudBackupSchedCreateResponse, error) {

	// Confirm credential id
	if err := d.CredsValidate(input.CredentialUUID); err != nil {
		return nil, err
	}

	// Check volume
	vols, err := d.Inspect([]string{input.SrcVolumeID})
	if err != nil {
		return nil, fmt.Errorf("Volume id not found")
	}
	if len(vols) < 1 {
		return nil, fmt.Errorf("Internal error. Volume found but no data returned")
	}
	vol := vols[0]
	if vol.GetSpec() == nil {
		return nil, fmt.Errorf("Internal error. Volume has no specificiation")
	}

	id := uuid.New()
	_, err = d.kv.Put(schedPrefix+"/"+id, &fakeSchedules{
		Id: id,
		Info: api.CloudBackupScheduleInfo{
			SrcVolumeID:    input.SrcVolumeID,
			CredentialUUID: input.CredentialUUID,
			Schedule:       input.Schedule,
			MaxBackups:     input.MaxBackups,
		},
	}, 0)
	if err != nil {
		return nil, err
	}

	return &api.CloudBackupSchedCreateResponse{
		UUID: id,
	}, nil
}

// CloudBackupSchedDelete delete a volume backup schedule to cloud
func (d *driver) CloudBackupSchedDelete(input *api.CloudBackupSchedDeleteRequest) error {
	d.kv.Delete(schedPrefix + "/" + input.UUID)
	return nil
}

// CloudBackupSchedEnumerate enumerates the configured backup schedules in the cluster
func (d *driver) CloudBackupSchedEnumerate() (*api.CloudBackupSchedEnumerateResponse, error) {
	kvp, err := d.kv.Enumerate(schedPrefix)
	if err != nil {
		return nil, err
	}
	schedules := make(map[string]api.CloudBackupScheduleInfo, len(kvp))
	for _, v := range kvp {
		elem := &fakeSchedules{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		schedules[elem.Id] = elem.Info
	}

	return &api.CloudBackupSchedEnumerateResponse{
		Schedules: schedules,
	}, nil
}

func (d *driver) Catalog(volumeID, path, depth string) (api.CatalogResponse, error) {
	return api.CatalogResponse{
		Root: &api.Catalog{
			Name:         "",
			Path:         "/var/lib/osd/catalog/12345678",
			Type:         "Directory",
			Size:         4096,
			LastModified: &timestamp.Timestamp{},
		},
		Report: &api.Report{
			Directories: 0,
			Files:       0,
		},
	}, nil
}

func (d *driver) VolService(volumeID string, vtreq *api.VolumeServiceRequest) (*api.VolumeServiceResponse, error) {
	return nil, volume.ErrNotSupported
}
