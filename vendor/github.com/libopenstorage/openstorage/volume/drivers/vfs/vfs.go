package vfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/libopenstorage/openstorage/volume/drivers/common"
	"github.com/pborman/uuid"
	"github.com/portworx/kvdb"
)

const (
	// Name of the driver
	Name = "vfs"
	// Type of the driver
	Type = api.DriverType_DRIVER_TYPE_FILE
	// freezebin free binary
	freezebin = "/usr/sbin/fsfreeze"
)

type driver struct {
	volume.IODriver
	volume.BlockDriver
	volume.SnapshotDriver
	volume.StoreEnumerator
	volume.StatsDriver
	volume.CredsDriver
	volume.CloudBackupDriver
	volume.CloudMigrateDriver
	volume.FilesystemTrimDriver
	volume.FilesystemCheckDriver
	volume.VerifyChecksumDriver
}

// Init Driver intialization.
func Init(params map[string]string) (volume.VolumeDriver, error) {
	return &driver{
		volume.IONotSupported,
		volume.BlockNotSupported,
		volume.SnapshotNotSupported,
		common.NewDefaultStoreEnumerator(Name, kvdb.Instance()),
		volume.StatsNotSupported,
		volume.CredsNotSupported,
		volume.CloudBackupNotSupported,
		volume.CloudMigrateNotSupported,
		volume.FilesystemTrimNotSupported,
		volume.FilesystemCheckNotSupported,
		volume.VerifyChecksumNotSupported,
	}, nil
}

func (d *driver) StartVolumeWatcher() {
	return
}

func (d *driver) GetVolumeWatcher(locator *api.VolumeLocator, labels map[string]string) (chan *api.Volume, error) {
	return nil, nil
}

func (d *driver) StopVolumeWatcher() {
	return
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
		Version: "1.0.0",
	}, nil
}

func (d *driver) Create(ctx context.Context, locator *api.VolumeLocator, source *api.Source, spec *api.VolumeSpec) (string, error) {
	volumeID := strings.TrimSuffix(uuid.New(), "\n")
	// Create a directory on the Local machine with this UUID.
	if err := os.MkdirAll(filepath.Join(volume.VolumeBase, string(volumeID)), 0744); err != nil {
		return "", err
	}
	v := common.NewVolume(
		volumeID,
		api.FSType_FS_TYPE_VFS,
		locator,
		source,
		spec,
	)
	v.DevicePath = filepath.Join(volume.VolumeBase, volumeID)
	if err := d.CreateVol(v); err != nil {
		return "", err
	}
	return v.Id, d.UpdateVol(v)
}

func (d *driver) Delete(ctx context.Context, volumeID string) error {
	if _, err := d.GetVol(volumeID); err != nil {
		return err
	}
	os.RemoveAll(filepath.Join(volume.VolumeBase, string(volumeID)))
	if err := d.DeleteVol(volumeID); err != nil {
		return err
	}
	return nil

}

func (d *driver) MountedAt(ctx context.Context, mountpath string) string {
	return ""
}

// Mount volume at specified path
// Errors ErrEnoEnt, ErrVolDetached may be returned.
func (d *driver) Mount(ctx context.Context, volumeID string, mountpath string, options map[string]string) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		logrus.Println(err)
		return err
	}
	if len(v.AttachPath) > 0 && len(v.AttachPath) > 0 {
		return fmt.Errorf("Volume %q already mounted at %q", volumeID, v.AttachPath[0])
	}
	syscall.Unmount(mountpath, 0)
	if err := syscall.Mount(
		filepath.Join(volume.VolumeBase, string(volumeID)),
		mountpath,
		string(v.Spec.Format),
		syscall.MS_BIND, "",
	); err != nil {
		logrus.Printf("Cannot mount %s at %s because %+v",
			filepath.Join(volume.VolumeBase, string(volumeID)),
			mountpath,
			err,
		)
		return err
	}
	if v.AttachPath == nil {
		v.AttachPath = make([]string, 1)
	}
	v.AttachPath[0] = mountpath
	return d.UpdateVol(v)
}

// Unmount volume at specified path
// Errors ErrEnoEnt, ErrVolDetached may be returned.
func (d *driver) Unmount(ctx context.Context, volumeID string, mountpath string, options map[string]string) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		return err
	}
	if len(v.AttachPath) == 0 || len(v.AttachPath[0]) == 0 {
		return fmt.Errorf("Device %v not mounted", volumeID)
	}
	if err := syscall.Unmount(v.AttachPath[0], 0); err != nil {
		return err
	}
	v.AttachPath = nil
	return d.UpdateVol(v)
}

func (d *driver) Set(ctx context.Context, volumeID string, locator *api.VolumeLocator, spec *api.VolumeSpec) error {
	if spec != nil {
		return volume.ErrNotSupported
	}
	v, err := d.GetVol(volumeID)
	if err != nil {
		return err
	}
	if locator != nil {
		v.Locator = locator
	}
	return d.UpdateVol(v)
}

func (d *driver) Status() [][2]string {
	return [][2]string{}
}

func (d *driver) Shutdown() {}

func (d *driver) fsFreeze(volumeID string, freeze bool) error {
	v, err := d.GetVol(volumeID)
	if err != nil {
		logrus.Println(err)
		return err
	}
	if len(v.AttachPath) == 0 {
		if !freeze {
			return nil
		}
		return fmt.Errorf("Volume not mounted")
	}
	freezeOpt := "-f"
	if !freeze {
		freezeOpt = "-u"
	}
	_, err = exec.Command(freezebin, freezeOpt,
		v.AttachPath[0]).Output()
	return err
}

func (d *driver) Quiesce(
	volumeID string,
	timeoutSec uint64,
	quiesceID string,
) error {
	if err := d.fsFreeze(volumeID, true); err != nil {
		return err
	}
	if timeoutSec > 0 {
		go func() {
			time.Sleep(time.Duration(timeoutSec) * time.Second)
			d.Unquiesce(volumeID)
		}()
	}
	return nil
}

func (d *driver) Unquiesce(volumeID string) error {
	return d.fsFreeze(volumeID, false)
}

func (d *driver) Catalog(volumeID, path string, depth string) (api.CatalogResponse, error) {
	return api.CatalogResponse{}, volume.ErrNotSupported
}

func (d *driver) VolService(volumeID string, vtreq *api.VolumeServiceRequest) (*api.VolumeServiceResponse, error) {
	return nil, volume.ErrNotSupported
}
