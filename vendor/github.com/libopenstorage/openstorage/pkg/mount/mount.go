//go:build linux
// +build linux

package mount

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/libopenstorage/openstorage/pkg/chattr"
	"github.com/libopenstorage/openstorage/pkg/keylock"
	"github.com/libopenstorage/openstorage/pkg/options"
	"github.com/libopenstorage/openstorage/pkg/sched"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/moby/sys/mountinfo"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
)

// Manager defines the interface for keep track of volume driver mounts.
type Manager interface {
	// String representation of the mount table
	String() string
	// Reload mount table for specified device.
	Reload(source string) error
	// Load mount table for all devices that match the list of identifiers
	Load(source []*regexp.Regexp) error
	// Inspect mount table for specified source. ErrEnoent may be returned.
	Inspect(source string) []*PathInfo
	// Mounts returns paths for specified source.
	Mounts(source string) []string
	// HasMounts determines returns the number of mounts for the source.
	HasMounts(source string) int
	// HasTarget determines returns the number of mounts for the target.
	HasTarget(target string) (string, bool)
	// Exists returns true if the device is mounted at specified path.
	// returned if the device does not exists.
	Exists(source, path string) (bool, error)
	// GetRootPath scans mounts for a specified mountPath and returns the
	// rootPath if found or returns an ErrEnoent
	GetRootPath(mountPath string) (string, error)
	// GetSourcePath scans mount for a specified mountPath and returns the
	// sourcePath if found or returns an ErrEnoent
	GetSourcePath(mountPath string) (string, error)
	// GetSourcePaths returns all source paths from the mount table
	GetSourcePaths() []string
	// Mount device at mountpoint
	Mount(
		minor int,
		device string,
		path string,
		fs string,
		flags uintptr,
		data string,
		timeout int,
		opts map[string]string) error
	// Unmount device at mountpoint and remove from the matrix.
	// ErrEnoent is returned if the device or mountpoint for the device
	// is not found.
	Unmount(source, path string, flags int, timeout int, opts map[string]string) error
	// RemoveMountPath removes the given path
	RemoveMountPath(path string, opts map[string]string) error
	// EmptyTrashDir removes all directories from the mounter trash directory
	EmptyTrashDir() error
}

// MountImpl backend implementation for Mount/Unmount calls
type MountImpl interface {
	Mount(source, target, fstype string, flags uintptr, data string, timeout int) error
	Unmount(target string, flags int, timeout int) error
}

// MountType indicates different mount types supported
type MountType int

const (
	// DeviceMount indicates a device mount type
	DeviceMount MountType = 1 << iota
	// NFSMount indicates a NFS mount point
	NFSMount
	// CustomMount indicates a custom mount type with its
	// own defined way of handling mount table
	CustomMount
	// BindMount indicates a bind mount point
	BindMount
	// RawMount indicates a raw mount point
	RawMount
)

const (
	mountPathRemoveDelay = 30 * time.Second
	testDeviceEnv        = "Test_Device_Mounter"
	bindMountPrefix      = "readonly"
	statTimeout          = 30 * time.Second
)

var (
	// ErrExist is returned if path is already mounted to a different device.
	ErrExist = errors.New("Mountpath already exists")
	// ErrEnoent is returned for a non existent mount point
	ErrEnoent = errors.New("Mountpath is not mounted")
	// ErrEinval is returned is fields for an entry do no match
	// existing fields
	ErrEinval = errors.New("Invalid arguments for mount entry")
	// ErrUnsupported is returned for an unsupported operation or a mount type.
	ErrUnsupported = errors.New("Not supported")
	// ErrMountpathNotAllowed is returned when the requested mountpath is not
	// a part of the provided allowed mount paths
	ErrMountpathNotAllowed = errors.New("Mountpath is not allowed")
)

// DeviceMap map device name to Info
type DeviceMap map[string]*Info

// PathMap map path name to device
type PathMap map[string]string

// PathInfo is a reference counted path
type PathInfo struct {
	Root string
	Path string
}

// Info per device
type Info struct {
	sync.Mutex
	Device string
	Minor  int
	// Guarded using the above lock.
	Mountpoint []*PathInfo
	Fs         string
	// Guarded using Mounter.Lock()
	MountsInProgress int
}

// Mounter implements Ops and keeps track of active mounts for volume drivers.
type Mounter struct {
	sync.Mutex
	mountImpl     MountImpl
	mounts        DeviceMap
	paths         PathMap
	allowedDirs   []string
	kl            keylock.KeyLock
	trashLocation string
}

type findMountPoint func(source *mountinfo.Info, destination *regexp.Regexp, mountInfo []*mountinfo.Info) (bool, string, string)

// DefaultMounter defaults to syscall implementation.
type DefaultMounter struct {
}

// Mount default mount implementation is syscall.
func (m *DefaultMounter) Mount(
	source string,
	target string,
	fstype string,
	flags uintptr,
	data string,
	timeout int,
) error {
	return syscall.Mount(source, target, fstype, flags, data)
}

// Unmount default unmount implementation is syscall.
func (m *DefaultMounter) Unmount(target string, flags int, timeout int) error {
	return syscall.Unmount(target, flags)
}

// String representation of Mounter
func (m *Mounter) String() string {
	s := struct {
		mounts        DeviceMap
		paths         PathMap
		allowedDirs   []string
		trashLocation string
	}{
		mounts:        m.mounts,
		paths:         m.paths,
		allowedDirs:   m.allowedDirs,
		trashLocation: m.trashLocation,
	}

	return fmt.Sprintf("%#v", s)
}

// Inspect mount table for device
func (m *Mounter) Inspect(sourcePath string) []*PathInfo {
	m.Lock()
	defer m.Unlock()

	v, ok := m.mounts[sourcePath]
	if !ok {
		return nil
	}
	return v.Mountpoint
}

// Mounts returns  mount table for device
func (m *Mounter) Mounts(sourcePath string) []string {
	m.Lock()
	defer m.Unlock()

	v, ok := m.mounts[sourcePath]
	if !ok {
		return nil
	}

	mounts := make([]string, len(v.Mountpoint))
	for i, v := range v.Mountpoint {
		mounts[i] = v.Path
	}

	return mounts
}

// GetSourcePaths returns all source paths from the mount table
func (m *Mounter) GetSourcePaths() []string {
	m.Lock()
	defer m.Unlock()

	sourcePaths := make([]string, len(m.mounts))
	i := 0
	for path := range m.mounts {
		sourcePaths[i] = path
		i++
	}
	return sourcePaths
}

// HasMounts determines returns the number of mounts for the device.
func (m *Mounter) HasMounts(sourcePath string) int {
	m.Lock()
	defer m.Unlock()

	v, ok := m.mounts[sourcePath]
	if !ok {
		return 0
	}
	return len(v.Mountpoint)
}

// HasTarget returns true/false based on the target provided
func (m *Mounter) HasTarget(targetPath string) (string, bool) {
	m.Lock()
	defer m.Unlock()

	for k, v := range m.mounts {
		for _, p := range v.Mountpoint {
			if p.Path == targetPath {
				return k, true
			}
		}
	}
	return "", false
}

// Exists scans mountpaths for specified device and returns true if path is one of the
// mountpaths. ErrEnoent may be retuned if the device is not found
func (m *Mounter) Exists(sourcePath string, path string) (bool, error) {
	m.Lock()
	defer m.Unlock()

	v, ok := m.mounts[sourcePath]
	if !ok {
		return false, ErrEnoent
	}
	for _, p := range v.Mountpoint {
		if p.Path == path {
			return true, nil
		}
	}
	return false, nil
}

// GetRootPath scans mounts for a specified mountPath and return the
// rootPath if found or returns an ErrEnoent
func (m *Mounter) GetRootPath(mountPath string) (string, error) {
	m.Lock()
	defer m.Unlock()

	for _, v := range m.mounts {
		for _, p := range v.Mountpoint {
			if p.Path == mountPath {
				return p.Root, nil
			}
		}
	}
	return "", ErrEnoent
}

// GetSourcePath scans mount for a specified mountPath and returns the sourcePath
// if found or returnes an ErrEnoent
func (m *Mounter) GetSourcePath(mountPath string) (string, error) {
	m.Lock()
	defer m.Unlock()

	for k, v := range m.mounts {
		for _, p := range v.Mountpoint {
			if p.Path == mountPath {
				return k, nil
			}
		}
	}
	return "", ErrEnoent
}

func normalizeMountPath(mountPath string) string {
	if len(mountPath) > 1 && strings.HasSuffix(mountPath, "/") {
		return mountPath[:len(mountPath)-1]
	}
	return mountPath
}

func (m *Mounter) maybeRemoveDevice(device string) {
	m.Lock()
	defer m.Unlock()
	if info, ok := m.mounts[device]; ok {
		// If the device has no more mountpoints and no mounts in progress, remove it from the map
		if len(info.Mountpoint) == 0 && info.MountsInProgress == 0 {
			delete(m.mounts, device)
		}
	}
}

// reload from newM
func (m *Mounter) reload(device string, newM *Info) error {
	m.Lock()
	defer m.Unlock()

	// New mountable has no mounts, delete old mounts.
	if newM == nil {
		delete(m.mounts, device)
		return nil
	}

	// Old mountable had no mounts, copy over new mounts.
	oldM, ok := m.mounts[device]
	if !ok {
		m.mounts[device] = newM
		return nil
	}

	// Overwrite old mount entries into new mount table, preserving refcnt.
	for _, oldP := range oldM.Mountpoint {
		for j, newP := range newM.Mountpoint {
			if newP.Path == oldP.Path {
				newM.Mountpoint[j] = oldP
				break
			}
		}
	}

	// Purge old mounts.
	m.mounts[device] = newM
	return nil
}

func (m *Mounter) load(prefixes []*regexp.Regexp, fmp findMountPoint) error {
	info, err := GetMounts()
	if err != nil {
		return err
	}
	for _, v := range info {
		var (
			sourcePath, devicePath, targetDevice string
			foundPrefix, foundTarget             bool
		)
		for _, devPrefix := range prefixes {
			foundPrefix, sourcePath, devicePath = fmp(v, devPrefix, info)
			targetDevice = getTargetDevice(devPrefix.String())
			if !foundPrefix && targetDevice != "" {
				foundTarget, _, _ = fmp(v, regexp.MustCompile(regexp.QuoteMeta(targetDevice)), info)
				// We could not find a mountpoint for devPrefix (/dev/mapper/vg-lvm1) but found
				// one for its target device (/dev/dm-0). Change the sourcePath to devPrefix
				// as fmp might have returned an incorrect or empty sourcePath
				sourcePath = devPrefix.String()
				devicePath = devPrefix.String()
			}

			if foundPrefix || foundTarget {
				break
			}
		}
		if !foundPrefix && !foundTarget {
			continue
		}

		addMountTableEntry := func(mountSourcePath, deviceSourcePath string, updatePaths bool) {
			mount, ok := m.mounts[mountSourcePath]
			if !ok {
				mount = &Info{
					Device:     deviceSourcePath,
					Fs:         v.FSType,
					Minor:      v.Minor,
					Mountpoint: make([]*PathInfo, 0),
				}
				m.mounts[mountSourcePath] = mount
			}
			// Allow Load to be called multiple times.
			for _, p := range mount.Mountpoint {

				if p.Path == v.Mountpoint {
					// No need of updating Mountpoint
					return
				}
			}
			pi := &PathInfo{
				Root: normalizeMountPath(v.Root),
				Path: normalizeMountPath(v.Mountpoint),
			}
			mount.Mountpoint = append(mount.Mountpoint, pi)
			if updatePaths {
				m.paths[v.Mountpoint] = mountSourcePath
			}
		}
		// Only update the paths map with the device with which load was called.
		addMountTableEntry(sourcePath, devicePath, true /*updatePaths*/)

		// Add a mountpoint entry for the target device as well.
		if targetDevice == "" {
			continue
		}
		addMountTableEntry(targetDevice, targetDevice, false /*updatePaths*/)
	}
	return nil
}

// Mount new mountpoint for specified device.
func (m *Mounter) Mount(
	minor int,
	devPath, path, fs string,
	flags uintptr,
	data string,
	timeout int,
	opts map[string]string,
) error {
	// device gets overwritten if opts specifies fuse mount with
	// options.OptionsDeviceFuseMount.
	device := devPath
	if value, ok := opts[options.OptionsDeviceFuseMount]; ok {
		// fuse mounts show-up with this key as device.
		device = value
	}

	path = normalizeMountPath(path)
	if len(m.allowedDirs) > 0 {
		foundPrefix := false
		for _, allowedDir := range m.allowedDirs {
			if strings.Contains(path, allowedDir) {
				foundPrefix = true
				break
			}
		}
		if !foundPrefix {
			return ErrMountpathNotAllowed
		}
	}
	dev, ok := m.HasTarget(path)
	if ok && dev != device {
		logrus.Warnf("cannot mount %q,  device %q is mounted at %q", device, dev, path)
		return ErrExist
	}
	m.Lock()
	info, ok := m.mounts[device]
	if !ok {
		info = &Info{
			Device:     device,
			Mountpoint: make([]*PathInfo, 0),
			Minor:      minor,
			Fs:         fs,
		}
	}
	// This variable in Info Structure is guarded using m.Lock()
	info.MountsInProgress++
	m.mounts[device] = info
	m.Unlock()
	info.Lock()
	defer func() {
		info.Unlock()
		m.Lock()
		// Info is not destroyed, even when we don't hold the lock.
		info.MountsInProgress--
		m.Unlock()
	}()

	// Validate input params
	// FS check is not needed if it is a bind mount
	if !strings.HasPrefix(info.Fs, fs) && (flags&syscall.MS_BIND) != syscall.MS_BIND {
		logrus.Warnf("%s Existing mountpoint has fs %q cannot change to %q",
			device, info.Fs, fs)
		return ErrEinval
	}

	// Try to find the mountpoint. If it already exists, do nothing
	for _, p := range info.Mountpoint {
		if p.Path == path {
			logrus.Infof("%q mountpoint for device %q already exists",
				device, path)
			return nil
		}
	}

	h := m.kl.Acquire(path)
	defer m.kl.Release(&h)

	// Record previous state of the path
	pathWasReadOnly := m.isPathSetImmutable(path)
	var (
		isBindMounted bool = false
		bindMountPath string
	)

	if err := m.makeMountpathReadOnly(path); err != nil {
		if strings.Contains(err.Error(), "Inappropriate ioctl for device") {
			logrus.Warnf("failed to make %s readonly. Err: %v", path, err)
			// If we cannot chattr the original mount path, we bind mount it to
			// a path in osd mount path and then chattr it
			if bindMountPath, err = m.bindMountOriginalPath(path); err != nil {
				return err
			}
			isBindMounted = true
		} else {
			return fmt.Errorf("failed to make %s readonly. Err: %v", path, err)
		}
	}

	// The device is not mounted at path, mount it and add to its mountpoints.
	if err := m.mountImpl.Mount(devPath, path, fs, flags, data, timeout); err != nil {
		// Rollback only if was writeable
		if !pathWasReadOnly {
			if e := m.makeMountpathWriteable(path); e != nil {
				return fmt.Errorf("failed to make %v writeable during rollback. Err: %v Mount err: %v",
					path, e, err)
			}
			if isBindMounted {
				if cleanupErr := m.cleanupBindMount(path, bindMountPath, err); cleanupErr != nil {
					return cleanupErr
				}
			}
		}

		return err
	}

	info.Mountpoint = append(info.Mountpoint, &PathInfo{Path: path})

	return nil
}

func (m *Mounter) bindMountOriginalPath(path string) (string, error) {
	bindMountPath := filepath.Join(volume.MountBase, bindMountPrefix, uuid.New())
	if err := os.MkdirAll(bindMountPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create bind mount directory %v. Err: %v",
			bindMountPath, err)
	}

	// Create a bind mount in osd mount path from the original mount path which
	// we can chattr instead of the original path
	if err := m.mountImpl.Mount(bindMountPath, path, "", syscall.MS_BIND, "", 0); err != nil {
		if e := os.Remove(bindMountPath); e != nil {
			logrus.Warnf("Failed to remove the bind mount dir %v. Err: %v Mount err: %v",
				bindMountPath, e, err)
		}
		return "", fmt.Errorf("failed to bind mount %v to %v. Err: %v", path, bindMountPath, err)
	}
	logrus.Infof("Successfully bind mounted path [%v] on [%v]", bindMountPath, path)

	if err := m.makeMountpathReadOnly(path); err != nil {
		if cleanupErr := m.cleanupBindMount(path, bindMountPath, err); cleanupErr != nil {
			logrus.Warnf(cleanupErr.Error())
		}
		return "", fmt.Errorf("failed to make %s readonly after bind mounting. Err: %v",
			path, err)
	}
	return bindMountPath, nil
}

func (m *Mounter) cleanupBindMount(path, bindMountPath string, err error) error {
	if e := m.mountImpl.Unmount(path, syscall.MS_BIND, 0); e != nil {
		return fmt.Errorf("failed to unmount bind mounted path %s. Err: %v Mount err: %v",
			path, e, err)
	}
	if e := os.Remove(bindMountPath); e != nil {
		return fmt.Errorf("failed to remove the bind mount dir %v. Err: %v Mount err: %v",
			bindMountPath, e, err)
	}
	return nil
}

// Unmount device at mountpoint and from the matrix.
// ErrEnoent is returned if the device or mountpoint for the device is not found.
func (m *Mounter) Unmount(
	devPath string,
	path string,
	flags int,
	timeout int,
	opts map[string]string,
) error {
	m.Lock()
	// device gets overwritten if opts specifies fuse mount with
	// options.OptionsDeviceFuseMount.
	device := devPath
	path = normalizeMountPath(path)
	if value, ok := opts[options.OptionsDeviceFuseMount]; ok {
		// fuse mounts show-up with this key as device.
		device = value
	}
	info, ok := m.mounts[device]
	if !ok {
		logrus.Warnf("Unable to unmount device %q path %q: %v",
			devPath, path, ErrEnoent.Error())
		logrus.Infof("Found %v mounts in mounter's cache: ", len(m.mounts))
		logrus.Infof("Mounter has the following mountpoints: ")
		for dev, info := range m.mounts {
			logrus.Infof("For Device %v: Info: %v", dev, info)
			if info == nil {
				continue
			}
			for _, path := range info.Mountpoint {
				logrus.Infof("\t Mountpath: %v Rootpath: %v", path.Path, path.Root)
			}
		}
		m.Unlock()
		return ErrEnoent
	}
	m.Unlock()
	info.Lock()
	defer info.Unlock()
	for i, p := range info.Mountpoint {
		if p.Path != path {
			continue
		}
		err := m.mountImpl.Unmount(path, flags, timeout)
		if err != nil {
			return err
		}
		// Blow away this mountpoint.
		info.Mountpoint[i] = info.Mountpoint[len(info.Mountpoint)-1]
		info.Mountpoint = info.Mountpoint[0 : len(info.Mountpoint)-1]
		m.maybeRemoveDevice(device)
		if options.IsBoolOptionSet(opts, options.OptionsDeleteAfterUnmount) {
			m.RemoveMountPath(path, opts)
		}

		return nil
	}
	logrus.Warnf("Device %q is not mounted at path %q", device, path)
	return ErrEnoent
}

func (m *Mounter) removeMountPath(path string) error {
	h := m.kl.Acquire(path)
	defer m.kl.Release(&h)

	if devicePath, mounted := m.HasTarget(path); !mounted {
		if err := m.makeMountpathWriteable(path); err != nil {
			logrus.Warnf("Failed to make path: %v writeable. Err: %v", path, err)
			return err
		}
	} else {
		logrus.Infof("Not making %v writeable as %v is mounted on it", path, devicePath)
		return nil
	}

	var bindMountPath string
	bindMounter, err := New(BindMount, nil, []*regexp.Regexp{regexp.MustCompile("")}, nil, []string{}, "")
	if err != nil {
		return err
	}
	if devicePath, mounted := bindMounter.HasTarget(path); mounted {
		bindMountPath, err = bindMounter.GetRootPath(path)
		if err := m.mountImpl.Unmount(path, 0, 0); err != nil {
			return fmt.Errorf("failed to unmount bind mount %v. Err: %v", devicePath, err)
		}
	}

	if _, err := os.Stat(path); err == nil {
		logrus.Infof("Removing mount path directory: %v", path)
		if err = os.Remove(path); err != nil {
			logrus.Warnf("Failed to remove path: %v Err: %v", path, err)
			return err
		}
	}

	if bindMountPath != "" {
		if _, err := os.Stat(bindMountPath); err == nil {
			logrus.Infof("Removing bind mount path source: %v", bindMountPath)
			if err = os.Remove(bindMountPath); err != nil {
				logrus.Warnf("Failed to remove bind mount path: %v Err: %v",
					bindMountPath, err)
				return err
			}
		}
	}
	return nil
}

// RemoveMountPath makes the path writeable and removes it after a fixed delay
func (m *Mounter) RemoveMountPath(mountPath string, opts map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), statTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "stat", mountPath)
	_, err := cmd.CombinedOutput()
	if err == nil {
		if options.IsBoolOptionSet(opts, options.OptionsWaitBeforeDelete) {
			hasher := md5.New()
			hasher.Write([]byte(mountPath))
			symlinkName := hex.EncodeToString(hasher.Sum(nil))
			symlinkPath := path.Join(m.trashLocation, symlinkName)
			if p, err := filepath.EvalSymlinks(symlinkPath); err == nil && p == mountPath {
				// we already scheduled the removal for this mountPath
				logrus.Infof("RemoveMountPath is called where symlink still exists on: %v", symlinkPath)
				return nil
			}

			if err = os.Symlink(mountPath, symlinkPath); err != nil {
				if !os.IsExist(err) {
					logrus.Errorf("Error creating sym link %s => %s. Err: %v", symlinkPath, mountPath, err)
				}
			}

			if _, err = sched.Instance().Schedule(
				func(sched.Interval) {
					logrus.Infof("[RemoveMountPath] Scheduled removing mount path %v ", mountPath)
					if err = m.removeMountPath(mountPath); err != nil {
						return
					}

					if err = os.Remove(symlinkPath); err != nil {
						return
					}
				},
				sched.Periodic(time.Second),
				time.Now().Add(mountPathRemoveDelay),
				true /* run only once */); err != nil {
				logrus.Errorf("Failed to schedule task to remove path:%v. Err: %v", mountPath, err)
				return err
			}
		} else {
			return m.removeMountPath(mountPath)
		}
	}

	return nil
}

func (m *Mounter) EmptyTrashDir() error {
	files, err := ioutil.ReadDir(m.trashLocation)
	if err != nil {
		logrus.Errorf("failed to read trash dir: %s. Err: %v", m.trashLocation, err)
		return err
	}

	if _, err := sched.Instance().Schedule(
		func(sched.Interval) {
			for _, file := range files {
				logrus.Infof("[EmptyTrashDir] Scheduled removing file %v in trash location %v", file.Name(), m.trashLocation)
				e := m.removeSoftlinkAndTarget(path.Join(m.trashLocation, file.Name()))
				if e != nil {
					logrus.Errorf("failed to remove link: %s. Err: %v", path.Join(m.trashLocation, file.Name()), e)
				}
			}
		},
		sched.Periodic(time.Second),
		time.Now().Add(mountPathRemoveDelay),
		true /* run only once */); err != nil {
		logrus.Errorf("Failed to cleanup of trash dir. Err: %v", err)
		return err
	}

	return nil
}

func (m *Mounter) removeSoftlinkAndTarget(link string) error {
	if _, err := os.Stat(link); err == nil {
		target, err := os.Readlink(link)
		if err != nil {
			return err
		}

		if err = m.removeMountPath(target); err != nil {
			return err
		}
	}

	if err := os.Remove(link); err != nil {
		return err
	}

	return nil
}

// isPathSetImmutable returns true on error in getting path info or if path
// is immutable .
func (m *Mounter) isPathSetImmutable(mountpath string) bool {
	return chattr.IsImmutable(mountpath)
}

// makeMountpathReadOnly makes given mountpath read-only
func (m *Mounter) makeMountpathReadOnly(mountpath string) error {
	return chattr.AddImmutable(mountpath)
}

// makeMountpathWriteable makes given mountpath writeable
func (m *Mounter) makeMountpathWriteable(mountpath string) error {
	return chattr.RemoveImmutable(mountpath)
}

// New returns a new Mount Manager
func New(
	mounterType MountType,
	mountImpl MountImpl,
	identifiers []*regexp.Regexp,
	customMounter CustomMounter,
	allowedDirs []string,
	trashLocation string,
) (Manager, error) {

	if mountImpl == nil {
		mountImpl = &DefaultMounter{}
	}

	switch mounterType {
	case DeviceMount:
		return NewDeviceMounter(identifiers, mountImpl, allowedDirs, trashLocation)
	case NFSMount:
		return NewNFSMounter(identifiers, mountImpl, allowedDirs, trashLocation)
	case BindMount:
		return NewBindMounter(identifiers, mountImpl, allowedDirs, trashLocation)
	case CustomMount:
		return NewCustomMounter(identifiers, mountImpl, customMounter, allowedDirs)
	case RawMount:
		return NewRawBindMounter(identifiers, mountImpl, allowedDirs, trashLocation)
	}
	return nil, ErrUnsupported
}

// GetMounts is a wrapper over mount.GetMounts(). It is mainly used to add a switch
// to enable device mounter tests.
func GetMounts() ([]*mountinfo.Info, error) {
	if os.Getenv(testDeviceEnv) != "" {
		return testGetMounts()
	}
	return parseMountTable()
}

var (
	// testMounts is a global test list of mount table entries
	testMounts []*mountinfo.Info
)

// testGetMounts is only used in tests to get the test list of mount table
// entries
func testGetMounts() ([]*mountinfo.Info, error) {
	var err error
	if len(testMounts) == 0 {
		testMounts, err = parseMountTable()
	}
	return testMounts, err
}
