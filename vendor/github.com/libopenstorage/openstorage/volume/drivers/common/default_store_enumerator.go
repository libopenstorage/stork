package common

import (
	"context"
	"encoding/json"
	"fmt"

	// TODO(pedge): what is this for?
	_ "sync"

	"github.com/portworx/kvdb"

	"github.com/libopenstorage/openstorage/api"
)

const (
	keyBase = "openstorage"
)

type defaultStoreEnumerator struct {
	driver string
	kvdb   kvdb.Kvdb
}

func newDefaultStoreEnumerator(driver string, kvdb kvdb.Kvdb) *defaultStoreEnumerator {
	return &defaultStoreEnumerator{
		kvdb:   kvdb,
		driver: driver,
	}
}

func (e *defaultStoreEnumerator) toID(value string) string {
	// Check if the value is the name
	volumes, err := e.Enumerate(&api.VolumeLocator{Name: value}, nil)
	if err != nil {
		return value
	}

	if len(volumes) == 1 {
		return volumes[0].GetId()
	}

	return value
}

// Lock volume specified by volumeID.
func (e *defaultStoreEnumerator) Lock(volumeID string) (interface{}, error) {
	volumeID = e.toID(volumeID)
	return e.kvdb.Lock(e.lockKey(volumeID))
}

// Lock volume with token obtained from call to Lock.
func (e *defaultStoreEnumerator) Unlock(token interface{}) error {
	v, ok := token.(*kvdb.KVPair)
	if !ok {
		return fmt.Errorf("Invalid token of type %T", token)
	}
	return e.kvdb.Unlock(v)
}

// CreateVol returns error if volume with the same ID already existe.
func (e *defaultStoreEnumerator) CreateVol(vol *api.Volume) error {
	_, err := e.kvdb.Create(e.volKey(vol.Id), vol, 0)
	return err
}

// GetVol from volumeID.
func (e *defaultStoreEnumerator) GetVol(volumeID string) (*api.Volume, error) {
	var v api.Volume
	volumeID = e.toID(volumeID)
	_, err := e.kvdb.GetVal(e.volKey(volumeID), &v)
	return &v, err
}

// UpdateVol with vol
func (e *defaultStoreEnumerator) UpdateVol(vol *api.Volume) error {
	_, err := e.kvdb.Put(e.volKey(vol.Id), vol, 0)
	return err
}

// DeleteVol. Returns error if volume does not exist.
func (e *defaultStoreEnumerator) DeleteVol(volumeID string) error {
	volumeID = e.toID(volumeID)
	_, err := e.kvdb.Delete(e.volKey(volumeID))
	return err
}

// Inspect specified volumes.
// Returns slice of volumes that were found.
func (e *defaultStoreEnumerator) Inspect(ctx context.Context, ids []string) ([]*api.Volume, error) {
	volumes := make([]*api.Volume, 0, len(ids))
	for _, id := range ids {
		volume, err := e.GetVol(id)
		// XXX Distinguish between ENOENT and an internal error from KVDB
		if err != nil {
			continue
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

// Enumerate volumes that map to the volumeLocator. Locator fields may be regexp.
// If locator fields are left blank, this will return all volumee.
func (e *defaultStoreEnumerator) Enumerate(
	locator *api.VolumeLocator,
	labels map[string]string,
) ([]*api.Volume, error) {

	for i, id := range locator.GetVolumeIds() {
		locator.GetVolumeIds()[i] = e.toID(id)
	}

	kvp, err := e.kvdb.Enumerate(e.volKeyPrefix())
	if err != nil {
		return nil, err
	}
	volumes := make([]*api.Volume, 0, len(kvp))
	for _, v := range kvp {
		elem := &api.Volume{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		if match(elem, locator, labels) {
			volumes = append(volumes, elem)
		}
	}
	return volumes, nil
}

// SnapEnumerate for specified volume
func (e *defaultStoreEnumerator) SnapEnumerate(
	volumeIDs []string,
	labels map[string]string,
) ([]*api.Volume, error) {
	kvp, err := e.kvdb.Enumerate(e.volKeyPrefix())
	if err != nil {
		return nil, err
	}
	volumes := make([]*api.Volume, 0, len(kvp))
	for _, v := range kvp {
		elem := &api.Volume{}
		if err := json.Unmarshal(v.Value, elem); err != nil {
			return nil, err
		}
		if elem.Source == nil ||
			elem.Source.Parent == "" ||
			(volumeIDs != nil && !contains(elem.Source.Parent, volumeIDs)) {
			continue
		}
		if hasSubset(elem.Locator.VolumeLabels, labels) {
			volumes = append(volumes, elem)
		}
	}
	return volumes, nil
}

func (e *defaultStoreEnumerator) lockKey(volumeID string) string {
	return e.volKeyPrefix() + volumeID + ".lock"
}

func (e *defaultStoreEnumerator) volKey(volumeID string) string {
	return e.volKeyPrefix() + volumeID
}

// TODO(pedge): not used - bug?
func (e *defaultStoreEnumerator) lockKeyPrefix() string {
	return fmt.Sprintf("%s/%s/locks/", keyBase, e.driver)
}

func (e *defaultStoreEnumerator) volKeyPrefix() string {
	return fmt.Sprintf("%s/%s/volumes/", keyBase, e.driver)
}

func hasSubset(set map[string]string, subset map[string]string) bool {
	if subset == nil || len(subset) == 0 {
		return true
	}
	if set == nil {
		return false
	}
	for k, subv := range subset {
		if v, ok := set[k]; ok {
			if v != subv {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func contains(volumeID string, set []string) bool {
	if len(set) == 0 {
		return true
	}
	for _, v := range set {
		if v == volumeID {
			return true
		}
	}
	return false
}

func match(
	v *api.Volume,
	locator *api.VolumeLocator,
	volumeLabels map[string]string,
) bool {
	if locator == nil {
		return hasSubset(v.Locator.VolumeLabels, volumeLabels)
	}

	if len(locator.GetVolumeIds()) != 0 && !contains(v.GetId(), locator.GetVolumeIds()) {
		return false
	}

	if locator.GetGroup() != nil {
		if v.GetSpec().GetGroup() == nil || !v.GetSpec().GetGroup().IsMatch(locator.GetGroup()) {
			return false
		}
	}

	if locator.GetOwnership() != nil {
		// They asked to match an ownership. Now check if the volume has it
		// and if it matches.
		// Keep these separate if statements to make it readable.
		if v.GetSpec().GetOwnership() == nil ||
			!v.GetSpec().GetOwnership().IsMatch(locator.GetOwnership()) {
			return false
		}
	}
	if locator.Name != "" && v.Locator.Name != locator.Name {
		return false
	}
	return hasSubset(v.Locator.VolumeLabels, locator.VolumeLabels)
}
