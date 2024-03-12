package maps

import (
	"sync"
)

// SyncMap is simplified implementation of Golang's sync.Map (see https://pkg.go.dev/sync#Map)
// - should be more appropriate for "smaller parallelism" (i.e. less than 4 parallel CPU cores -access)
type SyncMap[K comparable, V any] interface {
	Load(key K) (V, bool)
	LoadUnchecked(key K) V
	Delete(key K)
	Store(key K, value V)
	Range(fn func(key K, value V) bool)
}

type syncMap[K comparable, V any] struct {
	sync.RWMutex
	internal map[K]V
}

// MakeSyncMap creates a new instance of a SyncMap
func MakeSyncMap[K comparable, V any]() SyncMap[K, V] {
	return &syncMap[K, V]{
		internal: make(map[K]V),
	}
}

// Loads retrieves an element out of the map
func (m *syncMap[K, V]) Load(key K) (V, bool) {
	m.RLock()
	defer m.RUnlock()
	result, ok := m.internal[key]
	return result, ok
}

// LoadUnchecked retrieves an element out of the map.
// -note, if element does not exist, equivalent of nil/0 is returned.
func (m *syncMap[K, V]) LoadUnchecked(key K) V {
	result, _ := m.Load(key)
	return result
}

// Delete removes an element from the map
func (m *syncMap[K, V]) Delete(key K) {
	m.Lock()
	defer m.Unlock()
	delete(m.internal, key)
}

// Store puts an element into the map
func (m *syncMap[K, V]) Store(key K, value V) {
	m.Lock()
	defer m.Unlock()
	m.internal[key] = value
}

// Range is used to enumerate the elements of the map.
// - if cb callback func returns false, the enumeration is interrupted
func (m *syncMap[K, V]) Range(cb func(key K, value V) bool) {
	m.RLock()
	defer m.RUnlock()

	for k, v := range m.internal {
		if !cb(k, v) {
			break
		}
	}
}
