// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/libopenstorage/stork/drivers/volume (interfaces: Driver)

// Package drivervolume is a generated GoMock package.
package drivervolume

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	volume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	api "github.com/libopenstorage/openstorage/api"
	volume0 "github.com/libopenstorage/stork/drivers/volume"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8sutils "github.com/libopenstorage/stork/pkg/k8sutils"
	core "github.com/portworx/sched-ops/k8s/core"
	v10 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// MockDriver is a mock of Driver interface.
type MockDriver struct {
	ctrl     *gomock.Controller
	recorder *MockDriverMockRecorder
}

// MockDriverMockRecorder is the mock recorder for MockDriver.
type MockDriverMockRecorder struct {
	mock *MockDriver
}

// NewMockDriver creates a new mock instance.
func NewMockDriver(ctrl *gomock.Controller) *MockDriver {
	mock := &MockDriver{ctrl: ctrl}
	mock.recorder = &MockDriverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDriver) EXPECT() *MockDriverMockRecorder {
	return m.recorder
}

// ActivateClusterDomain mocks base method.
func (m *MockDriver) ActivateClusterDomain(arg0 *v1alpha1.ClusterDomainUpdate) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ActivateClusterDomain", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// ActivateClusterDomain indicates an expected call of ActivateClusterDomain.
func (mr *MockDriverMockRecorder) ActivateClusterDomain(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ActivateClusterDomain", reflect.TypeOf((*MockDriver)(nil).ActivateClusterDomain), arg0)
}

// CancelBackup mocks base method.
func (m *MockDriver) CancelBackup(arg0 *v1alpha1.ApplicationBackup) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelBackup", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelBackup indicates an expected call of CancelBackup.
func (mr *MockDriverMockRecorder) CancelBackup(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelBackup", reflect.TypeOf((*MockDriver)(nil).CancelBackup), arg0)
}

// CancelMigration mocks base method.
func (m *MockDriver) CancelMigration(arg0 *v1alpha1.Migration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelMigration", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelMigration indicates an expected call of CancelMigration.
func (mr *MockDriverMockRecorder) CancelMigration(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelMigration", reflect.TypeOf((*MockDriver)(nil).CancelMigration), arg0)
}

// CancelRestore mocks base method.
func (m *MockDriver) CancelRestore(arg0 *v1alpha1.ApplicationRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelRestore", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelRestore indicates an expected call of CancelRestore.
func (mr *MockDriverMockRecorder) CancelRestore(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelRestore", reflect.TypeOf((*MockDriver)(nil).CancelRestore), arg0)
}

// CleanupBackupResources mocks base method.
func (m *MockDriver) CleanupBackupResources(arg0 *v1alpha1.ApplicationBackup) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CleanupBackupResources", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CleanupBackupResources indicates an expected call of CleanupBackupResources.
func (mr *MockDriverMockRecorder) CleanupBackupResources(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CleanupBackupResources", reflect.TypeOf((*MockDriver)(nil).CleanupBackupResources), arg0)
}

// CleanupRestoreResources mocks base method.
func (m *MockDriver) CleanupRestoreResources(arg0 *v1alpha1.ApplicationRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CleanupRestoreResources", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CleanupRestoreResources indicates an expected call of CleanupRestoreResources.
func (mr *MockDriverMockRecorder) CleanupRestoreResources(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CleanupRestoreResources", reflect.TypeOf((*MockDriver)(nil).CleanupRestoreResources), arg0)
}

// CleanupSnapshotRestoreObjects mocks base method.
func (m *MockDriver) CleanupSnapshotRestoreObjects(arg0 *v1alpha1.VolumeSnapshotRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CleanupSnapshotRestoreObjects", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CleanupSnapshotRestoreObjects indicates an expected call of CleanupSnapshotRestoreObjects.
func (mr *MockDriverMockRecorder) CleanupSnapshotRestoreObjects(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CleanupSnapshotRestoreObjects", reflect.TypeOf((*MockDriver)(nil).CleanupSnapshotRestoreObjects), arg0)
}

// CompleteVolumeSnapshotRestore mocks base method.
func (m *MockDriver) CompleteVolumeSnapshotRestore(arg0 *v1alpha1.VolumeSnapshotRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompleteVolumeSnapshotRestore", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompleteVolumeSnapshotRestore indicates an expected call of CompleteVolumeSnapshotRestore.
func (mr *MockDriverMockRecorder) CompleteVolumeSnapshotRestore(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompleteVolumeSnapshotRestore", reflect.TypeOf((*MockDriver)(nil).CompleteVolumeSnapshotRestore), arg0)
}

// CreateGroupSnapshot mocks base method.
func (m *MockDriver) CreateGroupSnapshot(arg0 *v1alpha1.GroupVolumeSnapshot) (*volume0.GroupSnapshotCreateResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateGroupSnapshot", arg0)
	ret0, _ := ret[0].(*volume0.GroupSnapshotCreateResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateGroupSnapshot indicates an expected call of CreateGroupSnapshot.
func (mr *MockDriverMockRecorder) CreateGroupSnapshot(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateGroupSnapshot", reflect.TypeOf((*MockDriver)(nil).CreateGroupSnapshot), arg0)
}

// CreatePair mocks base method.
func (m *MockDriver) CreatePair(arg0 *v1alpha1.ClusterPair) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreatePair", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreatePair indicates an expected call of CreatePair.
func (mr *MockDriverMockRecorder) CreatePair(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePair", reflect.TypeOf((*MockDriver)(nil).CreatePair), arg0)
}

// CreateVolumeClones mocks base method.
func (m *MockDriver) CreateVolumeClones(arg0 *v1alpha1.ApplicationClone) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateVolumeClones", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateVolumeClones indicates an expected call of CreateVolumeClones.
func (mr *MockDriverMockRecorder) CreateVolumeClones(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateVolumeClones", reflect.TypeOf((*MockDriver)(nil).CreateVolumeClones), arg0)
}

// DeactivateClusterDomain mocks base method.
func (m *MockDriver) DeactivateClusterDomain(arg0 *v1alpha1.ClusterDomainUpdate) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeactivateClusterDomain", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeactivateClusterDomain indicates an expected call of DeactivateClusterDomain.
func (mr *MockDriverMockRecorder) DeactivateClusterDomain(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeactivateClusterDomain", reflect.TypeOf((*MockDriver)(nil).DeactivateClusterDomain), arg0)
}

// DeleteBackup mocks base method.
func (m *MockDriver) DeleteBackup(arg0 *v1alpha1.ApplicationBackup) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteBackup", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteBackup indicates an expected call of DeleteBackup.
func (mr *MockDriverMockRecorder) DeleteBackup(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteBackup", reflect.TypeOf((*MockDriver)(nil).DeleteBackup), arg0)
}

// DeleteGroupSnapshot mocks base method.
func (m *MockDriver) DeleteGroupSnapshot(arg0 *v1alpha1.GroupVolumeSnapshot) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteGroupSnapshot", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteGroupSnapshot indicates an expected call of DeleteGroupSnapshot.
func (mr *MockDriverMockRecorder) DeleteGroupSnapshot(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteGroupSnapshot", reflect.TypeOf((*MockDriver)(nil).DeleteGroupSnapshot), arg0)
}

// DeletePair mocks base method.
func (m *MockDriver) DeletePair(arg0 *v1alpha1.ClusterPair) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeletePair", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeletePair indicates an expected call of DeletePair.
func (mr *MockDriverMockRecorder) DeletePair(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeletePair", reflect.TypeOf((*MockDriver)(nil).DeletePair), arg0)
}

// Failover mocks base method.
func (m *MockDriver) Failover(arg0 *v1alpha1.Action) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Failover", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Failover indicates an expected call of Failover.
func (mr *MockDriverMockRecorder) Failover(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Failover", reflect.TypeOf((*MockDriver)(nil).Failover), arg0)
}

// GetBackupStatus mocks base method.
func (m *MockDriver) GetBackupStatus(arg0 *v1alpha1.ApplicationBackup) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBackupStatus", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// GetBackupStatus indicates an expected call of GetBackupStatus.
func (mr *MockDriverMockRecorder) GetBackupStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBackupStatus", reflect.TypeOf((*MockDriver)(nil).GetBackupStatus), arg0)
}

// GetCSIPodPrefix mocks base method.
func (m *MockDriver) GetCSIPodPrefix() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCSIPodPrefix")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCSIPodPrefix indicates an expected call of GetCSIPodPrefix.
func (mr *MockDriverMockRecorder) GetCSIPodPrefix() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCSIPodPrefix", reflect.TypeOf((*MockDriver)(nil).GetCSIPodPrefix))
}

// GetClusterDomainNodes mocks base method.
func (m *MockDriver) GetClusterDomainNodes() (map[string][]*api.Node, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterDomainNodes")
	ret0, _ := ret[0].(map[string][]*api.Node)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterDomainNodes indicates an expected call of GetClusterDomainNodes.
func (mr *MockDriverMockRecorder) GetClusterDomainNodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterDomainNodes", reflect.TypeOf((*MockDriver)(nil).GetClusterDomainNodes))
}

// GetClusterDomains mocks base method.
func (m *MockDriver) GetClusterDomains() (*v1alpha1.ClusterDomains, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterDomains")
	ret0, _ := ret[0].(*v1alpha1.ClusterDomains)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterDomains indicates an expected call of GetClusterDomains.
func (mr *MockDriverMockRecorder) GetClusterDomains() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterDomains", reflect.TypeOf((*MockDriver)(nil).GetClusterDomains))
}

// GetClusterID mocks base method.
func (m *MockDriver) GetClusterID() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterID")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterID indicates an expected call of GetClusterID.
func (mr *MockDriverMockRecorder) GetClusterID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterID", reflect.TypeOf((*MockDriver)(nil).GetClusterID))
}

// GetGroupSnapshotStatus mocks base method.
func (m *MockDriver) GetGroupSnapshotStatus(arg0 *v1alpha1.GroupVolumeSnapshot) (*volume0.GroupSnapshotCreateResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetGroupSnapshotStatus", arg0)
	ret0, _ := ret[0].(*volume0.GroupSnapshotCreateResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetGroupSnapshotStatus indicates an expected call of GetGroupSnapshotStatus.
func (mr *MockDriverMockRecorder) GetGroupSnapshotStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetGroupSnapshotStatus", reflect.TypeOf((*MockDriver)(nil).GetGroupSnapshotStatus), arg0)
}

// GetMigrationStatus mocks base method.
func (m *MockDriver) GetMigrationStatus(arg0 *v1alpha1.Migration) ([]*v1alpha1.MigrationVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMigrationStatus", arg0)
	ret0, _ := ret[0].([]*v1alpha1.MigrationVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMigrationStatus indicates an expected call of GetMigrationStatus.
func (mr *MockDriverMockRecorder) GetMigrationStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMigrationStatus", reflect.TypeOf((*MockDriver)(nil).GetMigrationStatus), arg0)
}

// GetNodes mocks base method.
func (m *MockDriver) GetNodes() ([]*volume0.NodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNodes")
	ret0, _ := ret[0].([]*volume0.NodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetNodes indicates an expected call of GetNodes.
func (mr *MockDriverMockRecorder) GetNodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNodes", reflect.TypeOf((*MockDriver)(nil).GetNodes))
}

// GetPair mocks base method.
func (m *MockDriver) GetPair(arg0 string) (*api.ClusterPairInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPair", arg0)
	ret0, _ := ret[0].(*api.ClusterPairInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPair indicates an expected call of GetPair.
func (mr *MockDriverMockRecorder) GetPair(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPair", reflect.TypeOf((*MockDriver)(nil).GetPair), arg0)
}

// GetPodPatches mocks base method.
func (m *MockDriver) GetPodPatches(arg0 string, arg1 *v10.Pod) ([]k8sutils.JSONPatchOp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPodPatches", arg0, arg1)
	ret0, _ := ret[0].([]k8sutils.JSONPatchOp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPodPatches indicates an expected call of GetPodPatches.
func (mr *MockDriverMockRecorder) GetPodPatches(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPodPatches", reflect.TypeOf((*MockDriver)(nil).GetPodPatches), arg0, arg1)
}

// GetPodVolumes mocks base method.
func (m *MockDriver) GetPodVolumes(arg0 *v10.PodSpec, arg1 string, arg2 bool) ([]*volume0.Info, []*volume0.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPodVolumes", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*volume0.Info)
	ret1, _ := ret[1].([]*volume0.Info)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetPodVolumes indicates an expected call of GetPodVolumes.
func (mr *MockDriverMockRecorder) GetPodVolumes(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPodVolumes", reflect.TypeOf((*MockDriver)(nil).GetPodVolumes), arg0, arg1, arg2)
}

// GetPreRestoreResources mocks base method.
func (m *MockDriver) GetPreRestoreResources(arg0 *v1alpha1.ApplicationBackup, arg1 *v1alpha1.ApplicationRestore, arg2 []runtime.Unstructured, arg3 []byte) ([]runtime.Unstructured, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPreRestoreResources", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]runtime.Unstructured)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPreRestoreResources indicates an expected call of GetPreRestoreResources.
func (mr *MockDriverMockRecorder) GetPreRestoreResources(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPreRestoreResources", reflect.TypeOf((*MockDriver)(nil).GetPreRestoreResources), arg0, arg1, arg2, arg3)
}

// GetRestoreStatus mocks base method.
func (m *MockDriver) GetRestoreStatus(arg0 *v1alpha1.ApplicationRestore) ([]*v1alpha1.ApplicationRestoreVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRestoreStatus", arg0)
	ret0, _ := ret[0].([]*v1alpha1.ApplicationRestoreVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetRestoreStatus indicates an expected call of GetRestoreStatus.
func (mr *MockDriverMockRecorder) GetRestoreStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRestoreStatus", reflect.TypeOf((*MockDriver)(nil).GetRestoreStatus), arg0)
}

// GetSnapshotPlugin mocks base method.
func (m *MockDriver) GetSnapshotPlugin() volume.Plugin {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSnapshotPlugin")
	ret0, _ := ret[0].(volume.Plugin)
	return ret0
}

// GetSnapshotPlugin indicates an expected call of GetSnapshotPlugin.
func (mr *MockDriverMockRecorder) GetSnapshotPlugin() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSnapshotPlugin", reflect.TypeOf((*MockDriver)(nil).GetSnapshotPlugin))
}

// GetSnapshotType mocks base method.
func (m *MockDriver) GetSnapshotType(arg0 *v1.VolumeSnapshot) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSnapshotType", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSnapshotType indicates an expected call of GetSnapshotType.
func (mr *MockDriverMockRecorder) GetSnapshotType(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSnapshotType", reflect.TypeOf((*MockDriver)(nil).GetSnapshotType), arg0)
}

// GetVolumeClaimTemplates mocks base method.
func (m *MockDriver) GetVolumeClaimTemplates(arg0 []v10.PersistentVolumeClaim) ([]v10.PersistentVolumeClaim, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumeClaimTemplates", arg0)
	ret0, _ := ret[0].([]v10.PersistentVolumeClaim)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVolumeClaimTemplates indicates an expected call of GetVolumeClaimTemplates.
func (mr *MockDriverMockRecorder) GetVolumeClaimTemplates(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumeClaimTemplates", reflect.TypeOf((*MockDriver)(nil).GetVolumeClaimTemplates), arg0)
}

// GetVolumeSnapshotRestoreStatus mocks base method.
func (m *MockDriver) GetVolumeSnapshotRestoreStatus(arg0 *v1alpha1.VolumeSnapshotRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVolumeSnapshotRestoreStatus", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// GetVolumeSnapshotRestoreStatus indicates an expected call of GetVolumeSnapshotRestoreStatus.
func (mr *MockDriverMockRecorder) GetVolumeSnapshotRestoreStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVolumeSnapshotRestoreStatus", reflect.TypeOf((*MockDriver)(nil).GetVolumeSnapshotRestoreStatus), arg0)
}

// Init mocks base method.
func (m *MockDriver) Init(arg0 interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockDriverMockRecorder) Init(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockDriver)(nil).Init), arg0)
}

// InspectNode mocks base method.
func (m *MockDriver) InspectNode(arg0 string) (*volume0.NodeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InspectNode", arg0)
	ret0, _ := ret[0].(*volume0.NodeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InspectNode indicates an expected call of InspectNode.
func (mr *MockDriverMockRecorder) InspectNode(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InspectNode", reflect.TypeOf((*MockDriver)(nil).InspectNode), arg0)
}

// InspectVolume mocks base method.
func (m *MockDriver) InspectVolume(arg0 string) (*volume0.Info, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InspectVolume", arg0)
	ret0, _ := ret[0].(*volume0.Info)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// InspectVolume indicates an expected call of InspectVolume.
func (mr *MockDriverMockRecorder) InspectVolume(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InspectVolume", reflect.TypeOf((*MockDriver)(nil).InspectVolume), arg0)
}

// IsVirtualMachineSupported mocks base method.
func (m *MockDriver) IsVirtualMachineSupported() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsVirtualMachineSupported")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsVirtualMachineSupported indicates an expected call of IsVirtualMachineSupported.
func (mr *MockDriverMockRecorder) IsVirtualMachineSupported() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsVirtualMachineSupported", reflect.TypeOf((*MockDriver)(nil).IsVirtualMachineSupported))
}

// OwnsPV mocks base method.
func (m *MockDriver) OwnsPV(arg0 *v10.PersistentVolume) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OwnsPV", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// OwnsPV indicates an expected call of OwnsPV.
func (mr *MockDriverMockRecorder) OwnsPV(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OwnsPV", reflect.TypeOf((*MockDriver)(nil).OwnsPV), arg0)
}

// OwnsPVC mocks base method.
func (m *MockDriver) OwnsPVC(arg0 core.Ops, arg1 *v10.PersistentVolumeClaim) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OwnsPVC", arg0, arg1)
	ret0, _ := ret[0].(bool)
	return ret0
}

// OwnsPVC indicates an expected call of OwnsPVC.
func (mr *MockDriverMockRecorder) OwnsPVC(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OwnsPVC", reflect.TypeOf((*MockDriver)(nil).OwnsPVC), arg0, arg1)
}

// OwnsPVCForBackup mocks base method.
func (m *MockDriver) OwnsPVCForBackup(arg0 core.Ops, arg1 *v10.PersistentVolumeClaim, arg2 bool, arg3 string) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OwnsPVCForBackup", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(bool)
	return ret0
}

// OwnsPVCForBackup indicates an expected call of OwnsPVCForBackup.
func (mr *MockDriverMockRecorder) OwnsPVCForBackup(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OwnsPVCForBackup", reflect.TypeOf((*MockDriver)(nil).OwnsPVCForBackup), arg0, arg1, arg2, arg3)
}

// StartBackup mocks base method.
func (m *MockDriver) StartBackup(arg0 *v1alpha1.ApplicationBackup, arg1 []v10.PersistentVolumeClaim) ([]*v1alpha1.ApplicationBackupVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartBackup", arg0, arg1)
	ret0, _ := ret[0].([]*v1alpha1.ApplicationBackupVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartBackup indicates an expected call of StartBackup.
func (mr *MockDriverMockRecorder) StartBackup(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartBackup", reflect.TypeOf((*MockDriver)(nil).StartBackup), arg0, arg1)
}

// StartMigration mocks base method.
func (m *MockDriver) StartMigration(arg0 *v1alpha1.Migration, arg1 []string) ([]*v1alpha1.MigrationVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartMigration", arg0, arg1)
	ret0, _ := ret[0].([]*v1alpha1.MigrationVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartMigration indicates an expected call of StartMigration.
func (mr *MockDriverMockRecorder) StartMigration(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartMigration", reflect.TypeOf((*MockDriver)(nil).StartMigration), arg0, arg1)
}

// StartRestore mocks base method.
func (m *MockDriver) StartRestore(arg0 *v1alpha1.ApplicationRestore, arg1 []*v1alpha1.ApplicationBackupVolumeInfo, arg2 []runtime.Unstructured) ([]*v1alpha1.ApplicationRestoreVolumeInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartRestore", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*v1alpha1.ApplicationRestoreVolumeInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartRestore indicates an expected call of StartRestore.
func (mr *MockDriverMockRecorder) StartRestore(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartRestore", reflect.TypeOf((*MockDriver)(nil).StartRestore), arg0, arg1, arg2)
}

// StartVolumeSnapshotRestore mocks base method.
func (m *MockDriver) StartVolumeSnapshotRestore(arg0 *v1alpha1.VolumeSnapshotRestore) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartVolumeSnapshotRestore", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// StartVolumeSnapshotRestore indicates an expected call of StartVolumeSnapshotRestore.
func (mr *MockDriverMockRecorder) StartVolumeSnapshotRestore(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartVolumeSnapshotRestore", reflect.TypeOf((*MockDriver)(nil).StartVolumeSnapshotRestore), arg0)
}

// Stop mocks base method.
func (m *MockDriver) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockDriverMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockDriver)(nil).Stop))
}

// String mocks base method.
func (m *MockDriver) String() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

// String indicates an expected call of String.
func (mr *MockDriverMockRecorder) String() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "String", reflect.TypeOf((*MockDriver)(nil).String))
}

// UpdateMigratedPersistentVolumeSpec mocks base method.
func (m *MockDriver) UpdateMigratedPersistentVolumeSpec(arg0 *v10.PersistentVolume, arg1 *v1alpha1.ApplicationRestoreVolumeInfo, arg2 map[string]string, arg3, arg4 string) (*v10.PersistentVolume, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateMigratedPersistentVolumeSpec", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(*v10.PersistentVolume)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateMigratedPersistentVolumeSpec indicates an expected call of UpdateMigratedPersistentVolumeSpec.
func (mr *MockDriverMockRecorder) UpdateMigratedPersistentVolumeSpec(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateMigratedPersistentVolumeSpec", reflect.TypeOf((*MockDriver)(nil).UpdateMigratedPersistentVolumeSpec), arg0, arg1, arg2, arg3, arg4)
}