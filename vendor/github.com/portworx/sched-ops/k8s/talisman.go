package k8s

import (
	talisman_v1beta2 "github.com/portworx/talisman/pkg/apis/portworx/v1beta2"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumePlacementStrategyOps is an interface to perform CRUD volume placememt strategy ops
type VolumePlacementStrategyOps interface {
	// CreateVolumePlacementStrategy creates a new volume placement strategy
	CreateVolumePlacementStrategy(spec *talisman_v1beta2.VolumePlacementStrategy) (*talisman_v1beta2.VolumePlacementStrategy, error)
	// UpdateVolumePlacementStrategy updates an existing volume placement strategy
	UpdateVolumePlacementStrategy(spec *talisman_v1beta2.VolumePlacementStrategy) (*talisman_v1beta2.VolumePlacementStrategy, error)
	// ListVolumePlacementStrategies lists all volume placement strategies
	ListVolumePlacementStrategies() (*talisman_v1beta2.VolumePlacementStrategyList, error)
	// DeleteVolumePlacementStrategy deletes the volume placement strategy with given name
	DeleteVolumePlacementStrategy(name string) error
	// GetVolumePlacementStrategy returns the volume placememt strategy with given name
	GetVolumePlacementStrategy(name string) (*talisman_v1beta2.VolumePlacementStrategy, error)
}

// VolumePlacementStrategy APIs - BEGIN

func (k *k8sOps) CreateVolumePlacementStrategy(spec *talisman_v1beta2.VolumePlacementStrategy) (*talisman_v1beta2.VolumePlacementStrategy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.talismanClient.Portworx().VolumePlacementStrategies().Create(spec)
}

func (k *k8sOps) UpdateVolumePlacementStrategy(spec *talisman_v1beta2.VolumePlacementStrategy) (*talisman_v1beta2.VolumePlacementStrategy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.talismanClient.Portworx().VolumePlacementStrategies().Update(spec)
}

func (k *k8sOps) ListVolumePlacementStrategies() (*talisman_v1beta2.VolumePlacementStrategyList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.talismanClient.Portworx().VolumePlacementStrategies().List(meta_v1.ListOptions{})
}

func (k *k8sOps) DeleteVolumePlacementStrategy(name string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.talismanClient.Portworx().VolumePlacementStrategies().Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) GetVolumePlacementStrategy(name string) (*talisman_v1beta2.VolumePlacementStrategy, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.talismanClient.Portworx().VolumePlacementStrategies().Get(name, meta_v1.GetOptions{})
}

// VolumePlacementStrategy APIs - END
