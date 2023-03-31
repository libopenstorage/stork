package talisman

import (
	talismanv1beta2 "github.com/portworx/talisman/pkg/apis/portworx/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumePlacementStrategyOps is an interface to perform CRUD volume placememt strategy ops
type VolumePlacementStrategyOps interface {
	// CreateVolumePlacementStrategy creates a new volume placement strategy
	CreateVolumePlacementStrategy(spec *talismanv1beta2.VolumePlacementStrategy) (*talismanv1beta2.VolumePlacementStrategy, error)
	// UpdateVolumePlacementStrategy updates an existing volume placement strategy
	UpdateVolumePlacementStrategy(spec *talismanv1beta2.VolumePlacementStrategy) (*talismanv1beta2.VolumePlacementStrategy, error)
	// ListVolumePlacementStrategies lists all volume placement strategies
	ListVolumePlacementStrategies() (*talismanv1beta2.VolumePlacementStrategyList, error)
	// DeleteVolumePlacementStrategy deletes the volume placement strategy with given name
	DeleteVolumePlacementStrategy(name string) error
	// GetVolumePlacementStrategy returns the volume placememt strategy with given name
	GetVolumePlacementStrategy(name string) (*talismanv1beta2.VolumePlacementStrategy, error)
}

// CreateVolumePlacementStrategy creates a new volume placement strategy
func (c *Client) CreateVolumePlacementStrategy(spec *talismanv1beta2.VolumePlacementStrategy) (*talismanv1beta2.VolumePlacementStrategy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.talisman.Portworx().VolumePlacementStrategies().Create(spec)
}

// UpdateVolumePlacementStrategy updates an existing volume placement strategy
func (c *Client) UpdateVolumePlacementStrategy(spec *talismanv1beta2.VolumePlacementStrategy) (*talismanv1beta2.VolumePlacementStrategy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.talisman.Portworx().VolumePlacementStrategies().Update(spec)
}

// ListVolumePlacementStrategies lists all volume placement strategies
func (c *Client) ListVolumePlacementStrategies() (*talismanv1beta2.VolumePlacementStrategyList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.talisman.Portworx().VolumePlacementStrategies().List(metav1.ListOptions{})
}

// DeleteVolumePlacementStrategy deletes the volume placement strategy with given name
func (c *Client) DeleteVolumePlacementStrategy(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.talisman.Portworx().VolumePlacementStrategies().Delete(name, &metav1.DeleteOptions{

		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// GetVolumePlacementStrategy returns the volume placememt strategy with given name
func (c *Client) GetVolumePlacementStrategy(name string) (*talismanv1beta2.VolumePlacementStrategy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.talisman.Portworx().VolumePlacementStrategies().Get(name, metav1.GetOptions{})
}
