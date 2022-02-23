package client

import "context"

// StoragePoolDefinition represents a storage pool definition in LINSTOR
type StoragePoolDefinition struct {
	StoragePoolName string `json:"storage_pool_name,omitempty"`
	// A string to string property map.
	Props map[string]string `json:"props,omitempty"`
}

// StoragePoolDefinitionModify holds properties of a storage pool definition to modify such a definition.
type StoragePoolDefinitionModify struct {
	GenericPropsModify
}

// custom code

// StoragePoolDefinitionProvider acts as an abstraction for a
// StoragePoolDefinitionService. It can be swapped out for another
// StoragePoolDefinitionService implementation, for example for testing.
type StoragePoolDefinitionProvider interface {
	// GetAll gets information for all existing storage pool definitions.
	GetAll(ctx context.Context, opts ...*ListOpts) ([]StoragePoolDefinition, error)
	// Get gets information for a particular storage pool definition.
	Get(ctx context.Context, spdName string, opts ...*ListOpts) (StoragePoolDefinition, error)
	// Create creates a new storage pool definition
	Create(ctx context.Context, spd StoragePoolDefinition) error
	// Modify modifies the given storage pool definition and sets/deletes the given properties.
	Modify(ctx context.Context, spdName string, props StoragePoolDefinitionModify) error
	// Delete deletes the given storage pool definition.
	Delete(ctx context.Context, spdName string) error
	// GetPropsInfos gets meta information about the properties that can be
	// set on a storage pool definition.
	GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error)
}

var _ StoragePoolDefinitionProvider = &StoragePoolDefinitionService{}

// StoragePoolDefinitionService is the service that deals with storage pool definition related tasks.
type StoragePoolDefinitionService struct {
	client *Client
}

// GetAll gets information for all existing storage pool definitions.
func (s *StoragePoolDefinitionService) GetAll(ctx context.Context, opts ...*ListOpts) ([]StoragePoolDefinition, error) {
	var spds []StoragePoolDefinition
	_, err := s.client.doGET(ctx, "/v1/storage-pool-definitions", &spds, opts...)
	return spds, err
}

// Get gets information for a particular storage pool definition.
func (s *StoragePoolDefinitionService) Get(ctx context.Context, spdName string, opts ...*ListOpts) (StoragePoolDefinition, error) {
	var spd StoragePoolDefinition
	_, err := s.client.doGET(ctx, "/v1/storage-pool-definitions/"+spdName, &spd, opts...)
	return spd, err
}

// Create creates a new storage pool definition
func (s *StoragePoolDefinitionService) Create(ctx context.Context, spd StoragePoolDefinition) error {
	_, err := s.client.doPOST(ctx, "/v1/storage-pool-definitions", spd)
	return err
}

// Modify modifies the given storage pool definition and sets/deletes the given properties.
func (s *StoragePoolDefinitionService) Modify(ctx context.Context, spdName string, props StoragePoolDefinitionModify) error {
	_, err := s.client.doPUT(ctx, "/v1/storage-pool-definitions/"+spdName, props)
	return err
}

// Delete deletes the given storage pool definition.
func (s *StoragePoolDefinitionService) Delete(ctx context.Context, spdName string) error {
	_, err := s.client.doDELETE(ctx, "/v1/storage-pool-definitions/"+spdName, nil)
	return err
}

// GetPropsInfos gets meta information about the properties that can be set on
// a storage pool definition.
func (s *StoragePoolDefinitionService) GetPropsInfos(ctx context.Context, opts ...*ListOpts) ([]PropsInfo, error) {
	var infos []PropsInfo
	_, err := s.client.doGET(ctx, "/v1/storage-pool-definitions/properties/info", &infos, opts...)
	return infos, err
}
