package client

import (
	"context"
	"fmt"
)

// copy & paste from generated code

// ExosConnectionMap struct for ExosConnectionMap
type ExosConnectionMap struct {
	NodeName      string   `json:"node_name,omitempty"`
	EnclosureName string   `json:"enclosure_name,omitempty"`
	Connections   []string `json:"connections,omitempty"`
}

// ExosDefaults Default settings for EXOS enclosures
type ExosDefaults struct {
	Username    string `json:"username,omitempty"`
	UsernameEnv string `json:"username_env,omitempty"`
	Password    string `json:"password,omitempty"`
	PasswordEnv string `json:"password_env,omitempty"`
}

// ExosDefaultsModifyAllOf struct for ExosDefaultsModifyAllOf
type ExosDefaultsModifyAllOf struct {
	// A list of keys to unset. The keys have to exist in ExosDefaults
	UnsetKeys []string `json:"unset_keys,omitempty"`
}

// ExosDefaultsModify struct for ExosDefaultsModify
type ExosDefaultsModify struct {
	Username    string `json:"username,omitempty"`
	UsernameEnv string `json:"username_env,omitempty"`
	Password    string `json:"password,omitempty"`
	PasswordEnv string `json:"password_env,omitempty"`
	// A list of keys to unset. The keys have to exist in ExosDefaults
	UnsetKeys []string `json:"unset_keys,omitempty"`
}

// ExosEnclosureEvent EXOS event
type ExosEnclosureEvent struct {
	Severity              string `json:"severity,omitempty"`
	EventId               string `json:"event_id,omitempty"`
	Controller            string `json:"controller,omitempty"`
	TimeStamp             string `json:"time_stamp,omitempty"`
	TimeStampNumeric      int64  `json:"time_stamp_numeric,omitempty"`
	Message               string `json:"message,omitempty"`
	AdditionalInformation string `json:"additional_information,omitempty"`
	RecommendedAction     string `json:"recommended_action,omitempty"`
}

// ExosEnclosure EXOS enclosure
type ExosEnclosure struct {
	Name        string `json:"name,omitempty"`
	CtrlAIp     string `json:"ctrl_a_ip,omitempty"`
	CtrlBIp     string `json:"ctrl_b_ip,omitempty"`
	Username    string `json:"username,omitempty"`
	UsernameEnv string `json:"username_env,omitempty"`
	Password    string `json:"password,omitempty"`
	PasswordEnv string `json:"password_env,omitempty"`
}

// ExosEnclosureHealth EXOS enclosure name, controller IPs and health status
type ExosEnclosureHealth struct {
	Name         string `json:"name,omitempty"`
	CtrlAIp      string `json:"ctrl_a_ip,omitempty"`
	CtrlBIp      string `json:"ctrl_b_ip,omitempty"`
	Health       string `json:"health,omitempty"`
	HealthReason string `json:"health_reason,omitempty"`
}

// custom code

type VendorProvider interface {
	// GetExosDefaults lists default settings for all EXOS enclosures
	GetExosDefaults(ctx context.Context) (ExosDefaults, error)
	// ModifyExosDefaults sets or modifies default username / password for EXOS enclosures
	ModifyExosDefaults(ctx context.Context, defaults ExosDefaultsModify) error
	// GetExosEnclosures lists EXOS enclosures including controller IP and health status
	GetExosEnclosures(ctx context.Context, noCache bool) ([]ExosEnclosure, error)
	// CreateExosEnclosure creates a new enclosure unless it already exists
	CreateExosEnclosure(ctx context.Context, enclosure ExosEnclosure) error
	// ModifyExosEnclosure modifies an existing enclosure
	ModifyExosEnclosure(ctx context.Context, name string, enclosure ExosEnclosure) error
	// DeleteExosEnclosure deletes an existing enclosure
	DeleteExosEnclosure(ctx context.Context, name string) error
	// GetExosEvents lists the most current "count" events
	GetExosEvents(ctx context.Context, name string, count int32) ([]ExosEnclosureEvent, error)
	// GetExosConnectionMap lists the connection-mesh of EXOS Ports to LINSTOR Nodes
	GetExosConnectionMap(ctx context.Context) (ExosConnectionMap, error)
}

type VendorService struct {
	client *Client
}

// GetExosDefaults lists default settings for all EXOS enclosures
func (s *VendorService) GetExosDefaults(ctx context.Context) (ExosDefaults, error) {
	var defaults ExosDefaults
	_, err := s.client.doGET(ctx, "/v1/vendor/seagate/exos/defaults", &defaults)
	return defaults, err
}

// ModifyExosDefaults sets or modifies default username / password for EXOS enclosures
func (s *VendorService) ModifyExosDefaults(ctx context.Context, defaults ExosDefaultsModify) error {
	_, err := s.client.doPUT(ctx, "/v1/vendor/seagate/exos/defaults", defaults)
	return err
}

// GetExosEnclosures lists EXOS enclosures including controller IP and health status
func (s *VendorService) GetExosEnclosures(ctx context.Context, noCache bool) ([]ExosEnclosure, error) {
	var enclosures []ExosEnclosure
	_, err := s.client.doGET(ctx, fmt.Sprintf("/v1/vendor/seagate/exos/enclosures?nocache=%t", noCache), &enclosures)
	return enclosures, err
}

// CreateExosEnclosure creates a new enclosure unless it already exists
func (s *VendorService) CreateExosEnclosure(ctx context.Context, enclosure ExosEnclosure) error {
	_, err := s.client.doPOST(ctx, "/v1/vendor/seagate/exos/enclosures", enclosure)
	return err
}

// ModifyExosEnclosure modifies an existing enclosure
func (s *VendorService) ModifyExosEnclosure(ctx context.Context, name string, enclosure ExosEnclosure) error {
	_, err := s.client.doPUT(ctx, "/v1/vendor/seagate/exos/enclosures/"+name, enclosure)
	return err
}

// DeleteExosEnclosure deletes an existing enclosure
func (s *VendorService) DeleteExosEnclosure(ctx context.Context, name string) error {
	_, err := s.client.doDELETE(ctx, "/v1/vendor/seagate/exos/enclosures/"+name, nil)
	return err
}

// GetExosEvents lists the most current "count" events
func (s *VendorService) GetExosEvents(ctx context.Context, name string, count int32) ([]ExosEnclosureEvent, error) {
	var events []ExosEnclosureEvent
	_, err := s.client.doGET(ctx, "/v1/vendor/seagate/exos/enclosures/"+name+"/events", &events)
	return events, err
}

// GetExosConnectionMap lists the connection-mesh of EXOS Ports to LINSTOR Nodes
func (s *VendorService) GetExosConnectionMap(ctx context.Context) (ExosConnectionMap, error) {
	var connMap ExosConnectionMap
	_, err := s.client.doGET(ctx, "/v1/vendor/seagate/exos/map", &connMap)
	return connMap, err
}
