// Copyright (C) LINBIT HA-Solutions GmbH
// All Rights Reserved.
// Author: Roland Kammerer <roland.kammerer@linbit.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package client

import (
	"context"
	"net/url"
	"strconv"
	"time"
)

// copy & paste from generated code

// ControllerVersion represents version information of the LINSTOR controller
type ControllerVersion struct {
	Version        string `json:"version,omitempty"`
	GitHash        string `json:"git_hash,omitempty"`
	BuildTime      string `json:"build_time,omitempty"`
	RestApiVersion string `json:"rest_api_version,omitempty"`
}

// ErrorReport struct for ErrorReport
type ErrorReport struct {
	NodeName  string `json:"node_name,omitempty"`
	ErrorTime int64  `json:"error_time"`
	// Filename of the error report on the server.  Format is: ```ErrorReport-{instanceid}-{nodeid}-{sequencenumber}.log```
	Filename string `json:"filename,omitempty"`
	// Contains the full text of the error report file.
	Text string `json:"text,omitempty"`
	// Which module this error occurred.
	Module string `json:"module,omitempty"`
	// Linstor version this error report was created.
	Version string `json:"version,omitempty"`
	// Peer client that was involved.
	Peer string `json:"peer,omitempty"`
	// Exception that occurred
	Exception string `json:"exception,omitempty"`
	// Exception message
	ExceptionMessage string `json:"exception_message,omitempty"`
	// Origin file of the exception
	OriginFile string `json:"origin_file,omitempty"`
	// Origin method where the exception occurred
	OriginMethod string `json:"origin_method,omitempty"`
	// Origin line number
	OriginLine int32 `json:"origin_line,omitempty"`
}

// custom code

// ControllerService is the service that deals with the LINSTOR controller.
type ControllerService struct {
	client *Client
}

// GetVersion queries version information for the controller.
func (s *ControllerService) GetVersion(ctx context.Context, opts ...*ListOpts) (ControllerVersion, error) {
	var vers ControllerVersion
	_, err := s.client.doGET(ctx, "/v1/controller/version", &vers, opts...)
	return vers, err
}

// GetConfig queries the configuration of a controller
func (s *ControllerService) GetConfig(ctx context.Context, opts ...*ListOpts) (ControllerConfig, error) {
	var cfg ControllerConfig
	_, err := s.client.doGET(ctx, "/v1/controller/config", &cfg, opts...)
	return cfg, err
}

// Modify modifies the controller node and sets/deletes the given properties.
func (s *ControllerService) Modify(ctx context.Context, props GenericPropsModify) error {
	_, err := s.client.doPOST(ctx, "/v1/controller/properties", props)
	return err
}

type ControllerProps map[string]string

// GetProps gets all properties of a controller
func (s *ControllerService) GetProps(ctx context.Context, opts ...*ListOpts) (ControllerProps, error) {
	var props ControllerProps
	_, err := s.client.doGET(ctx, "/v1/controller/properties", &props, opts...)
	return props, err
}

// DeleteProp deletes the given property/key from the controller object.
func (s *ControllerService) DeleteProp(ctx context.Context, prop string) error {
	_, err := s.client.doDELETE(ctx, "/v1/controller/properties/"+prop, nil)
	return err
}

// GetErrorReports returns all error reports. The Text field is not populated,
// use GetErrorReport to get the text of an error report.
func (s *ControllerService) GetErrorReports(ctx context.Context, opts ...*ListOpts) ([]ErrorReport, error) {
	var reports []ErrorReport
	_, err := s.client.doGET(ctx, "/v1/error-reports", &reports, opts...)
	return reports, err
}

// unixMilli returns t formatted as milliseconds since Unix epoch
func unixMilli(t time.Time) int64 {
	return t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

// GetErrorReportsSince returns all error reports created after a certain point in time.
func (s *ControllerService) GetErrorReportsSince(ctx context.Context, since time.Time, opts ...*ListOpts) ([]ErrorReport, error) {
	var reports []ErrorReport
	v := url.Values{}
	v.Set("since", strconv.FormatInt(unixMilli(since), 10))
	_, err := s.client.doGET(ctx, "/v1/error-reports/?"+v.Encode(), &reports, opts...)
	return reports, err
}

// GetErrorReport returns a specific error report, including its text.
func (s *ControllerService) GetErrorReport(ctx context.Context, id string, opts ...*ListOpts) (ErrorReport, error) {
	var report []ErrorReport
	_, err := s.client.doGET(ctx, "/v1/error-reports/"+id, &report, opts...)
	return report[0], err
}
