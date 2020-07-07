// A REST client to interact with LINSTOR's REST API
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
	"net/url"
	"reflect"

	"github.com/google/go-querystring/query"
)

// ListOpts is a struct primarily used to define parameters used for pagination. It is also used for filtering (e.g., the /view/ calls)
type ListOpts struct {
	Page    int `url:"offset"`
	PerPage int `url:"limit"`

	StoragePool []string `url:"storage_pools"`
	Resource    []string `url:"resources"`
	Node        []string `url:"nodes"`
}

func genOptions(opts ...*ListOpts) *ListOpts {
	if opts == nil || len(opts) == 0 {
		return nil
	}

	return opts[0]
}

func addOptions(s string, opt interface{}) (string, error) {
	v := reflect.ValueOf(opt)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return s, nil
	}

	u, err := url.Parse(s)
	if err != nil {
		return s, err
	}

	vs, err := query.Values(opt)
	if err != nil {
		return s, err
	}

	u.RawQuery = vs.Encode()
	return u.String(), nil
}
