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

type ControllerConfig struct {
	Config ControllerConfigConfig `json:"config,omitempty" toml:"config,omitempty,omitzero"`
	Debug  ControllerConfigDebug  `json:"debug,omitempty" toml:"debug,omitempty,omitzero"`
	Log    ControllerConfigLog    `json:"log,omitempty" toml:"log,omitempty,omitzero"`
	Db     ControllerConfigDb     `json:"db,omitempty" toml:"db,omitempty,omitzero"`
	Http   ControllerConfigHttp   `json:"http,omitempty" toml:"http,omitempty,omitzero"`
	Https  ControllerConfigHttps  `json:"https,omitempty" toml:"https,omitempty,omitzero"`
	Ldap   ControllerConfigLdap   `json:"ldap,omitempty" toml:"ldap,omitempty,omitzero"`
}

type ControllerConfigConfig struct {
	Dir string `json:"dir,omitempty" toml:"dir,omitempty,omitzero"`
}

type ControllerConfigDbEtcd struct {
	OperationsPerTransaction int32 `json:"operations_per_transaction,omitempty" toml:"operations_per_transaction,omitempty,omitzero"`
}

type ControllerConfigDb struct {
	ConnectionUrl        string                 `json:"connection_url,omitempty" toml:"connection_url,omitempty,omitzero"`
	CaCertificate        string                 `json:"ca_certificate,omitempty" toml:"ca_certificate,omitempty,omitzero"`
	ClientCertificate    string                 `json:"client_certificate,omitempty" toml:"client_certificate,omitempty,omitzero"`
	ClientKeyPkcs8Pem    string                 `json:"client_key_pkcs8_pem,omitempty" toml:"client_key_pkcs8_pem,omitempty,omitzero"`
	InMemory             string                 `json:"in_memory,omitempty" toml:"in_memory,omitempty,omitzero"`
	VersionCheckDisabled bool                   `json:"version_check_disabled,omitempty" toml:"version_check_disabled,omitempty,omitzero"`
	Etcd                 ControllerConfigDbEtcd `json:"etcd,omitempty" toml:"etcd,omitempty,omitzero"`
}

type ControllerConfigDebug struct {
	ConsoleEnabled bool `json:"console_enabled,omitempty" toml:"console_enabled,omitempty,omitzero"`
}

type ControllerConfigHttp struct {
	Enabled       bool   `json:"enabled,omitempty" toml:"enabled,omitempty,omitzero"`
	ListenAddress string `json:"listen_address,omitempty" toml:"listen_address,omitempty,omitzero"`
	Port          int32  `json:"port,omitempty" toml:"port,omitempty,omitzero"`
}

type ControllerConfigHttps struct {
	Enabled            bool   `json:"enabled,omitempty" toml:"enabled,omitempty,omitzero"`
	ListenAddress      string `json:"listen_address,omitempty" toml:"listen_address,omitempty,omitzero"`
	Port               int32  `json:"port,omitempty" toml:"port,omitempty,omitzero"`
	Keystore           string `json:"keystore,omitempty" toml:"keystore,omitempty,omitzero"`
	KeystorePassword   string `json:"keystore_password,omitempty" toml:"keystore_password,omitempty,omitzero"`
	Truststore         string `json:"truststore,omitempty" toml:"truststore,omitempty,omitzero"`
	TruststorePassword string `json:"truststore_password,omitempty" toml:"truststore_password,omitempty,omitzero"`
}

type ControllerConfigLdap struct {
	Enabled             bool   `json:"enabled,omitempty" toml:"enabled,omitempty,omitzero"`
	PublicAccessAllowed bool   `json:"public_access_allowed,omitempty" toml:"public_access_allowed,omitempty,omitzero"`
	Uri                 string `json:"uri,omitempty" toml:"uri,omitempty,omitzero"`
	Dn                  string `json:"dn,omitempty" toml:"dn,omitempty,omitzero"`
	SearchBase          string `json:"search_base,omitempty" toml:"search_base,omitempty,omitzero"`
	SearchFilter        string `json:"search_filter,omitempty" toml:"search_filter,omitempty,omitzero"`
}

type ControllerConfigLog struct {
	PrintStackTrace   bool     `json:"print_stack_trace,omitempty" toml:"print_stack_trace,omitempty,omitzero"`
	Directory         string   `json:"directory,omitempty" toml:"directory,omitempty,omitzero"`
	Level             LogLevel `json:"level,omitempty" toml:"level,omitempty,omitzero"`
	LevelLinstor      LogLevel `json:"level_linstor,omitempty" toml:"level_linstor,omitempty,omitzero"`
	RestAccessLogPath string   `json:"rest_access_log_path,omitempty" toml:"rest_access_log_path,omitempty,omitzero"`
	RestAccessMode    string   `json:"rest_access_mode,omitempty" toml:"rest_access_mode,omitempty,omitzero"`
}

type LogLevel string

// List of LogLevel
const (
	ERROR LogLevel = "ERROR"
	WARN  LogLevel = "WARN"
	INFO  LogLevel = "INFO"
	DEBUG LogLevel = "DEBUG"
	TRACE LogLevel = "TRACE"
)
