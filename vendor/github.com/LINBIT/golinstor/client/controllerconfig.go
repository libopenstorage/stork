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
	Config ControllerConfigConfig `json:"config,omitempty"`
	Debug  ControllerConfigDebug  `json:"debug,omitempty"`
	Log    ControllerConfigLog    `json:"log,omitempty"`
	Db     ControllerConfigDb     `json:"db,omitempty"`
	Http   ControllerConfigHttp   `json:"http,omitempty"`
	Https  ControllerConfigHttps  `json:"https,omitempty"`
	Ldap   ControllerConfigLdap   `json:"ldap,omitempty"`
}

type ControllerConfigConfig struct {
	Dir string `json:"dir,omitempty"`
}

type ControllerConfigDbEtcd struct {
	OperationsPerTransaction int32  `json:"operations_per_transaction,omitempty"`
	Prefix                   string `json:"prefix,omitempty"`
}

type ControllerConfigDb struct {
	ConnectionUrl        string                 `json:"connection_url,omitempty"`
	CaCertificate        string                 `json:"ca_certificate,omitempty"`
	ClientCertificate    string                 `json:"client_certificate,omitempty"`
	ClientKeyPkcs8Pem    string                 `json:"client_key_pkcs8_pem,omitempty"`
	InMemory             string                 `json:"in_memory,omitempty"`
	VersionCheckDisabled bool                   `json:"version_check_disabled,omitempty"`
	Etcd                 ControllerConfigDbEtcd `json:"etcd,omitempty"`
}

type ControllerConfigDebug struct {
	ConsoleEnabled bool `json:"console_enabled,omitempty"`
}

type ControllerConfigHttp struct {
	Enabled       bool   `json:"enabled,omitempty"`
	ListenAddress string `json:"listen_address,omitempty"`
	Port          int32  `json:"port,omitempty"`
}

type ControllerConfigHttps struct {
	Enabled            bool   `json:"enabled,omitempty"`
	ListenAddress      string `json:"listen_address,omitempty"`
	Port               int32  `json:"port,omitempty"`
	Keystore           string `json:"keystore,omitempty"`
	KeystorePassword   string `json:"keystore_password,omitempty"`
	Truststore         string `json:"truststore,omitempty"`
	TruststorePassword string `json:"truststore_password,omitempty"`
}

type ControllerConfigLdap struct {
	Enabled             bool   `json:"enabled,omitempty"`
	PublicAccessAllowed bool   `json:"public_access_allowed,omitempty"`
	Uri                 string `json:"uri,omitempty"`
	Dn                  string `json:"dn,omitempty"`
	SearchBase          string `json:"search_base,omitempty"`
	SearchFilter        string `json:"search_filter,omitempty"`
}

type ControllerConfigLog struct {
	PrintStackTrace    bool     `json:"print_stack_trace,omitempty"`
	Directory          string   `json:"directory,omitempty"`
	Level              LogLevel `json:"level,omitempty"`
	LevelGlobal        LogLevel `json:"level_global,omitempty"`
	LevelLinstor       LogLevel `json:"level_linstor,omitempty"`
	LevelLinstorGlobal LogLevel `json:"level_linstor_global,omitempty"`
	RestAccessLogPath  string   `json:"rest_access_log_path,omitempty"`
	RestAccessMode     string   `json:"rest_access_mode,omitempty"`
}

// SatelliteConfig struct for SatelliteConfig
type SatelliteConfig struct {
	Config               ControllerConfigConfig `json:"config,omitempty"`
	Debug                ControllerConfigDebug  `json:"debug,omitempty"`
	Log                  SatelliteConfigLog     `json:"log,omitempty"`
	StltOverrideNodeName string                 `json:"stlt_override_node_name,omitempty"`
	Openflex             bool                   `json:"openflex,omitempty"`
	RemoteSpdk           bool                   `json:"remote_spdk,omitempty"`
	SpecialSatellite     bool                   `json:"special_satellite,omitempty"`
	DrbdKeepResPattern   string                 `json:"drbd_keep_res_pattern,omitempty"`
	Net                  SatelliteConfigNet     `json:"net,omitempty"`
}

// SatelliteConfigLog struct for SatelliteConfigLog
type SatelliteConfigLog struct {
	PrintStackTrace bool     `json:"print_stack_trace,omitempty"`
	Directory       string   `json:"directory,omitempty"`
	Level           LogLevel `json:"level,omitempty"`
	LevelLinstor    LogLevel `json:"level_linstor,omitempty"`
}

// SatelliteConfigNet struct for SatelliteConfigNet
type SatelliteConfigNet struct {
	BindAddress string `json:"bind_address,omitempty"`
	Port        int32  `json:"port,omitempty"`
	ComType     string `json:"com_type,omitempty"`
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
