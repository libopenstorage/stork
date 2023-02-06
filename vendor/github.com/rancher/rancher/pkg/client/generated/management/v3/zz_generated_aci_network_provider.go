package client

const (
	AciNetworkProviderType                                   = "aciNetworkProvider"
	AciNetworkProviderFieldAEP                               = "aep"
	AciNetworkProviderFieldAddExternalSubnetsToRdconfig      = "addExternalSubnetsToRdconfig"
	AciNetworkProviderFieldApicHosts                         = "apicHosts"
	AciNetworkProviderFieldApicRefreshTickerAdjust           = "apicRefreshTickerAdjust"
	AciNetworkProviderFieldApicRefreshTime                   = "apicRefreshTime"
	AciNetworkProviderFieldApicSubscriptionDelay             = "apicSubscriptionDelay"
	AciNetworkProviderFieldApicUserCrt                       = "apicUserCrt"
	AciNetworkProviderFieldApicUserKey                       = "apicUserKey"
	AciNetworkProviderFieldApicUserName                      = "apicUserName"
	AciNetworkProviderFieldCApic                             = "capic"
	AciNetworkProviderFieldControllerLogLevel                = "controllerLogLevel"
	AciNetworkProviderFieldDisablePeriodicSnatGlobalInfoSync = "disablePeriodicSnatGlobalInfoSync"
	AciNetworkProviderFieldDisableWaitForNetwork             = "disableWaitForNetwork"
	AciNetworkProviderFieldDropLogEnable                     = "dropLogEnable"
	AciNetworkProviderFieldDurationWaitForNetwork            = "durationWaitForNetwork"
	AciNetworkProviderFieldDynamicExternalSubnet             = "externDynamic"
	AciNetworkProviderFieldEnableEndpointSlice               = "enableEndpointSlice"
	AciNetworkProviderFieldEncapType                         = "encapType"
	AciNetworkProviderFieldEpRegistry                        = "epRegistry"
	AciNetworkProviderFieldGbpPodSubnet                      = "gbpPodSubnet"
	AciNetworkProviderFieldHostAgentLogLevel                 = "hostAgentLogLevel"
	AciNetworkProviderFieldImagePullPolicy                   = "imagePullPolicy"
	AciNetworkProviderFieldImagePullSecret                   = "imagePullSecret"
	AciNetworkProviderFieldInfraVlan                         = "infraVlan"
	AciNetworkProviderFieldInstallIstio                      = "installIstio"
	AciNetworkProviderFieldIstioProfile                      = "istioProfile"
	AciNetworkProviderFieldKafkaBrokers                      = "kafkaBrokers"
	AciNetworkProviderFieldKafkaClientCrt                    = "kafkaClientCrt"
	AciNetworkProviderFieldKafkaClientKey                    = "kafkaClientKey"
	AciNetworkProviderFieldKubeAPIVlan                       = "kubeApiVlan"
	AciNetworkProviderFieldL3Out                             = "l3out"
	AciNetworkProviderFieldL3OutExternalNetworks             = "l3outExternalNetworks"
	AciNetworkProviderFieldMTUHeadRoom                       = "mtuHeadRoom"
	AciNetworkProviderFieldMaxNodesSvcGraph                  = "maxNodesSvcGraph"
	AciNetworkProviderFieldMcastRangeEnd                     = "mcastRangeEnd"
	AciNetworkProviderFieldMcastRangeStart                   = "mcastRangeStart"
	AciNetworkProviderFieldMultusDisable                     = "multusDisable"
	AciNetworkProviderFieldNoPriorityClass                   = "noPriorityClass"
	AciNetworkProviderFieldNoWaitForServiceEpReadiness       = "noWaitForServiceEpReadiness"
	AciNetworkProviderFieldNodePodIfEnable                   = "nodePodIfEnable"
	AciNetworkProviderFieldNodeSubnet                        = "nodeSubnet"
	AciNetworkProviderFieldOVSMemoryLimit                    = "ovsMemoryLimit"
	AciNetworkProviderFieldOpflexAgentLogLevel               = "opflexLogLevel"
	AciNetworkProviderFieldOpflexClientSSL                   = "opflexClientSsl"
	AciNetworkProviderFieldOpflexDeviceDeleteTimeout         = "opflexDeviceDeleteTimeout"
	AciNetworkProviderFieldOpflexMode                        = "opflexMode"
	AciNetworkProviderFieldOpflexServerPort                  = "opflexServerPort"
	AciNetworkProviderFieldOverlayVRFName                    = "overlayVrfName"
	AciNetworkProviderFieldPBRTrackingNonSnat                = "pbrTrackingNonSnat"
	AciNetworkProviderFieldPodSubnetChunkSize                = "podSubnetChunkSize"
	AciNetworkProviderFieldRunGbpContainer                   = "runGbpContainer"
	AciNetworkProviderFieldRunOpflexServerContainer          = "runOpflexServerContainer"
	AciNetworkProviderFieldServiceGraphEndpointAddDelay      = "serviceGraphEndpointAddDelay"
	AciNetworkProviderFieldServiceGraphEndpointAddServices   = "serviceGraphEndpointAddServices"
	AciNetworkProviderFieldServiceGraphSubnet                = "nodeSvcSubnet"
	AciNetworkProviderFieldServiceMonitorInterval            = "serviceMonitorInterval"
	AciNetworkProviderFieldServiceVlan                       = "serviceVlan"
	AciNetworkProviderFieldSnatContractScope                 = "snatContractScope"
	AciNetworkProviderFieldSnatNamespace                     = "snatNamespace"
	AciNetworkProviderFieldSnatPortRangeEnd                  = "snatPortRangeEnd"
	AciNetworkProviderFieldSnatPortRangeStart                = "snatPortRangeStart"
	AciNetworkProviderFieldSnatPortsPerNode                  = "snatPortsPerNode"
	AciNetworkProviderFieldSriovEnable                       = "sriovEnable"
	AciNetworkProviderFieldStaticExternalSubnet              = "externStatic"
	AciNetworkProviderFieldSubnetDomainName                  = "subnetDomainName"
	AciNetworkProviderFieldSystemIdentifier                  = "systemId"
	AciNetworkProviderFieldTenant                            = "tenant"
	AciNetworkProviderFieldToken                             = "token"
	AciNetworkProviderFieldUseAciAnywhereCRD                 = "useAciAnywhereCrd"
	AciNetworkProviderFieldUseAciCniPriorityClass            = "useAciCniPriorityClass"
	AciNetworkProviderFieldUseClusterRole                    = "useClusterRole"
	AciNetworkProviderFieldUseHostNetnsVolume                = "useHostNetnsVolume"
	AciNetworkProviderFieldUseOpflexServerVolume             = "useOpflexServerVolume"
	AciNetworkProviderFieldUsePrivilegedContainer            = "usePrivilegedContainer"
	AciNetworkProviderFieldVRFName                           = "vrfName"
	AciNetworkProviderFieldVRFTenant                         = "vrfTenant"
	AciNetworkProviderFieldVmmController                     = "vmmController"
	AciNetworkProviderFieldVmmDomain                         = "vmmDomain"
)

type AciNetworkProvider struct {
	AEP                               string              `json:"aep,omitempty" yaml:"aep,omitempty"`
	AddExternalSubnetsToRdconfig      string              `json:"addExternalSubnetsToRdconfig,omitempty" yaml:"addExternalSubnetsToRdconfig,omitempty"`
	ApicHosts                         []string            `json:"apicHosts,omitempty" yaml:"apicHosts,omitempty"`
	ApicRefreshTickerAdjust           string              `json:"apicRefreshTickerAdjust,omitempty" yaml:"apicRefreshTickerAdjust,omitempty"`
	ApicRefreshTime                   string              `json:"apicRefreshTime,omitempty" yaml:"apicRefreshTime,omitempty"`
	ApicSubscriptionDelay             string              `json:"apicSubscriptionDelay,omitempty" yaml:"apicSubscriptionDelay,omitempty"`
	ApicUserCrt                       string              `json:"apicUserCrt,omitempty" yaml:"apicUserCrt,omitempty"`
	ApicUserKey                       string              `json:"apicUserKey,omitempty" yaml:"apicUserKey,omitempty"`
	ApicUserName                      string              `json:"apicUserName,omitempty" yaml:"apicUserName,omitempty"`
	CApic                             string              `json:"capic,omitempty" yaml:"capic,omitempty"`
	ControllerLogLevel                string              `json:"controllerLogLevel,omitempty" yaml:"controllerLogLevel,omitempty"`
	DisablePeriodicSnatGlobalInfoSync string              `json:"disablePeriodicSnatGlobalInfoSync,omitempty" yaml:"disablePeriodicSnatGlobalInfoSync,omitempty"`
	DisableWaitForNetwork             string              `json:"disableWaitForNetwork,omitempty" yaml:"disableWaitForNetwork,omitempty"`
	DropLogEnable                     string              `json:"dropLogEnable,omitempty" yaml:"dropLogEnable,omitempty"`
	DurationWaitForNetwork            string              `json:"durationWaitForNetwork,omitempty" yaml:"durationWaitForNetwork,omitempty"`
	DynamicExternalSubnet             string              `json:"externDynamic,omitempty" yaml:"externDynamic,omitempty"`
	EnableEndpointSlice               string              `json:"enableEndpointSlice,omitempty" yaml:"enableEndpointSlice,omitempty"`
	EncapType                         string              `json:"encapType,omitempty" yaml:"encapType,omitempty"`
	EpRegistry                        string              `json:"epRegistry,omitempty" yaml:"epRegistry,omitempty"`
	GbpPodSubnet                      string              `json:"gbpPodSubnet,omitempty" yaml:"gbpPodSubnet,omitempty"`
	HostAgentLogLevel                 string              `json:"hostAgentLogLevel,omitempty" yaml:"hostAgentLogLevel,omitempty"`
	ImagePullPolicy                   string              `json:"imagePullPolicy,omitempty" yaml:"imagePullPolicy,omitempty"`
	ImagePullSecret                   string              `json:"imagePullSecret,omitempty" yaml:"imagePullSecret,omitempty"`
	InfraVlan                         string              `json:"infraVlan,omitempty" yaml:"infraVlan,omitempty"`
	InstallIstio                      string              `json:"installIstio,omitempty" yaml:"installIstio,omitempty"`
	IstioProfile                      string              `json:"istioProfile,omitempty" yaml:"istioProfile,omitempty"`
	KafkaBrokers                      []string            `json:"kafkaBrokers,omitempty" yaml:"kafkaBrokers,omitempty"`
	KafkaClientCrt                    string              `json:"kafkaClientCrt,omitempty" yaml:"kafkaClientCrt,omitempty"`
	KafkaClientKey                    string              `json:"kafkaClientKey,omitempty" yaml:"kafkaClientKey,omitempty"`
	KubeAPIVlan                       string              `json:"kubeApiVlan,omitempty" yaml:"kubeApiVlan,omitempty"`
	L3Out                             string              `json:"l3out,omitempty" yaml:"l3out,omitempty"`
	L3OutExternalNetworks             []string            `json:"l3outExternalNetworks,omitempty" yaml:"l3outExternalNetworks,omitempty"`
	MTUHeadRoom                       string              `json:"mtuHeadRoom,omitempty" yaml:"mtuHeadRoom,omitempty"`
	MaxNodesSvcGraph                  string              `json:"maxNodesSvcGraph,omitempty" yaml:"maxNodesSvcGraph,omitempty"`
	McastRangeEnd                     string              `json:"mcastRangeEnd,omitempty" yaml:"mcastRangeEnd,omitempty"`
	McastRangeStart                   string              `json:"mcastRangeStart,omitempty" yaml:"mcastRangeStart,omitempty"`
	MultusDisable                     string              `json:"multusDisable,omitempty" yaml:"multusDisable,omitempty"`
	NoPriorityClass                   string              `json:"noPriorityClass,omitempty" yaml:"noPriorityClass,omitempty"`
	NoWaitForServiceEpReadiness       string              `json:"noWaitForServiceEpReadiness,omitempty" yaml:"noWaitForServiceEpReadiness,omitempty"`
	NodePodIfEnable                   string              `json:"nodePodIfEnable,omitempty" yaml:"nodePodIfEnable,omitempty"`
	NodeSubnet                        string              `json:"nodeSubnet,omitempty" yaml:"nodeSubnet,omitempty"`
	OVSMemoryLimit                    string              `json:"ovsMemoryLimit,omitempty" yaml:"ovsMemoryLimit,omitempty"`
	OpflexAgentLogLevel               string              `json:"opflexLogLevel,omitempty" yaml:"opflexLogLevel,omitempty"`
	OpflexClientSSL                   string              `json:"opflexClientSsl,omitempty" yaml:"opflexClientSsl,omitempty"`
	OpflexDeviceDeleteTimeout         string              `json:"opflexDeviceDeleteTimeout,omitempty" yaml:"opflexDeviceDeleteTimeout,omitempty"`
	OpflexMode                        string              `json:"opflexMode,omitempty" yaml:"opflexMode,omitempty"`
	OpflexServerPort                  string              `json:"opflexServerPort,omitempty" yaml:"opflexServerPort,omitempty"`
	OverlayVRFName                    string              `json:"overlayVrfName,omitempty" yaml:"overlayVrfName,omitempty"`
	PBRTrackingNonSnat                string              `json:"pbrTrackingNonSnat,omitempty" yaml:"pbrTrackingNonSnat,omitempty"`
	PodSubnetChunkSize                string              `json:"podSubnetChunkSize,omitempty" yaml:"podSubnetChunkSize,omitempty"`
	RunGbpContainer                   string              `json:"runGbpContainer,omitempty" yaml:"runGbpContainer,omitempty"`
	RunOpflexServerContainer          string              `json:"runOpflexServerContainer,omitempty" yaml:"runOpflexServerContainer,omitempty"`
	ServiceGraphEndpointAddDelay      string              `json:"serviceGraphEndpointAddDelay,omitempty" yaml:"serviceGraphEndpointAddDelay,omitempty"`
	ServiceGraphEndpointAddServices   []map[string]string `json:"serviceGraphEndpointAddServices,omitempty" yaml:"serviceGraphEndpointAddServices,omitempty"`
	ServiceGraphSubnet                string              `json:"nodeSvcSubnet,omitempty" yaml:"nodeSvcSubnet,omitempty"`
	ServiceMonitorInterval            string              `json:"serviceMonitorInterval,omitempty" yaml:"serviceMonitorInterval,omitempty"`
	ServiceVlan                       string              `json:"serviceVlan,omitempty" yaml:"serviceVlan,omitempty"`
	SnatContractScope                 string              `json:"snatContractScope,omitempty" yaml:"snatContractScope,omitempty"`
	SnatNamespace                     string              `json:"snatNamespace,omitempty" yaml:"snatNamespace,omitempty"`
	SnatPortRangeEnd                  string              `json:"snatPortRangeEnd,omitempty" yaml:"snatPortRangeEnd,omitempty"`
	SnatPortRangeStart                string              `json:"snatPortRangeStart,omitempty" yaml:"snatPortRangeStart,omitempty"`
	SnatPortsPerNode                  string              `json:"snatPortsPerNode,omitempty" yaml:"snatPortsPerNode,omitempty"`
	SriovEnable                       string              `json:"sriovEnable,omitempty" yaml:"sriovEnable,omitempty"`
	StaticExternalSubnet              string              `json:"externStatic,omitempty" yaml:"externStatic,omitempty"`
	SubnetDomainName                  string              `json:"subnetDomainName,omitempty" yaml:"subnetDomainName,omitempty"`
	SystemIdentifier                  string              `json:"systemId,omitempty" yaml:"systemId,omitempty"`
	Tenant                            string              `json:"tenant,omitempty" yaml:"tenant,omitempty"`
	Token                             string              `json:"token,omitempty" yaml:"token,omitempty"`
	UseAciAnywhereCRD                 string              `json:"useAciAnywhereCrd,omitempty" yaml:"useAciAnywhereCrd,omitempty"`
	UseAciCniPriorityClass            string              `json:"useAciCniPriorityClass,omitempty" yaml:"useAciCniPriorityClass,omitempty"`
	UseClusterRole                    string              `json:"useClusterRole,omitempty" yaml:"useClusterRole,omitempty"`
	UseHostNetnsVolume                string              `json:"useHostNetnsVolume,omitempty" yaml:"useHostNetnsVolume,omitempty"`
	UseOpflexServerVolume             string              `json:"useOpflexServerVolume,omitempty" yaml:"useOpflexServerVolume,omitempty"`
	UsePrivilegedContainer            string              `json:"usePrivilegedContainer,omitempty" yaml:"usePrivilegedContainer,omitempty"`
	VRFName                           string              `json:"vrfName,omitempty" yaml:"vrfName,omitempty"`
	VRFTenant                         string              `json:"vrfTenant,omitempty" yaml:"vrfTenant,omitempty"`
	VmmController                     string              `json:"vmmController,omitempty" yaml:"vmmController,omitempty"`
	VmmDomain                         string              `json:"vmmDomain,omitempty" yaml:"vmmDomain,omitempty"`
}
