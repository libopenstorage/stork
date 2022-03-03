package ipv6util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidIpv6Address(t *testing.T) {
	// pxctl status tests
	addrs, _ := ParseIPv6AddressInPxctlCommand(PxctlStatus, sampleIpv6PxctlStatusOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlStatus)
	isIpv6 := AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlStatus, addrs)

	addrs, _ = ParseIPv6AddressInPxctlCommand(PxctlStatus, sampleIpv4PxctlStatusOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlStatus)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.False(t, isIpv6, "running command %v. addresses are expected to be ipv4, got: %v", PxctlStatus, addrs)

	// pxctl cluster list tests
	addrs, _ = ParseIPv6AddressInPxctlCommand(PxctlClusterList, sampleIpv6PxctlClusterListOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlClusterList)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlClusterList, addrs)

	// pxctl cluster inspect
	addrs, _ = ParseIPv6AddressInPxctlCommand(PxctlClusterInspect, sampleIpv6PxctlClusterInspectOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlClusterInspect)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlClusterInspect, addrs)

	addrs, _ = ParseIPv6AddressInPxctlCommand(PxctlVolumeList, sampleIpv6PxctlVolumeListOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlVolumeList)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlVolumeList, addrs)

	addrs, _ = ParseIPv6AddressInPxctlCommand(PxctlVolumeInspect, sampleIpv6PxctlVolumeInspectOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlVolumeInspect)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlVolumeInspect, addrs)

	var (
		err error
		ip  string
	)

	addrs, err = ParseIPv6AddressInPxctlCommand(PxctlServiceKvdbEndpoints, sampleIpv6PxctlSvcKvdbEndPtsOutput, -1)
	assert.NoError(t, err, "Failed to parse addresses running command: %v", PxctlServiceKvdbEndpoints)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlServiceKvdbEndpoints, addrs)

	addrs, err = ParseIPv6AddressInPxctlCommand(PxctlServiceKvdbMembers, sampleIpv6PxctlSvcKvdbMembersOutput, -1)
	assert.NoError(t, err, "Failed to parse addresses running command: %v", PxctlServiceKvdbMembers)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlServiceKvdbMembers, addrs)

	ip, err = ParseIPAddressInPxctlResourceDownAlert(sampleIpv6PxctlResourceDownAlertOutput, sampleDownResource)
	assert.NoError(t, err, "Failed to parse addresses running command: %v", PxctlAlertsShow)
	isIpv6 = AreAddressesIPv6([]string{ip})
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlAlertsShow, ip)

}
