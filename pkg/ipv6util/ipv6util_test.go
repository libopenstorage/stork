package ipv6util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidIpv6Address(t *testing.T) {
	// pxctl status tests
	addrs := ParseIPv6AddressInPxctlCommand(PxctlStatus, sampleIpv6PxctlStatusOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlStatus)
	isIpv6 := AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlStatus, addrs)

	addrs = ParseIPv6AddressInPxctlCommand(PxctlStatus, sampleIpv4PxctlStatusOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlStatus)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.False(t, isIpv6, "running command %v. addresses are expected to be ipv4, got: %v", PxctlStatus, addrs)

	// pxctl cluster list tests
	addrs = ParseIPv6AddressInPxctlCommand(PxctlClusterList, sampleIpv6PxctlClusterListOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlClusterList)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlClusterList, addrs)

	// pxctl cluster inspect
	addrs = ParseIPv6AddressInPxctlCommand(PxctlClusterInspect, sampleIpv6PxctlClusterInspectOutput, sampleNodeCount)
	assert.NotEmpty(t, addrs, "addresses are not expected to be empty. running command: %v", PxctlClusterInspect)
	isIpv6 = AreAddressesIPv6(addrs)
	assert.True(t, isIpv6, "running command %v. addresses are expected to be ipv6, got: %v", PxctlClusterInspect, addrs)
}
