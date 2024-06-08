package portworx

import (
	"fmt"
	"strings"
	"testing"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/pborman/uuid"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/units"
	"github.com/stretchr/testify/require"
)

func TestCalculateAutopilotObjectSize(t *testing.T) {
	inst := &portworx{}

	type testCase struct {
		rule           apapi.AutopilotRule
		pool           node.StoragePool
		node           node.Node
		expectedSize   uint64
		expectedToFail bool
	}

	testCases := []testCase{
		{
			rule:         aututils.PoolRuleByTotalSize(11, 50, aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(11, 300, aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(21, 10, aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 30 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(11, 10, aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 11 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(11, 300, aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(41, "10Gi", aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 50 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(31, "1Gi", aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(30, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(30, 3, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 33 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(650, "23Gi", aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(640, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(640, 5, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 755 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(11, "5Gi", aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(11, "25Gi", aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(21, "5Gi", aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 30 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(11, "5Gi", aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 15 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleFixedScaleSizeByTotalSize(11, "25Gi", aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 35 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 10, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(10, 6, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 50, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(10, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 50 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 100, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(20, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 50, aututils.RuleScaleTypeResizeDisk),
			pool:         getTestPool(10, 6, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 22.5 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 100, aututils.RuleScaleTypeResizeDisk),
			pool:         getTestPool(10, 6, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 50, aututils.RuleScaleTypeResizeDisk),
			pool:         getTestPool(10, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 50.625 * units.GiB,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 100, aututils.RuleScaleTypeResizeDisk),
			pool:         getTestPool(10, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40 * units.GiB,
		},
	}

	for _, tc := range testCases {
		calculatedSize, err := inst.EstimatePoolExpandSize(tc.rule, tc.pool, tc.node)
		if !tc.expectedToFail {
			require.NoError(t, err)
			require.Equalf(t, tc.expectedSize, calculatedSize, fmt.Sprintf("expected: %d actual: %d", tc.expectedSize, calculatedSize))
		} else {
			require.Errorf(t, err, "test case was expected to fail")
		}
	}
}

func getTestPool(initialSize, workloadSize uint64, medium api.StorageMedium) node.StoragePool {
	return node.StoragePool{
		// not needed for test for now
		// StoragePool:       nil,
		StoragePoolAtInit: &api.StoragePool{
			TotalSize: initialSize * units.GiB,
			Uuid:      uuid.New(),
			Medium:    medium,
		},
		WorkloadSize: workloadSize * units.GiB,
	}
}
func getTestNode(poolSize, totalDisks uint64, medium api.StorageMedium) node.Node {
	toChar := func(i int) rune {
		return rune('A' - 1 + i)
	}

	perDiskSize := poolSize / totalDisks
	suffixStart := 33 // 'a'
	disks := make(map[string]*api.StorageResource)
	for totalDisks > 0 {
		path := fmt.Sprintf("/dec/sd%q", toChar(suffixStart))
		disks[path] = &api.StorageResource{
			Medium: medium,
			Size:   perDiskSize * units.GiB,
			Path:   path,
		}
		suffixStart++
		totalDisks--
	}
	return node.Node{
		StorageNode: &api.StorageNode{Disks: disks},
	}
}

func TestEstimatedVolumeSize(t *testing.T) {
	driver := portworx{}
	testCases := []struct {
		rule                   apapi.AutopilotRule
		initialSize            uint64
		workloadSize           uint64
		expectedCalculatedSize uint64
		expectedResizeCount    int
		errorExpected          bool
	}{
		{
			rule:                   aututils.PVCRuleByTotalSize(6, 100, "10Gi"),
			initialSize:            5 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 10 * units.GiB,
			expectedResizeCount:    1,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByTotalSize(6, 100, "5Gi"),
			initialSize:            5 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 5 * units.GiB,
			expectedResizeCount:    0,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByTotalSize(15, 100, "12Gi"),
			initialSize:            5 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 12 * units.GiB,
			expectedResizeCount:    2,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByTotalSize(15, 100, "4Gi"),
			initialSize:            5 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 5 * units.GiB,
			expectedResizeCount:    0,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByTotalSize(6, 100, ""),
			initialSize:            5 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 10 * units.GiB,
			expectedResizeCount:    1,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByUsageCapacity(50, 100, ""),
			initialSize:            10 * units.GiB,
			workloadSize:           4 * units.GiB,
			expectedCalculatedSize: 10 * units.GiB,
			expectedResizeCount:    0,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByUsageCapacity(50, 100, ""),
			initialSize:            10 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 20 * units.GiB,
			expectedResizeCount:    1,
			errorExpected:          false,
		},
		{
			rule:                   aututils.PVCRuleByUsageCapacity(50, 100, "12Gi"),
			initialSize:            10 * units.GiB,
			workloadSize:           10 * units.GiB,
			expectedCalculatedSize: 12 * units.GiB,
			expectedResizeCount:    1,
			errorExpected:          false,
		},
	}
	for _, tc := range testCases {
		size, resizeCount, err := driver.EstimateVolumeExpand(tc.rule, tc.initialSize, tc.workloadSize)
		msg := fmt.Sprintf("Expected: %v, got: %v", tc.expectedCalculatedSize, size)
		require.NoError(t, err)
		require.Equal(t, tc.expectedCalculatedSize, size, msg)
		msg = fmt.Sprintf("Expected: %v, got: %v", tc.expectedResizeCount, resizeCount)
		require.Equal(t, tc.expectedResizeCount, resizeCount)
	}
}

func TestParseLsblkOutput_PFormat(t *testing.T) {
	lsblkOutput := strings.ReplaceAll(`
3624a9370afb23ff64fc142a600062859p1    3219128320
*-3624a9370afb23ff64fc142a600062859 1073741824000
  |-sdf                             1073741824000
  *-sdg                             1073741824000
sdd                                  137438953472
sdb                                   68719476736
3624a9370afb23ff64fc142a600062859p2 1070521630208
*-3624a9370afb23ff64fc142a600062859 1073741824000
  |-sdf                             1073741824000
  *-sdg                             1073741824000
sde                                  137438953472
sdc                                  137438953472
sda2                                 133438636032
*-sda                                137438953472
sda1                                   3999268864
*-sda                                137438953472
3624a9370afb23ff64fc142a600062868     34359738368
|-sdp                                 34359738368
*-sdap                                34359738368`, "*", "`")
	out, err := parseLsblkOutput(lsblkOutput)
	require.NoError(t, err)
	require.Contains(t, out, "3624a9370afb23ff64fc142a600062859")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062859"].SinglePaths, "sdf")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062859"].SinglePaths, "sdg")
	require.Equal(t, uint64(1073741824000), out["3624a9370afb23ff64fc142a600062859"].Size)
	require.Contains(t, out, "3624a9370afb23ff64fc142a600062868")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062868"].SinglePaths, "sdp")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062868"].SinglePaths, "sdap")
	require.Equal(t, uint64(34359738368), out["3624a9370afb23ff64fc142a600062868"].Size)
}

func TestParseLsblkOutput_PartFormat(t *testing.T) {
	lsblkOutput := strings.ReplaceAll(`
3624a9370afb23ff64fc142a600062859-part1    3219128320
*-3624a9370afb23ff64fc142a600062859     1073741824000
  |-sdf                                 1073741824000
  *-sdg                                 1073741824000
sdd                                      137438953472
sdb                                       68719476736
3624a9370afb23ff64fc142a600062859-part2 1070521630208
*-3624a9370afb23ff64fc142a600062859     1073741824000
  |-sdf                                 1073741824000
  *-sdg                                 1073741824000
sde                                      137438953472
sdc                                      137438953472
sda2                                     133438636032
*-sda                                    137438953472
sda1                                       3999268864
*-sda                                    137438953472
3624a9370afb23ff64fc142a600062868         34359738368
|-sdap                                    34359738368
*-sdp                                     34359738368`, "*", "`")
	out, err := parseLsblkOutput(lsblkOutput)
	require.NoError(t, err)
	require.Contains(t, out, "3624a9370afb23ff64fc142a600062859")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062859"].SinglePaths, "sdf")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062859"].SinglePaths, "sdg")
	require.Equal(t, uint64(1073741824000), out["3624a9370afb23ff64fc142a600062859"].Size)
	require.Contains(t, out, "3624a9370afb23ff64fc142a600062868")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062868"].SinglePaths, "sdp")
	require.Contains(t, out["3624a9370afb23ff64fc142a600062868"].SinglePaths, "sdap")
	require.Equal(t, uint64(34359738368), out["3624a9370afb23ff64fc142a600062868"].Size)
}
