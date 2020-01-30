package portworx

import (
	"fmt"
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
			rule:         aututils.PoolRuleByTotalSize(11, 10, aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(21, 10, aututils.RuleScaleTypeAddDisk, nil),
			pool:         getTestPool(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(20, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 30,
		},
		{
			rule:         aututils.PoolRuleByTotalSize(11, 10, aututils.RuleScaleTypeResizeDisk, nil),
			pool:         getTestPool(10, 2, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 11,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 10, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(10, 6, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 20,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 10, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(10, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40,
		},
		{
			rule:         aututils.PoolRuleByAvailableCapacity(50, 100, aututils.RuleScaleTypeAddDisk),
			pool:         getTestPool(10, 16, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			node:         getTestNode(10, 1, api.StorageMedium_STORAGE_MEDIUM_MAGNETIC),
			expectedSize: 40,
		},
	}

	for _, tc := range testCases {
		calculatedSize, err := inst.EstimatePoolExpandSize(tc.rule, tc.pool, tc.node)
		if !tc.expectedToFail {
			require.NoError(t, err)
			require.Equalf(t, tc.expectedSize*units.GiB, calculatedSize, fmt.Sprintf("expected: %d actual: %d", tc.expectedSize, calculatedSize/units.GiB))
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
		StorageNode: api.StorageNode{Disks: disks},
	}
}
