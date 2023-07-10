//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/libopenstorage/stork/pkg/utils"
)

const (
	aetosStatsURL = "http://aetos.pwx.purestorage.com/dashboard/stats?stats_type=migration_stats_new&limit=100"
)

func TestExportStatsGetStats(t *testing.T) {
	fmt.Println("Hello")
	data, err := utils.GetMigrationStatsFromAetos(aetosStatsURL)
	require.NoError(t, err, "Failed to get stats: %v")

	prettyData, err := PrettyStruct(data)
	require.NoError(t, err, "Failed to print pretty data: %v")
	fmt.Println(prettyData)
}

func TestExportStatsPushMockStats(t *testing.T) {
	err := utils.WriteMigrationStatsToAetos(NewStat())
	require.NoError(t, err, "Failed to write stats: %v")
}

func NewStat() utils.StatsExportType {
	mockStat := utils.StatsExportType{}
	return mockStat
}

func PrettyStruct(data interface{}) (string, error) {
	val, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return "", err
	}
	return string(val), nil
}
