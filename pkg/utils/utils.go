package utils

import (
	"fmt"
	"strings"
)

const (
	// CattlePrefix is the prefix to all Rancher related annotations and labels
	CattlePrefix = "cattle.io"
	// CattleProjectPrefix is the prefix used in all Rancher project related annotations and labels
	CattleProjectPrefix = "cattle.io/projectId"
)

// ParseKeyValueList parses a list of key=values string into a map
func ParseKeyValueList(expressions []string) (map[string]string, error) {
	matchLabels := make(map[string]string)
	for _, e := range expressions {
		entry := strings.SplitN(e, "=", 2)
		if len(entry) != 2 {
			return nil, fmt.Errorf("invalid key value: %s provided. "+
				"Example format: app=mysql", e)
		}

		matchLabels[entry[0]] = entry[1]
	}

	return matchLabels, nil
}
