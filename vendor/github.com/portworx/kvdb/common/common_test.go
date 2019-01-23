package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrunePrefixes(t *testing.T) {
	inputPrefixes := []string{"snapshot/", "snapshot/test", "snapshot1/"}
	expectedPrefixes := []string{"snapshot/", "snapshot1/"}

	testPrunePrefixes(t, expectedPrefixes, inputPrefixes)

	inputPrefixes = []string{"", "snapshot/test", "snapshot1/"}
	expectedPrefixes = []string{""}

	testPrunePrefixes(t, expectedPrefixes, inputPrefixes)

	inputPrefixes = []string{"snapshot", "snapshot/test", "snapshot1"}
	expectedPrefixes = []string{"snapshot"}

	testPrunePrefixes(t, expectedPrefixes, inputPrefixes)

}

func testPrunePrefixes(t *testing.T, expectedPrefixes, inputPrefixes []string) {
	e := PrunePrefixes(inputPrefixes)
	assert.Equal(t, len(e), len(expectedPrefixes), "Unexpected no. of prefixes")
	for i := 0; i < len(e); i++ {
		found := false
		for j := 0; j < len(expectedPrefixes); j++ {
			if e[i] == expectedPrefixes[j] {
				found = true
			}
		}
		assert.True(t, found, "Expected prefix not found")
	}

}
