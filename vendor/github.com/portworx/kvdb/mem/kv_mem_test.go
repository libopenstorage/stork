package mem

import (
	"testing"

	"github.com/portworx/kvdb/test"
)

func TestAll(t *testing.T) {
	test.RunBasic(New, t, Start, Stop)
}

func Start() error {
	return nil
}

func Stop() error {
	return nil
}
