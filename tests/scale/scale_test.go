package tests

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

func TestScale(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : Scale")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs scale test of starting an application and destroying it (along with storage)
var _ = Describe("Scaled Setup and teardown", func() {
	It("has to setup, validate and teardown large scale apps", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("scalesetupteardown-%d", i))...)
		}

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
})

var _ = AfterSuite(func() {
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
