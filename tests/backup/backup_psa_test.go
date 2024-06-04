// TODO: Remove this file before merging to master
package tests

import (
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This is a dummy PSA testcase to validate the PSA related methods for RKE
var _ = Describe("{DummyPSATestcase}", Label(TestCaseLabelsMap[DummyPSATestcase]...), func() {
	JustBeforeEach(func() {
		log.InfoD("No pre-configuration needed")
	})

	It("Dummy PSA testcase to validate the PSA related methods for vanilla", func() {
		Step("Dummy PSA testcase to validate the PSA related methods for vanilla", func() {
			err := ConfigureClusterLevelPSA("restricted", []string{})
			dash.VerifyFatal(err, nil, "Setting PSA level configuration ")
			err = VerifyClusterlevelPSA()
			dash.VerifyFatal(err, nil, "Verify PSA level configuration ")
			err = RevertClusterLevelPSA()
			dash.VerifyFatal(err, nil, "Revert PSA level configuration ")
		})
	})

	JustAfterEach(func() {
		log.InfoD("Nothing to be deleted")
	})
})
