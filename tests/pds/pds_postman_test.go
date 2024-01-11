package tests

import (
	. "github.com/onsi/ginkgo"
	postmanLib "github.com/portworx/torpedo/drivers/postmanApiLoadDriver"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{RunPdsPostManApiLoadTests}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RunPdsPostManApiLoadTests", "Run PDS Specific Api Load Tests using Postman-Newman ", pdsLabels, 0)
	})
	It("Deploy Dataservices", func() {
		Step("Starting to execute Postman-Newman on PDS", func() {
			var resultsFileName = "postman_results"
			ctx, err := GetSourceClusterConfigPath()
			log.FailOnError(err, "failed while getting dest cluster path")
			postmanParams := postmanLib.PostmanDriver{
				ResultsFileName: resultsFileName,
				ResultType:      "json, cli",
				Iteration:       "2",
				Kubeconfig:      ctx}
			err = postmanLib.GetProjectNameToExecutePostman("pds", &postmanParams)
			log.FailOnError(err, "Postman execution failed.. Please check the logs manually.")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
