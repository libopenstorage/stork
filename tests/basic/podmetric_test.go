package tests

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	rest "github.com/portworx/torpedo/pkg/restutil"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	logglyIterateUrl = "https://pxlite.loggly.com/apiv2/events/iterate"
)

var _ = Describe("{PodMetricFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context
	var namespacePrefix string

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)

		StartTorpedoTest("PodMetricFunctional", "Functional Tests for Pod Metrics", nil, testrailID)
		log.InfoD("Deploy applications")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}

		log.InfoD("Validate applications")
		ValidateApplications(contexts)
	})

	Context("{PodMetricsSample}", func() {
		namespacePrefix = "podmetricsample"

		// shared test function for pod metric functional tests
		sharedTestFunction := func() {
			It("has to fetch the logs from loggly", func() {
				log.InfoD("fetching logs from loggly")
				resp, code, err := getLogglyData("q=tag:meteringData&from=-60m&until=now&size=1")

				log.FailOnError(err, "Failed to make request to loggly")
				dash.VerifyFatal(code, 200, fmt.Sprintf("loggly return code %v not equal to 200", code))
				dash.VerifyFatal(len(resp) == 0, false, "loggy return empty response")
			})
		}

		// Sample pod metric tests
		Describe("{SamplePodMetricTest}", func() {
			JustBeforeEach(func() {
				// testrailID =
			})
			sharedTestFunction()
		})

	})

	AfterEach(func() {
		Step("destroy apps", func() {
			log.InfoD("destroying apps")
			if CurrentGinkgoTestDescription().Failed {
				log.InfoD("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}
		})
	})

	AfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
		defer EndTorpedoTest()
	})
})

func getLogglyData(query string) ([]byte, int, error) {
	logglyToken, ok := os.LookupEnv("LOGGLY_API_TOKEN")
	dash.VerifyFatal(ok, true, "failed to fetch loggly api token")
	headers := make(map[string]string)
	headers["Authorization"] = fmt.Sprintf("Bearer %v", logglyToken)
	return rest.Get(fmt.Sprintf("%v?%v", logglyIterateUrl, query), nil, headers)
}
