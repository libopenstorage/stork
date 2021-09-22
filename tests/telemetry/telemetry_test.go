package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

func TestTelemetryBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_telemetry.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Telemetry", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{DiagsBasic}", func() {
	var contexts []*scheduler.Context
	It("has to setup, validate, try to get diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("diagsbasic-%d", i))...)
		}

		ValidateApplications(contexts)

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s", currNode.Name), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/torpedo-diagsbasic-%s-%d.tar.gz", currNode.Name, time.Now().Unix()),
					ContainerName: "",
					OnHost:        true,
				}
				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true})
				Expect(err).NotTo(HaveOccurred())
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{DiagsAsyncBasic}", func() {
	var contexts []*scheduler.Context
	It("has to setup, validate, try to get a-sync diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("diagsasyncbasic-%d", i))...)
		}

		ValidateApplications(contexts)

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s", currNode.Name), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/torpedo-diagsasync-%s-%d.tar.gz", currNode.Name, time.Now().Unix()),
					ContainerName: "",
					OnHost:        true,
				}
				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true, Async: true})
				Expect(err).NotTo(HaveOccurred())
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{DiagsAsyncLiveBasic}", func() {
	var contexts []*scheduler.Context
	It("has to setup, validate, try to get a-sync diags on nodes and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("diagsasynclivebasic-%d", i))...)
		}

		ValidateApplications(contexts)

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		// One node at a time, collect diags and verify in S3
		for _, currNode := range node.GetWorkerNodes() {
			Step(fmt.Sprintf("collect diags on node: %s", currNode.Name), func() {

				config := &torpedovolume.DiagRequestConfig{
					DockerHost:    "unix:///var/run/docker.sock",
					OutputFile:    fmt.Sprintf("/var/cores/torpedo-diagsasynclive-%s-%d.tar.gz", currNode.Name, time.Now().Unix()),
					ContainerName: "",
					Live:          true,
					OnHost:        true,
				}

				err := Inst().V.CollectDiags(currNode, config, torpedovolume.DiagOps{Validate: true, Async: true})
				Expect(err).NotTo(HaveOccurred())
			})
		}

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
