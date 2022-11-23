package tests

import (
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
)

const (
	storkDeploymentName      = "stork"
	storkDeploymentNamespace = "kube-system"
	clusterName              = "tp-cluster"
	restoreNamePrefix        = "tp-restore"
	configMapName            = "kubeconfigs"
	sourceClusterName        = "source-cluster"
	destinationClusterName   = "destination-cluster"
	backupLocationName       = "tp-blocation"
	appReadinessTimeout      = 10 * time.Minute
	defaultTimeout           = 6 * time.Minute
	defaultRetryInterval     = 10 * time.Second
)

var (
	// enable all the after suite actions
	wantAllAfterSuiteActions bool = true

	// selectively enable after suite actions by setting wantAllAfterSuiteActions to false and setting these to true
	wantAfterSuiteSystemCheck     bool = false
	wantAfterSuiteValidateCleanup bool = false
)

var dash *aetosutil.Dashboard

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Basic", specReporters)
}

var _ = BeforeSuite(func() {
	dash = Inst().Dash
	log.Infof("Init instance")
	InitInstance()
	dash.TestSetBegin(dash.TestSet)
})

var _ = AfterSuite(func() {

	TestLogger = CreateLogger("SystemCheck.log")
	defer dash.TestSetEnd()

	defer CloseLogger(TestLogger)
	defer dash.TestCaseEnd()
	log.SetTorpedoFileOutput(TestLogger)

	dash.TestCaseBegin("System check", "validating system check and clean up", "", nil)
	if wantAllAfterSuiteActions || wantAfterSuiteSystemCheck {
		PerformSystemCheck()
	}
	if wantAllAfterSuiteActions || wantAfterSuiteValidateCleanup {
		ValidateCleanup()
	}
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
