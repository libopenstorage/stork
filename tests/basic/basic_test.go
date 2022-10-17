package tests

import (
	"github.com/portworx/torpedo/pkg/aetosutil"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
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

var log *logrus.Logger
var dash *aetosutil.Dashboard
var f *os.File

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Basic", specReporters)
}

var _ = BeforeSuite(func() {
	log = Inst().Logger
	dash = Inst().Dash
	log.Infof("Init instance")
	InitInstance()
	dash.TestSetBegin(dash.TestSet)
})

var _ = AfterSuite(func() {
	f = CreateLogFile("SystemCheck.log")
	defer dash.TestSetEnd()
	defer CloseLogFile(f)
	defer dash.TestCaseEnd()
	if f != nil {
		SetTorpedoFileOutput(log, f)
	}

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
