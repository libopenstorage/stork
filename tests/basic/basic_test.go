package tests

import (
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
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
	RunSpecs(t, "Torpedo : Backup")
}

var _ = BeforeSuite(func() {
	dash = Inst().Dash
	log.Infof("Init instance")
	value, exists := os.LookupEnv("NOMAD_ADDR")
	if !exists {
		InitInstance()
		dash.TestSetBegin(dash.TestSet)
		EnableAutoFSTrim()
	} else {
		log.Infof("Value set for Nomad cluster is: %v", value)
		dash.TestSetBegin(dash.TestSet)
	}
})

var _ = AfterSuite(func() {
	_, exists := os.LookupEnv("NOMAD_ADDR")
	if !exists {
		TestLogger = CreateLogger("SystemCheck.log")
		defer dash.TestSetEnd()
		defer CloseLogger(TestLogger)
		defer dash.TestCaseEnd()
		// making sure validate clean up executed even if systemcheck failed
		defer func() {
			if wantAllAfterSuiteActions || wantAfterSuiteValidateCleanup {
				dash.TestCaseBegin("Validate Cleanup", "Validating clean up", "", nil)
				ValidateCleanup()
			}
		}()

		log.SetTorpedoFileOutput(TestLogger)
		if !Inst().SkipSystemChecks {
			if wantAllAfterSuiteActions || wantAfterSuiteSystemCheck {
				dash.TestCaseBegin("System Checks", "Perform system checks", "", nil)
				PerformSystemCheck()
			}
		}
	}
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
