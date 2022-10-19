package tests

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/pkg/aetosutil"
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
	post_install_hook_pod    = "pxcentral-post-install-hook"
	quick_maintenance_pod    = "quick-maintenance-repo"
	full_maintenance_pod     = "full-maintenance-repo"
	taskNamePrefix           = "backupcreaterestore"
)

var (
	// enable all the after suite actions
	wantAllAfterSuiteActions bool = true

	// selectively enable after suite actions by setting wantAllAfterSuiteActions to false and setting these to true
	wantAfterSuiteSystemCheck     bool = false
	wantAfterSuiteValidateCleanup bool = false
	pre_action_list                    = map[string]string{"cassandra": "nodetool flush -- keyspace1;", "postgres": "PGPASSWORD=$POSTGRES_PASSWORD; psql -U '$POSTGRES_USER' -c 'CHECKPOINT';"}
	post_action_list                   = map[string]string{"cassandra": "nodetool verify -- keyspace1;"}
	background_check_dict              = map[string]bool{"cassandra": false, "postgres": false}
	run_in_single_pod_dict             = map[string]bool{"cassandra": false, "postgres": false}
)

var log *logrus.Logger
var dash *aetosutil.Dashboard

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Backup", specReporters)
}

var _ = BeforeSuite(func() {
	dash = Inst().Dash
	logrus.Infof("Init instance")
	InitInstance()
})

var _ = AfterSuite(func() {
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
