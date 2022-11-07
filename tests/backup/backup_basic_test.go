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
	defaultRetryInterval     = 10 * time.Second
	enumerateBatchSize       = 100
	taskNamePrefix           = "backupcreaterestore"
	orgID                    = "default"
	defaultTimeout           = 5 * time.Minute
)

var (
	// enable all the after suite actions
	wantAllAfterSuiteActions bool = true

	// selectively enable after suite actions by setting wantAllAfterSuiteActions to false and setting these to true
	wantAfterSuiteSystemCheck     bool = false
	wantAfterSuiteValidateCleanup bool = false
	// User should keep updating the below 3 datas
	pre_rule_app   = []string{"cassandra", "postgres"}
	post_rule_app  = []string{"cassandra"}
	app_parameters = map[string]map[string]string{
		"cassandra": {"pre_action_list": "nodetool flush -- keyspace1;", "post_action_list": "nodetool verify -- keyspace1;", "background": "false", "run_in_single_pod": "false"},
		"postgres":  {"pre_action_list": "PGPASSWORD=$POSTGRES_PASSWORD; psql -U '$POSTGRES_USER' -c 'CHECKPOINT';", "background": "false", "run_in_single_pod": "false"},
	}
)

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Backup", specReporters)
}

var log *logrus.Logger
var dash *aetosutil.Dashboard
var _ = BeforeSuite(func() {
	dash = Inst().Dash
	log = Inst().Logger
	log.Infof("Init instance")
	InitInstance()
	dash.TestSetBegin(dash.TestSet)
})

var _ = AfterSuite(func() {
	defer dash.TestSetEnd()
	defer dash.TestCaseEnd()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
