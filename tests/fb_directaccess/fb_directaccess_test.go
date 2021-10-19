package fbdirectaccess

import (
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	. "github.com/portworx/torpedo/tests"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
)

const (
	formattingPxctlEstablishBackupCredential = "pxctl credentials create --provider s3 --s3-access-key %s --s3-secret-key %s --s3-region us-east-1 --s3-endpoint %s --s3-storage-class STANDARD %s"
	formattingPxctlDeleteFBBackupCredential  = "pxctl credentials delete %s"
	FBS3Credential                           = "fbS3bucket"
)

type FlashBladeEntry struct {
	ObjectStoreEndpoint string `json:"ObjectStoreEndpoint"`
	S3AccessKey         string `json:"S3AccessKey"`
	S3SecretKey         string `json:"S3SecretKey"`
}

type DiscoveryConfig struct {
	Blades []FlashBladeEntry `json:"FlashBlades"`
}

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : FB", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of making sure FB direct access are running as expected
var _ = Describe("{FBVolumeCRUDWithSDK}", func() {
	var contexts []*scheduler.Context

	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		Step("setup credential necessary for cloudsnap", func() {
			fbconfigs, err := GetPureSecret()
			Expect(err).NotTo(HaveOccurred())
			nodes := node.GetStorageDriverNodes()
			_, err = Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlEstablishBackupCredential, fbconfigs.Blades[0].S3AccessKey, fbconfigs.Blades[0].S3SecretKey, fbconfigs.Blades[0].ObjectStoreEndpoint, FBS3Credential), node.ConnectionOpts{
				Timeout:         k8s.DefaultTimeout,
				TimeBeforeRetry: k8s.DefaultRetryInterval,
				Sudo:            true,
			})
			Expect(err).NotTo(HaveOccurred())
		})
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPureSDK(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
		Step("delete credential used for cloudsnap", func() {
			nodes := node.GetStorageDriverNodes()
			_, err := Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlDeleteFBBackupCredential, FBS3Credential), node.ConnectionOpts{
				Timeout:         k8s.DefaultTimeout,
				TimeBeforeRetry: k8s.DefaultRetryInterval,
				Sudo:            true,
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This test performs basic test of making sure FB direct access are running as expected
var _ = Describe("{FBVolumeCRUDWithPXCTL}", func() {
	var contexts []*scheduler.Context
	It("schedule pure volumes on applications, run CRUD, tear down", func() {
		Step("setup credential necessary for cloudsnap", func() {
			fbconfigs, err := GetPureSecret()
			Expect(err).NotTo(HaveOccurred())
			nodes := node.GetStorageDriverNodes()
			_, err = Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlEstablishBackupCredential, fbconfigs.Blades[0].S3AccessKey, fbconfigs.Blades[0].S3SecretKey, fbconfigs.Blades[0].ObjectStoreEndpoint, FBS3Credential), node.ConnectionOpts{
				Timeout:         k8s.DefaultTimeout,
				TimeBeforeRetry: k8s.DefaultRetryInterval,
				Sudo:            true,
			})
			Expect(err).NotTo(HaveOccurred())
		})
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("purevolumestest-%d", i))...)
		}
		ValidateApplicationsPurePxctl(contexts)
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
		Step("delete credential used for cloudsnap", func() {
			nodes := node.GetStorageDriverNodes()
			_, err := Inst().N.RunCommand(nodes[0], fmt.Sprintf(formattingPxctlDeleteFBBackupCredential, FBS3Credential), node.ConnectionOpts{
				Timeout:         k8s.DefaultTimeout,
				TimeBeforeRetry: k8s.DefaultRetryInterval,
				Sudo:            true,
			})
			Expect(err).NotTo(HaveOccurred())
		})
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

func GetPureSecret() (DiscoveryConfig, error) {
	var pureConnectionJSON []byte
	conf := DiscoveryConfig{}
	secret, err := core.Instance().GetSecret("px-pure-secret-fbs3", "kube-system")
	if err != nil {
		log.Error("failed to retrieve px-pure-secret-fbs3 for FB S3 credentials")
		return conf, err
	}
	var ok bool
	if pureConnectionJSON, ok = secret.Data["pure.json"]; !ok {
		log.Error("failed to parse Json")
		return conf, err
	}

	err = json.Unmarshal(pureConnectionJSON, &conf)
	if err != nil {
		log.Error("Error unmarshaling config file: ", err)
		return conf, err
	}

	return conf, nil
}
