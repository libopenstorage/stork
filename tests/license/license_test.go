package tests

import (
	"fmt"
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

// Label used to name the licensing features
type Label string

const (
	// LabNodes - Number of nodes maximum
	LabNodes Label = "Nodes"
	// LabVolumeSize - Volume capacity [TB] maximum
	LabVolumeSize Label = "VolumeSize"
	// LabVolumes - Number of volumes per cluster maximum
	LabVolumes Label = "Volumes"
	// LabSnapshots - Number of snapshots per volume maximum
	LabSnapshots Label = "Snapshots"
	// LabHaLevel - Volume replica count
	LabHaLevel Label = "HaLevel"
	// LabSharedVol - Shared volumes
	LabSharedVol Label = "SharedVolume"
	// LabEncryptedVol - BYOK data encryption
	LabEncryptedVol Label = "EncryptedVolume"
	// LabScaledVol - Volume sets
	LabScaledVol Label = "ScaledVolume"
	// LabAggregatedVol - Storage aggregation
	LabAggregatedVol Label = "AggregatedVolume"
	// LabResizeVolume - Resize volumes on demand
	LabResizeVolume Label = "ResizeVolume"
	// LabCloudSnap - Snapshot to object store [CloudSnap]
	LabCloudSnap Label = "SnapshotToObjectStore"
	// LabCloudSnapDaily - Number of CloudSnaps daily per volume maximum
	LabCloudSnapDaily Label = "SnapshotToObjectStoreDaily"
	// LabCloudMigration -Cluster-level migration [Kube-motion/Data Migration]
	LabCloudMigration Label = "CloudMigration"
	// LabDisasterRecovery - Disaster Recovery [PX-DR]
	LabDisasterRecovery Label = "DisasterRecovery"
	// LabAUTCapacityMgmt - Autopilot Capacity Management
	LabAUTCapacityMgmt Label = "AUTCapacityManagement"
	// LabPlatformBare - Bare-metal hosts
	LabPlatformBare Label = "EnablePlatformBare"
	// LabPlatformVM - Virtual machine hosts
	LabPlatformVM Label = "EnablePlatformVM"
	// LabNodeCapacity - Node disk capacity [TB] maximum
	LabNodeCapacity Label = "NodeCapacity"
	// LabNodeCapacityExtend - Node disk capacity extension
	LabNodeCapacityExtend Label = "NodeCapacityExtension"
	// LabLocalAttaches - Number of attached volumes per node maximum
	LabLocalAttaches Label = "LocalVolumeAttaches"
	// LabOIDCSecurity - OIDC Security
	LabOIDCSecurity Label = "OIDCSecurity"
	// LabGlobalSecretsOnly - Limit BYOK encryption to cluster-wide secrets
	LabGlobalSecretsOnly Label = "GlobalSecretsOnly"

	essentialsFaFbSKU = "PX-Essentials FA/FB"
	// UnlimitedNumber represents the unlimited number of licensed resource.
	// note - the max # Flex counts handle, is actually 999999999999999990
	UnlimitedNumber = int64(0x7FFFFFFF) // C.FLX_FEATURE_UNCOUNTED_VALUE = 0x7FFFFFFF  (=2147483647)

	// -- Testing maximums below

	// MaxNumNodes is a maximum nodes in a cluster
	MaxNumNodes = int64(1000)
	// MaxNumVolumes is a maximum number of volumes in a cluster
	MaxNumVolumes = int64(100000)
	// MaxVolumeSize is a maximum volume size for single volume [in TB]
	MaxVolumeSize = int64(40)
	// MaxNodeCapacity defines the maximum node's disk capacity [in TB]
	MaxNodeCapacity = int64(256)
	// MaxLocalAttachCount is a maximum number of local volume attaches
	MaxLocalAttachCount = int64(256)
	// MaxHaLevel is a maximum replication factor
	MaxHaLevel = int64(3)
	// MaxNumSnapshots is a maximum number of snapshots
	MaxNumSnapshots = int64(64)
)

var (
	faLicense = map[Label]int64{
		LabNodes:              1000,
		LabVolumeSize:         40,
		LabVolumes:            200,
		LabHaLevel:            MaxHaLevel,
		LabSnapshots:          5,
		LabAggregatedVol:      0,
		LabSharedVol:          UnlimitedNumber,
		LabEncryptedVol:       UnlimitedNumber,
		LabGlobalSecretsOnly:  UnlimitedNumber,
		LabScaledVol:          UnlimitedNumber,
		LabResizeVolume:       UnlimitedNumber,
		LabCloudSnap:          UnlimitedNumber,
		LabCloudSnapDaily:     1,
		LabCloudMigration:     0,
		LabDisasterRecovery:   0,
		LabPlatformBare:       UnlimitedNumber,
		LabPlatformVM:         UnlimitedNumber,
		LabNodeCapacity:       MaxNodeCapacity,
		LabNodeCapacityExtend: 0,
		LabLocalAttaches:      128,
		LabOIDCSecurity:       0,
		LabAUTCapacityMgmt:    0,
	}
)

func TestLicense(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_license.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : License", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{BasicEssentialsFaFbTest}", func() {
	var contexts []*scheduler.Context

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("setupteardown-license-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("Get SKU and compare with PX-Essentials FA/FB", func() {
			summary, err := Inst().V.GetLicenseSummary()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))

			Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
				fmt.Sprintf("SKU did not match: [%v]", essentialsFaFbSKU))

			Step("Compare PX-Essentials FA/FB features vs activated license", func() {
				for _, feature := range summary.Features {
					Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
						fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
				}
			})
		})
		ValidateAndDestroy(contexts, nil)
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
