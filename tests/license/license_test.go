package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	pxapi "github.com/portworx/torpedo/porx/px/api"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"

	. "github.com/portworx/torpedo/tests"
)

// Label used to name the licensing features
type Label string

const (
	defaultWaitRebootTimeout = 5 * time.Minute
	defaultWaitRebootRetry   = 10 * time.Second
	defaultCommandRetry      = 5 * time.Second
	defaultCommandTimeout    = 1 * time.Minute

	defaultTestConnectionTimeout = 15 * time.Minute
	defaultRetryInterval         = 10 * time.Second

	pureSecretName      = "px-pure-secret"
	pureSecretNamespace = "kube-system"
	pureSecretDataField = "pure.json"
	expiredLicString    = "License is expired"

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

	essentialsFaFbSKU = "PX-Essential FA/FB"

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
	faLicense = map[Label]interface{}{
		LabNodes:              &pxapi.LicensedFeature_Count{Count: 1000},
		LabVolumeSize:         &pxapi.LicensedFeature_CapacityTb{CapacityTb: 40},
		LabVolumes:            &pxapi.LicensedFeature_Count{Count: 200},
		LabHaLevel:            &pxapi.LicensedFeature_Count{Count: MaxHaLevel},
		LabSnapshots:          &pxapi.LicensedFeature_Count{Count: 5},
		LabAggregatedVol:      &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabSharedVol:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabEncryptedVol:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabGlobalSecretsOnly:  &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabScaledVol:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabResizeVolume:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabCloudSnap:          &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabCloudSnapDaily:     &pxapi.LicensedFeature_Count{Count: 1},
		LabCloudMigration:     &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabDisasterRecovery:   &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabPlatformBare:       &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabPlatformVM:         &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabNodeCapacity:       &pxapi.LicensedFeature_CapacityTb{CapacityTb: MaxNodeCapacity},
		LabNodeCapacityExtend: &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabLocalAttaches:      &pxapi.LicensedFeature_Count{Count: 128},
		LabOIDCSecurity:       &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabAUTCapacityMgmt:    &pxapi.LicensedFeature_Enabled{Enabled: false},
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
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[Label(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
					}
				}
			})
		})
		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This test performs basic reboot test of starting an application and destroying it (along with storage)
var _ = Describe("{BasicEssentialsRebootTest}", func() {
	var err error
	var contexts []*scheduler.Context

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("setupteardown-license-reboot-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get all nodes and reboot one by one", func() {
			nodesToReboot := node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				for _, n := range nodesToReboot {
					if n.IsStorageDriverInstalled {
						Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
							err = Inst().N.RebootNode(n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
								},
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         defaultTestConnectionTimeout,
								TimeBeforeRetry: defaultWaitRebootRetry,
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
							err := Inst().V.WaitDriverDownOnNode(n)
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String()), func() {

							err = Inst().S.IsNodeReady(n)
							Expect(err).NotTo(HaveOccurred())

							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							Expect(err).NotTo(HaveOccurred())
						})

						Step("validate apps", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					}
				}
			})
		})

		Step("Get SKU and compare with PX-Essentials FA/FB", func() {
			summary, err := Inst().V.GetLicenseSummary()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))

			Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
				fmt.Sprintf("SKU did not match: [%v] with [%v]",
					summary.SKU, essentialsFaFbSKU))
		})
		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

/* This test
1. Deletes px-pure-secret
2. Waits for lic_expiry_timeout
3. Verifies that Essentials lic expires
4. Re-creates px-pure-secret
5. Waits for next metering interval
6. Verifies that Essentials lic gets renewed again
*/
var _ = Describe("{DeleteSecretLicExpiryAndRenewal}", func() {
	var contexts []*scheduler.Context
	var pureSecretJSON string

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("delseclicexprenewal-%d", i))...)
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
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[Label(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
					}
				}
			})
		})

		Step("Fetch and store Pure secret", func() {
			var err error
			pureSecretJSON, err = Inst().S.GetSecretData(pureSecretNamespace, pureSecretName, pureSecretDataField)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to fetch secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step("Delete Pure secret", func() {
			err := Inst().S.DeleteSecret(pureSecretNamespace, pureSecretName)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to delete secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step(fmt.Sprintf("Wait for license expiry timeout of [%v]",
			Inst().LicenseExpiryTimeoutHours), func() {
			SleepWithContext(context.Background(), Inst().LicenseExpiryTimeoutHours)
			// Additional sleep to wait for lic to get expired on all nodes
			SleepWithContext(context.Background(), 10*time.Minute)
		})

		Step("Verify license is expired", func() {
			summary, err := Inst().V.GetLicenseSummary()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))
			Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
				fmt.Sprintf("SKU did not match: [%v]", essentialsFaFbSKU))
			Expect(summary.LicenesConditionMsg).To(ContainSubstring(expiredLicString),
				fmt.Sprintf("License did not expire after deleting [%s] secret", pureSecretName))
		})

		Step("Re-create Pure secret", func() {
			err := Inst().S.CreateSecret(pureSecretNamespace, pureSecretName, pureSecretDataField, pureSecretJSON)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step(fmt.Sprintf("Wait for next metering interval which is going to happen in [%v]",
			Inst().MeteringIntervalMins), func() {
			SleepWithContext(context.Background(), Inst().MeteringIntervalMins)
			// Additional sleep to wait for lic to get renewed on all nodes
			SleepWithContext(context.Background(), 5*time.Minute)
		})

		Step("Verify correct license got re-activated for PX-Essentials FA/FB", func() {
			summary, err := Inst().V.GetLicenseSummary()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))

			Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
				fmt.Sprintf("SKU did not match: [%v]", essentialsFaFbSKU))

			Expect(summary.LicenesConditionMsg).To(BeEmpty(),
				fmt.Sprintf("License did not got re-activated after recreating [%s] secret", pureSecretName))

			Step("Compare PX-Essentials FA/FB features vs activated license", func() {
				for _, feature := range summary.Features {
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[Label(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
					}
				}
			})
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

/*
  This Test:
1. Deletes px-pure-secret
2. Restarts PX on all nodes
3. Expects PX-Essentials FA/FB lic does not falls back to PX-Essentials license
*/
var _ = Describe("{DeleteSecretRebootAllNodes}", func() {
	var contexts []*scheduler.Context
	var err error
	var pureSecretJSON string
	var nodesToReboot []node.Node

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("delseclicexprenewal-%d", i))...)
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
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[Label(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
					}
				}
			})
		})

		Step("Fetch and store Pure secret", func() {
			var err error
			pureSecretJSON, err = Inst().S.GetSecretData(pureSecretNamespace, pureSecretName, pureSecretDataField)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to fetch secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step("Delete Pure secret", func() {
			err := Inst().S.DeleteSecret(pureSecretNamespace, pureSecretName)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to delete secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step("get all nodes and reboot one by one", func() {
			nodesToReboot = node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				for _, n := range nodesToReboot {
					if n.IsStorageDriverInstalled {
						Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
							err = Inst().N.RebootNode(n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
								},
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         defaultTestConnectionTimeout,
								TimeBeforeRetry: defaultWaitRebootRetry,
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
							err := Inst().V.WaitDriverDownOnNode(n)
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String()), func() {

							err = Inst().S.IsNodeReady(n)
							Expect(err).NotTo(HaveOccurred())

							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							Expect(err).NotTo(HaveOccurred())
						})

						Step("validate apps", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					}
				}
			})
		})

		Step("Get SKU and compare with PX-Essentials FA/FB", func() {
			summary, err := Inst().V.GetLicenseSummary()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))

			Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
				fmt.Sprintf("SKU changed after deleting [%s] secret and reboot to [%v]", pureSecretName, summary.SKU))

			Step("Compare PX-Essentials FA/FB features vs activated license", func() {
				for _, feature := range summary.Features {
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[Label(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[Label(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[Label(feature.Name)]))
					}
				}
			})
		})

		// Perform below steps to recover setup for other tests to continue
		Step("Re-create Pure secret", func() {
			err := Inst().S.CreateSecret(pureSecretNamespace, pureSecretName, pureSecretDataField, pureSecretJSON)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create secret [%s] in [%s] namespace. Error: [%v]",
					pureSecretName, pureSecretNamespace, err))
		})

		Step("Recover Portworx", func() {
			for _, node := range nodesToReboot {
				err := Inst().V.RestartDriver(node, nil)
				Expect(err).NotTo(HaveOccurred(), "failed to restart service on node: [%v]. Error: [%v]", node, err)
			}
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

// SleepWithContext will wait for the timer duration to expire, or the context
// is canceled. Which ever happens first. If the context is canceled the Context's
// error will be returned.
//
// Expects Context to always return a non-nil error if the Done channel is closed.
func SleepWithContext(ctx context.Context, dur time.Duration) error {
	t := time.NewTimer(dur)
	defer t.Stop()

	select {
	case <-t.C:
		break
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
