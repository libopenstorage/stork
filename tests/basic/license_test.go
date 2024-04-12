package tests

import (
	"fmt"
	"strings"
	"time"

	pxapi "github.com/libopenstorage/operator/api/px"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/portworx/torpedo/tests"
)

const (
	defaultReadynessTimeout = 2 * time.Minute

	PureSecretNamespace = "kube-system"
	pureSecretDataField = "pure.json"
	expiredLicString    = "License is expired"

	essentialsFaFbSKU = "Portworx CSI for FA/FB"
)

var (
	faLicense = map[LabLabel]interface{}{
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
		LabNodeCapacityExtend: &pxapi.LicensedFeature_Enabled{Enabled: true},
		LabLocalAttaches:      &pxapi.LicensedFeature_Count{Count: 128},
		LabOIDCSecurity:       &pxapi.LicensedFeature_Enabled{Enabled: false},
		LabAUTCapacityMgmt:    &pxapi.LicensedFeature_Enabled{Enabled: false},
	}
)

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{BasicEssentialsFaFbTest}", func() {
	var testrailID = 56354
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/56354
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("BasicEssentialsFaFbTest", "Validates `Portworx CSI for FA/FB` license SKU", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
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
					if _, ok := faLicense[LabLabel(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[LabLabel(feature.Name)]))
					}
				}
			})
		})
		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs basic reboot test of starting an application and destroying it (along with storage)
var _ = Describe("{BasicEssentialsRebootTest}", func() {
	var testrailID = 56356
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/56356
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("BasicEssentialsRebootTest", "Validates `Portworx CSI for FA/FB` remains active after reboot", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
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
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs basic limit test of starting an application and destroying it (along with storage)
var _ = Describe("{BasicEssentialsAggrSnapLimitTest}", func() {
	var testrailID = 56355
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/56355
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("BasicEssentialsAggrSnapLimitTest", "Validates `Portworx CSI for FA/FB` lic's limits ", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to setup, validate and teardown apps", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("license-aggrsnaplimit-%d", i))...)
		}
		appScaleFactor := time.Duration(Inst().GlobalScaleFactor)
		for _, ctx := range contexts {
			if strings.Contains(ctx.App.Key, "snap") || strings.Contains(ctx.App.Key, "aggr") {
				Step(fmt.Sprintf("Expect volume validation for %s app to fail", ctx.App.Key), func() {
					err := Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultReadynessTimeout, defaultRetryInterval, &scheduler.VolumeOptions{ExpectError: false})
					Expect(err).To(HaveOccurred(),
						fmt.Sprintf("No error occurred while validating storage for app [%s]", ctx.App.Key))
				})
			} else {
				Step(fmt.Sprintf("Expect volume validation for %s app to pass", ctx.App.Key), func() {
					err := Inst().S.ValidateVolumes(ctx, appScaleFactor*defaultReadynessTimeout, defaultRetryInterval, &scheduler.VolumeOptions{ExpectError: false})
					Expect(err).ToNot(HaveOccurred(),
						fmt.Sprintf("Error occurred during validating storage for app [%s]. Error: %v", ctx.App.Key, err))
				})
			}
			// If we are running the mysql-aggr test execute next steps.
			if strings.Contains(ctx.App.Key, "snap") || strings.Contains(ctx.App.Key, "aggr") {
				Step(fmt.Sprintf("Expect %s app to fail to start", ctx.App.Key), func() {
					err := Inst().S.WaitForRunning(ctx, appScaleFactor*defaultReadynessTimeout, defaultRetryInterval)
					Expect(err).To(HaveOccurred(),
						"app with aggregated volumes got deployed successfully when lic does not allow aggregated volumes.")
				})
			} else {
				Step(fmt.Sprintf("Wait for %s app to start running", ctx.App.Key), func() {
					err := Inst().S.WaitForRunning(ctx, appScaleFactor*defaultReadynessTimeout, defaultRetryInterval)
					Expect(err).NotTo(HaveOccurred())
				})
			}
		}
	})

	for _, ctx := range contexts {
		TearDownContext(ctx, nil)
	}

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

/*
	This test

1. Deletes px-pure-secret
2. Waits for lic_expiry_timeout
3. Verifies that Essentials lic expires
4. Re-creates px-pure-secret
5. Waits for next metering interval
6. Verifies that Essentials lic gets renewed again
*/
var _ = Describe("{DeleteSecretLicExpiryAndRenewal}", func() {
	var testrailID = 56357
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/56357
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteSecretLicExpiryAndRenewal", "Validates lic expires if `px-pure-secret` is deleted and it gets renewed when secret is re-created", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
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
					if _, ok := faLicense[LabLabel(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[LabLabel(feature.Name)]))
					}
				}
			})
		})

		Step("Fetch and store Pure secret", func() {
			var err error
			pureSecretJSON, err = Inst().S.GetSecretData(PureSecretNamespace, PureSecretName, pureSecretDataField)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to fetch secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
		})

		Step("Delete Pure secret", func() {
			err := Inst().S.DeleteSecret(PureSecretNamespace, PureSecretName)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to delete secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
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
				fmt.Sprintf("License did not expire after deleting [%s] secret", PureSecretName))
		})

		Step("Re-create Pure secret", func() {
			err := Inst().S.CreateSecret(PureSecretNamespace, PureSecretName, pureSecretDataField, pureSecretJSON)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
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
				fmt.Sprintf("License did not got re-activated after recreating [%s] secret", PureSecretName))

			Step("Compare PX-Essentials FA/FB features vs activated license", func() {
				for _, feature := range summary.Features {
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[LabLabel(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[LabLabel(feature.Name)]))
					}
				}
			})
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

/*
This Test:

1. Deletes px-pure-secret
2. Restarts PX on all nodes
3. Expects PX-Essentials FA/FB lic does not falls back to PX-Essentials license
*/
var _ = Describe("{DeleteSecretRebootAllNodes}", func() {
	var testrailID = 84245
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/84245
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("DeleteSecretRebootAllNodes", "Validates `Portworx CSI for FA/FB` does not fall back to `PX-Essentials` after deleting `PX-Pure-Secret`", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
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
					if _, ok := faLicense[LabLabel(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[LabLabel(feature.Name)]))
					}
				}
			})
		})

		Step("Fetch and store Pure secret", func() {
			var err error
			pureSecretJSON, err = Inst().S.GetSecretData(PureSecretNamespace, PureSecretName, pureSecretDataField)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to fetch secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
		})

		Step("Delete Pure secret", func() {
			err := Inst().S.DeleteSecret(PureSecretNamespace, PureSecretName)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to delete secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
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
				fmt.Sprintf("SKU changed after deleting [%s] secret and reboot to [%v]", PureSecretName, summary.SKU))

			Step("Compare PX-Essentials FA/FB features vs activated license", func() {
				for _, feature := range summary.Features {
					// if the feature limit exists in the hardcoded license limits we test it.
					if _, ok := faLicense[LabLabel(feature.Name)]; ok {
						Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
							fmt.Sprintf("%v did not match: [%v]", feature.Quantity, faLicense[LabLabel(feature.Name)]))
					}
				}
			})
		})

		// Perform below steps to recover setup for other tests to continue
		Step("Re-create Pure secret", func() {
			err := Inst().S.CreateSecret(PureSecretNamespace, PureSecretName, pureSecretDataField, pureSecretJSON)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create secret [%s] in [%s] namespace. Error: [%v]",
					PureSecretName, PureSecretNamespace, err))
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
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs basic test disabling callhome and checking if the licnse stays valid
var _ = Describe("{DisableCallHomeTest}", func() {
	var testrailID = 84245
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/84245
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("DisableCallHomeTest", "Validates disabling callhome does not expires lic", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to setup, validate and teardown apps, then disable callhome and wait 65 minutes to verify the license is still valid.", func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("setupteardown-license-callhome-%d", i))...)
		}
		ValidateApplications(contexts)

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		currNode := node.GetWorkerNodes()[0]
		Step(fmt.Sprintf("Set License expiry timeout to 1 hour"), func() {
			err := Inst().V.SetClusterRunTimeOpts(currNode, map[string]string{
				"metering_interval_mins":       "10",
				"license_expiry_timeout_hours": "1",
			})
			Expect(err).NotTo(HaveOccurred())
		})

		Step(fmt.Sprintf("Disable call-home"), func() {
			err := Inst().V.ToggleCallHome(currNode, false)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("get all nodes and reboot one by one", func() {
			nodesToReboot := node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				for _, n := range nodesToReboot {
					if n.IsStorageDriverInstalled {
						Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
							err := Inst().N.RebootNode(n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
								},
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
							err := Inst().N.TestConnection(n, node.ConnectionOpts{
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

							err := Inst().S.IsNodeReady(n)
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

		Step("Wait 65 Minutes to make sure we passed the 1 hour mark and test if our license is still valid", func() {
			time.Sleep(65 * time.Minute)

			Step("Get SKU and compare with PX-Essentials FA/FB", func() {
				summary, err := Inst().V.GetLicenseSummary()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get license SKU. Error: [%v]", err))

				Expect(summary.SKU).To(Equal(essentialsFaFbSKU),
					fmt.Sprintf("SKU did not match: [%v]", essentialsFaFbSKU))

				Step("Compare PX-Essentials FA/FB features vs activated license", func() {
					for _, feature := range summary.Features {
						// if the feature limit exists in the hardcoded license limits we test it.
						if _, ok := faLicense[LabLabel(feature.Name)]; ok {
							Expect(feature.Quantity).To(Equal(faLicense[LabLabel(feature.Name)]),
								fmt.Sprintf("%v: %v did not match: [%v]", feature.Name, feature.Quantity, faLicense[LabLabel(feature.Name)]))
						}
					}
				})
			})
		})

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// LicenseValidation validates license summary against expected SKU and features
var _ = Describe("{LicenseValidation}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("LicenseValidation", "Validates license summary against expected SKU and features", nil, 0)
	})

	It("Validate license summary against expected SKU and features", func() {
		log.Infof("Validating license summary against expected SKU and features")
		err = ValidatePxLicenseSummary()
		log.FailOnError(err, "failed to validate license summary against expected SKU and features")
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

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
