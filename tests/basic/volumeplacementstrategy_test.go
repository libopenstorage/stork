package tests

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/k8s/talisman"
	"github.com/portworx/talisman/pkg/apis/portworx/v1beta1"
	"github.com/portworx/talisman/pkg/apis/portworx/v1beta2"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/portworx/torpedo/pkg/vpsutil"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{VolumePlacementStrategyFunctional}", func() {
	var testrailID, runID int
	var contexts []*scheduler.Context

	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)

		StartTorpedoTest("VolumePlacementStrategyFunctional", "Functional Tests for VPS", nil, testrailID)
	})

	Context("VolumePlacementStrategyValidation", func() {
		var vpsTestCase vpsutil.VolumePlaceMentStrategyTestCase

		testValidateVPS := func() {
			It("has to deploy VPS and validate the scheduled application follow specified rules", func() {
				Step("Deploying VPS", func() {
					log.InfoD("Deploy VPS for %v", vpsTestCase.TestName())
					err := vpsTestCase.DeployVPS()
					log.FailOnError(err, "Failed to Deploy VPS Spec")
				})

				Step("Deploy and Validate Applications", func() {
					log.InfoD("Deploy Applications")
					contexts = make([]*scheduler.Context, 0)
					for i := 0; i < Inst().GlobalScaleFactor; i++ {
						contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", vpsTestCase.TestName(), i))...)
					}
					log.InfoD("Validate Applications")
					ValidateApplications(contexts)
				})

				Step("Validate Deployment with VPS", func() {
					log.InfoD("Validate Deployment with VPS")
					err := vpsTestCase.ValidateVPSDeployment(contexts)
					log.FailOnError(err, "Failed to Validate Deployments with respect to VPS")
				})

				Step("Destroy VPS Deployment", func() {
					log.InfoD("Destroy VPS Deployment")
					err := vpsTestCase.DestroyVPSDeployment()
					log.FailOnError(err, "Failed to Destroy VPS Deployments")
				})

			})
		}

		// test mongo volume anti affinity
		Context("{VPSMongoVolumeAntiAffinity}", func() {
			BeforeEach(func() {
				vpsTestCase = &mongoVolumeAntiAffinity{}
			})
			testValidateVPS()
		})

		// test mongo volume anti affinity
		Context("{VPSMongVolumeoAffinity}", func() {
			BeforeEach(func() {
				vpsTestCase = &mongoVolumeAffinity{}
			})
			testValidateVPS()
		})
	})

	AfterEach(func() {
		Step("destroy apps", func() {
			log.InfoD("destroying apps")
			if CurrentGinkgoTestDescription().Failed {
				log.InfoD("not destroying apps because the test failed\n")
				return
			}
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{scheduler.OptionsWaitForResourceLeakCleanup: true})
			}

		})
	})

	AfterEach(func() {
		AfterEachTest(contexts, testrailID, runID)
		defer EndTorpedoTest()
	})
})

type VolumePlacementStrategySpec struct {
	spec *v1beta2.VolumePlacementStrategy
}

type mongoVolumeAntiAffinity struct {
	VolumePlacementStrategySpec
}

func (m *mongoVolumeAntiAffinity) TestName() string {
	return "mongovolumeantiaffinity"
}

func (m *mongoVolumeAntiAffinity) DeployVPS() error {

	matchExpression := []*v1beta1.LabelSelectorRequirement{
		{
			Key:      "px/statefulset-pod",
			Operator: v1beta1.LabelSelectorOpIn,
			Values:   []string{"${pvc.statefulset-pod}"},
		},
		{
			Key:      "app",
			Operator: v1beta1.LabelSelectorOpIn,
			Values:   []string{"mongo-sts"},
		},
	}

	vpsSpec := vpsutil.VolumeAntiAffinityByMatchExpression("mongo-vps", matchExpression)
	_, err := talisman.Instance().CreateVolumePlacementStrategy(&vpsSpec)
	m.spec = &vpsSpec
	return err
}

func (m *mongoVolumeAntiAffinity) DestroyVPSDeployment() error {
	return talisman.Instance().DeleteVolumePlacementStrategy(m.spec.Name)
}

// mongoVPSAntiAffinity is expecting to have deploy 2 replica of vol for each pod that has label [mongo-0, mongo-1]
// since this is antiaffinity, we are expecting that vol with the same labels are not deployed on the same pool/node.
// To validate that, we get the label from each deployed vol and extract the pool it's deployed on. if deployed correctly,
// there should be two pools per label.
func (m *mongoVolumeAntiAffinity) ValidateVPSDeployment(contexts []*scheduler.Context) error {
	vols, err := Inst().S.GetVolumes(contexts[0])
	if err != nil {
		return err
	}
	apiVols, err := getApiVols(vols)
	if err != nil {
		return err
	}

	volumeLabelKey := "px/statefulset-pod"
	expectedNodeLength := 2

	return vpsutil.ValidateVolumeAntiAffinityByNode(apiVols, volumeLabelKey, expectedNodeLength)
}

type mongoVolumeAffinity struct {
	VolumePlacementStrategySpec
}

func (m *mongoVolumeAffinity) TestName() string {
	return "mongovolumeaffinity"
}

func (m *mongoVolumeAffinity) DeployVPS() error {

	matchExpression := []*v1beta1.LabelSelectorRequirement{
		{
			Key:      "app",
			Operator: v1beta1.LabelSelectorOpIn,
			Values:   []string{"mongo-sts"},
		},
	}

	vpsSpec := vpsutil.VolumeAffinityByMatchExpression("mongo-vps", matchExpression)
	_, err := talisman.Instance().CreateVolumePlacementStrategy(&vpsSpec)
	m.spec = &vpsSpec
	return err
}

func (m *mongoVolumeAffinity) DestroyVPSDeployment() error {
	return talisman.Instance().DeleteVolumePlacementStrategy(m.spec.Name)
}

// mongoVolumeAffinity is expecting to have deploy 2 replica of vol for each pod that has label app=mongo-sts
// since this is affinity, we are expecting that vol with the same labels are not deployed on the same pool/node.
// to validate that, we get the label from each deployed vol and extracts the pool it's deployed on. if deployed correctly,
// there should be one pools per label only.
func (m *mongoVolumeAffinity) ValidateVPSDeployment(contexts []*scheduler.Context) error {
	vols, err := Inst().S.GetVolumes(contexts[0])
	if err != nil {
		return err
	}

	apiVols, err := getApiVols(vols)
	if err != nil {
		return err
	}

	volumeLabelKey := "app"

	return vpsutil.ValidateVolumeAffinityByNode(apiVols, volumeLabelKey)
}

func getApiVols(vols []*volume.Volume) ([]*api.Volume, error) {
	var apiVols []*api.Volume
	for _, vol := range vols {
		vol, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			return nil, err
		}
		apiVols = append(apiVols, vol)
	}
	return apiVols, nil
}
