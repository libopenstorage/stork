package preflight

import (
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	coreops "github.com/portworx/sched-ops/k8s/core"
)

var (
	instance CheckerOps
	once     sync.Once
)

type checker struct {
	providerName     string
	distributionName string
	k8sClient        client.Client
}

// CheckerOps is a list of APIs to do preflight check
type CheckerOps interface {
	// ProviderName returns the providerName of the cloud provider
	ProviderName() string

	// K8sDistributionName returns the k8s distribution providerName
	K8sDistributionName() string

	// CheckCloudDrivePermission checks if permissions for drive operation is granted
	CheckCloudDrivePermission(cluster *corev1.StorageCluster) error

	// SetProvider helps set the provider when that information is not available to Init
	SetProvider(string)
}

func (c *checker) ProviderName() string {
	return c.providerName
}

func (c *checker) SetProvider(providerName string) {
	c.providerName = providerName
}

func (c *checker) K8sDistributionName() string {
	return c.distributionName
}

func (c *checker) CheckCloudDrivePermission(cluster *corev1.StorageCluster) error {
	return nil
}

// Instance returns a singleton instance of preflight check
func Instance() CheckerOps {
	once.Do(func() {
		if instance == nil {
			instance = &checker{}
		}
	})
	return instance
}

// SetInstance replaces the instance with the provided one. Should be used only for testing purposes.
func SetInstance(i CheckerOps) {
	instance = i
}

// InitPreflightChecker initialize the preflight check instance
func InitPreflightChecker(client client.Client) error {
	providerName, err := getCloudProviderName()
	if err != nil {
		return err
	}
	distributionName, err := getK8sDistributionName()
	if err != nil {
		return err
	}

	c := &checker{
		providerName:     providerName,
		distributionName: distributionName,
		k8sClient:        client,
	}
	instance = c
	if IsEKS() {
		instance = &aws{
			checker: *c,
		}
	} else if IsGKE() {
		instance = &gce{
			checker: *c,
		}
	}

	return nil
}

func getCloudProviderName() (string, error) {
	nodeList, err := coreops.Instance().GetNodes()
	if err != nil {
		return "", err
	}

	for _, node := range nodeList.Items {
		// Get the cloud provider
		// From kubernetes node spec: <ProviderName>://<ProviderSpecificNodeID>
		if node.Spec.ProviderID != "" {
			tokens := strings.Split(node.Spec.ProviderID, "://")
			if len(tokens) == 2 {
				return tokens[0], nil
			} // else provider id is invalid
		}
	}
	return "", nil
}

func getK8sDistributionName() (string, error) {
	k8sVersion, err := coreops.Instance().GetVersion()
	if err != nil {
		return "", err
	}

	logrus.Infof("cluster is running k8s distribution %s", k8sVersion.String())
	if strings.Contains(strings.ToLower(k8sVersion.String()), eksDistribution) {
		return eksDistribution, nil
	} else if strings.Contains(strings.ToLower(k8sVersion.String()), gkeDistribution) {
		return gkeDistribution, nil
	}
	// TODO: detect other k8s distribution names
	return "", nil
}
