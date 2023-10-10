package preflight

import (
	"github.com/libopenstorage/cloudops"
)

// IsEKS returns whether the cloud environment is running EKS
func IsEKS() bool {
	return Instance().ProviderName() == string(cloudops.AWS) && Instance().K8sDistributionName() == eksDistribution
}

// IsGKE returns whether the cloud environment is running GKE
func IsGKE() bool {
	return Instance().ProviderName() == string(cloudops.GCE) && Instance().K8sDistributionName() == gkeDistribution
}

// RequiresCheck returns whether a preflight check is needed based on the platform
func RequiresCheck() bool {
	return Instance().ProviderName() == string(cloudops.AWS) ||
		Instance().ProviderName() == string(cloudops.Vsphere) ||
		Instance().ProviderName() == string(cloudops.Pure)
}

// RunningOnCloud checks whether portworx is running on cloud
func RunningOnCloud() bool {
	// TODO: add other clouds
	return IsEKS()
}
