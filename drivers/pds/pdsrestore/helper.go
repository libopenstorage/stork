package pdsrestore

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"math/rand"
	"sync"
	"time"
)

const NameSuffixLength = 3

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu     sync.Mutex
)

const lowerAlphaNumeric = "abcdefghijklmnopqrstuvwxyz0123456789"

func AlphaNumericString(length int) string {
	mu.Lock()
	defer mu.Unlock()

	result := make([]uint8, length)
	for i := range result {
		result[i] = lowerAlphaNumeric[random.Intn(len(lowerAlphaNumeric))]
	}
	return string(result)
}

func generateRandomName(prefix string) string {
	nameSuffix := AlphaNumericString(NameSuffixLength)
	return fmt.Sprintf("%s-systest-%s", prefix, nameSuffix)
}

func resourceStructToMap(resources *pds.ModelsDeploymentResources) map[string]interface{} {
	resourceMap := make(map[string]interface{})
	if resources.CpuLimit != nil {
		resourceMap["cpu_limit"] = resources.GetCpuLimit()
	}
	if resources.CpuRequest != nil {
		resourceMap["cpu_request"] = resources.GetCpuRequest()
	}
	if resources.MemoryLimit != nil {
		resourceMap["memory_limit"] = resources.GetMemoryLimit()
	}
	if resources.MemoryRequest != nil {
		resourceMap["memory_request"] = resources.GetMemoryRequest()
	}
	if resources.StorageRequest != nil {
		resourceMap["storage_request"] = resources.GetStorageRequest()
	}
	return resourceMap
}

func storageOptionsStructToMap(storageOptions *pds.ModelsDeploymentStorageOptions) map[string]interface{} {
	storageMap := make(map[string]interface{})
	if storageOptions.Fg != nil {
		storageMap["fg"] = storageOptions.GetFg()
	}
	if storageOptions.Fs != nil {
		storageMap["fs"] = storageOptions.GetFg()
	}
	if storageOptions.Repl != nil {
		storageMap["repl"] = storageOptions.GetRepl()
	}
	if storageOptions.Secure != nil {
		storageMap["secure"] = storageOptions.GetSecure()
	}
	return storageMap
}
