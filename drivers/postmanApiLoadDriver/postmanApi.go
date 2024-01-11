package postmanApiLoadDriver

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"path/filepath"
	"strings"
)

const (
	defaultCollectionPath = "../drivers/postmanApiLoadDriver/collections/collection.json"
	PxDataServices        = "pds"
)

// PostmanDriver Struct to define variables to pass to the newman run command
type PostmanDriver struct {
	ResultsFileName string
	ResultType      string
	Iteration       string
	Kubeconfig      string
}

// GetProjectNameToExecutePostman MAIN driver function which will decide which project to run
func GetProjectNameToExecutePostman(projectName string, driver *PostmanDriver) error {
	if projectName == PxDataServices {
		_, err := ExecutePostmanCommandInTorpedoForPDS(driver)
		if err != nil {
			return err
		}
	}
	//ToDo: Add cases for other PX Projects
	return nil
}

// GetPostmanCollectionPath to check if collection is present in the folder
func GetPostmanCollectionPath() (string, error) {
	postmanCollectionFile, err := filepath.Abs(defaultCollectionPath)
	if err != nil {
		return "", fmt.Errorf("postman Collection Json not found, Please create a Collection json manually and export to {%v} folder", defaultCollectionPath)
	}
	log.InfoD("PostmanCollectionFile found is- [%v]", postmanCollectionFile)
	return postmanCollectionFile, nil
}

// ExecuteCommandInShell to execute commands in shell
func ExecuteCommandInShell(command string) (string, string, error) {
	out, res, err := osutils.ExecShell(command)
	if err != nil {
		return "", "", err
	}
	return out, res, nil
}

// ExecutePostmanCommandInTorpedoForPDS execute PDS specific collections
func ExecutePostmanCommandInTorpedoForPDS(postmanParams *PostmanDriver) (bool, error) {
	collectionPath, err := GetPostmanCollectionPath()
	if err != nil {
		return false, fmt.Errorf("postman collection json not found- [%v], Please create a Collection json manually and export to defaultCollectionPath folder", err)
	}
	log.InfoD("Postman Collection found is- %v", collectionPath)

	iterations := postmanParams.Iteration
	newmanCmd := "newman run " + collectionPath + " -n " + iterations + " --verbose"
	log.InfoD("Newman command formed is- [%v]", newmanCmd)
	output, res, err := ExecuteCommandInShell(newmanCmd)
	if err != nil {
		return false, fmt.Errorf("there was some problem in executing Postman Newman container due to- [%v]", err)
	}
	log.InfoD("output from the newman execution is- %v", res)
	if strings.Contains(output, "failure") {
		return false, fmt.Errorf("newman exited with a failure.. [%v], Please check logs for more details", err)
	}
	return true, nil
}
