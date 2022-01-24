package version

import (
	"fmt"
	"regexp"

	version "github.com/hashicorp/go-version"
	coreops "github.com/portworx/sched-ops/k8s/core"
)

// Version will be overridden with the current version at build time using the -X linker flag
var Version string

var (
	kbVerRegex = regexp.MustCompile(`^(v\d+\.\d+\.\d+)(.*)`)
)

// RequiresV1Registration returns true if crd nees to be registered as apiVersion V1
func RequiresV1Registration() (bool, error) {
	k8sVersion, _, err := GetFullVersion()
	if err != nil {
		return false, err
	}
	k8sVer1_16, err := version.NewVersion("1.16")
	if err != nil {
		return false, err

	}
	if k8sVersion.GreaterThanOrEqual(k8sVer1_16) {
		return true, nil
	}
	return false, nil
}

// GetFullVersion returns the full kubernetes server version
func GetFullVersion() (*version.Version, string, error) {
	k8sVersion, err := coreops.Instance().GetVersion()
	if err != nil {
		return nil, "", fmt.Errorf("unable to get kubernetes version: %v", err)
	}
	matches := kbVerRegex.FindStringSubmatch(k8sVersion.GitVersion)
	if len(matches) < 2 {
		return nil, "", fmt.Errorf("invalid kubernetes version received: %v", k8sVersion.GitVersion)
	}

	ver, err := version.NewVersion(matches[1])
	if len(matches) == 3 {
		return ver, matches[2], err
	}
	return ver, "", err
}

// GetVersion returns the kubernetes server version
func GetVersion() (*version.Version, error) {
	ver, _, err := GetFullVersion()
	return ver, err
}
