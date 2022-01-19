package version

import (
	"fmt"
	"regexp"
	"runtime"

	version "github.com/hashicorp/go-version"
	coreops "github.com/portworx/sched-ops/k8s/core"
)

// Base version information.
//
// These variables typically come from -ldflags settings.
var (
	gitVersion = "1.1.0"
	gitCommit  = ""                     // sha1 from git, output of $(git rev-parse HEAD)
	buildDate  = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
	kbVerRegex = regexp.MustCompile(`^(v\d+\.\d+\.\d+)(.*)`)
)

var major string
var minor string
var patch string

// Info contains versioning information.
type Info struct {
	Major      string `json:"major"`
	Minor      string `json:"minor"`
	Patch      string `json:"patch"`
	GitVersion string `json:"gitVersion"`
	GitCommit  string `json:"gitCommit"`
	BuildDate  string `json:"buildDate"`
	GoVersion  string `json:"goVersion"`
	Compiler   string `json:"compiler"`
	Platform   string `json:"platform"`
}

// String returns info as a human-friendly version string.
func (info Info) String() string {
	return fmt.Sprintf("%s-%s", info.GitVersion, info.GitCommit)
}

// Get returns the overall codebase version.
func Get() Info {
	return Info{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		GitVersion: gitVersion,
		GitCommit:  gitCommit,
		BuildDate:  buildDate,
		GoVersion:  runtime.Version(),
		Compiler:   runtime.Compiler,
		Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// ToString returns a string representation of the version
func ToString(i Info) string {
	return fmt.Sprintf("%v.%v.%v-%v", i.Major, i.Minor, i.Patch, i.GitCommit)
}

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
