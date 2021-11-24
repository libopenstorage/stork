package version

import (
	"fmt"
	"runtime"
)

// Base version information.
//
// These variables typically come from -ldflags settings.
var (
	gitVersion = "1.0.0"
	gitCommit  = ""                     // sha1 from git, output of $(git rev-parse HEAD)
	buildDate  = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
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
