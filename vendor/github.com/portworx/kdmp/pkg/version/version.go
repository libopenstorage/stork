package version

import (
	"fmt"
	"runtime"
)

// Base version information.
//
// These variables typically come from -ldflags settings.
var (
	gitVersion = "raw"
	gitCommit  = ""                     // sha1 from git, output of $(git rev-parse HEAD)
	buildDate  = "1970-01-01T00:00:00Z" // build date in ISO8601 format, output of $(date -u +'%Y-%m-%dT%H:%M:%SZ')
)

// Info contains versioning information.
type Info struct {
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
		GitVersion: gitVersion,
		GitCommit:  gitCommit,
		BuildDate:  buildDate,
		GoVersion:  runtime.Version(),
		Compiler:   runtime.Compiler,
		Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
