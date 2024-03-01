package buildinfo

import "os"

type BuildInfo struct {
	BuildNumber string
	Job         string
	URL         string
}

func OnJenkins() bool {
	return os.Getenv("BUILD_NUMBER") != ""
}

func GetBuildInfo() BuildInfo {
	jobName := os.Getenv("JOB_BASE_NAME")
	if jobName == "" {
		jobName = os.Getenv("JOB_NAME")
	}
	return BuildInfo{
		BuildNumber: os.Getenv("BUILD_NUMBER"),
		Job:         jobName,
		URL:         os.Getenv("BUILD_URL"),
	}
}
