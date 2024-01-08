package main

import (
	"fmt"
	"time"

	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/stats"
)

func main() {
	dashUtil := aetosutil.Get()
	rebootStats := &stats.NodeRebootStatsType{
		RebootTime: time.Now().Format("2006-01-02 15:04:05"),
		Node:       "test",
		PxVersion:  "3.0.4",
	}
	err := stats.PushStats(dashUtil, rebootStats)
	if err != nil {
		fmt.Printf("failed to create exportable stats")
	}

	dashStats := make(map[string]string)
	dashStats["node"] = "Test node"
	updateLongevityStats(dashUtil, "test-stats", "Node reboot", dashStats)
}

func updateLongevityStats(dash *aetosutil.Dashboard, name, eventStatName string, dashStats map[string]string) {
	eventStat := &stats.EventStat{
		EventName: eventStatName,
		EventTime: time.Now().Format(time.RFC1123),
		Version:   "3.1.0",
		DashStats: dashStats,
	}

	stats.PushStatsToAetos(dash, name, "px-base-enterprise", "Longevity", eventStat)

}
