package stats

import (
	"encoding/json"
	"fmt"

	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
)

const (
	NodeRebootEventName          = "Node Reboot"
	PXRestartEventName           = "PX Restart"
	PXCrashEventName             = "PX-Storage Crash"
	PXDaemonCrashEventName       = "PX-Daemon Crash"
	HAIncreaseEventName          = "HA Increase"
	HADecreaseEventName          = "HA Decrease"
	AddDiskEventName             = "Add Disk"
	ResizeDiskEventName          = "Resize Disk"
	AddPoolEventName             = "New Pool Creation"
	DeletePoolEventName          = "Pool Deletion"
	CloudsnapEventName           = "Cloud Snapshot"
	CloudsnapRestorEventName     = "Cloud Snapshot Restore"
	LocalsnapEventName           = "Local Snapshot"
	LocalsnapRestorEventName     = "Local Snapshot Restore"
	DeployAppsEventName          = "Deploy Apps"
	DeletePodsEventName          = "Delete Pods"
	NodeCrashEventName           = "Node Crash"
	VolumeResizeEventName        = "Volume Resize"
	VolumeUpdateEventName        = "Volume Update"
	NodeRecycleEventName         = "Node Recycle"
	NodeScaleUpEventName         = "Node Scale Up"
	NodeDecommEventName          = "Node Decommission"
	NodeRejoinEventName          = "Node Rejoin"
	UpgradeStorkEventName        = "Upgrade Stork"
	UpgradeVolumeDriverEventName = "Upgrade Volume Driver"
	NodeMaintenanceEventName     = "Node Maintenance Cycle"
	PoolMaintenanceEventName     = "Pool Maintenance Cycle"
)

// Add more fields here if required
type NodeRebootStatsType struct {
	RebootTime string
	Node       string
	PxVersion  string
}

type EventStat struct {
	EventName string
	EventTime string
	Version   string
	DashStats map[string]string
}

func getRebootStats(rebootTime, nodeID, pxVersion string) (map[string]string, error) {
	rebootStats := &NodeRebootStatsType{
		RebootTime: rebootTime,
		Node:       nodeID,
		PxVersion:  pxVersion,
	}

	data, _ := json.Marshal(rebootStats)
	rebootExportable := make(map[string]string)
	json.Unmarshal(data, &rebootExportable)
	log.InfoD("Reboot Stats are: %v", rebootExportable)
	return rebootExportable, nil
}

func PushStats(dashUtils *aetosutil.Dashboard, eventType interface{}) error {
	var exportableData map[string]string
	var err error
	// TODO: implement this for all eventTypes not just reboots
	if obj, ok := eventType.(*NodeRebootStatsType); ok {
		//  TODO: Here exportableData.PxVersion may be replaced by the current release for which this is being run
		pxVersion := obj.PxVersion
		exportableData, err = getRebootStats(obj.RebootTime, obj.Node, pxVersion)
		if err != nil {
			return err
		}
		dashUtils.IsEnabled = true
		fmt.Printf("Pushing stats: %v", dashUtils.IsEnabled)
		dashUtils.UpdateStats("longevity", "SSIE", "reboot", pxVersion, exportableData)
	} else {
		fmt.Printf("Object not identified")
	}
	return nil
}

func PushStatsToAetos(dashUtils *aetosutil.Dashboard, name, product, statsType string, eventStat *EventStat) {
	data, err := json.Marshal(flattenDashStats(eventStat))
	if err != nil {
		log.Errorf("error marshalling event stat: %v ", err)
		return
	}
	var statsMap map[string]string
	err = json.Unmarshal(data, &statsMap)
	if err != nil {
		log.Errorf("error unmarshalling event stat: %v ", err)
		return
	}
	log.Infof("Stats are: %v", statsMap)
	dashUtils.UpdateStats(name, product, statsType, eventStat.Version, statsMap)
}

func flattenDashStats(eventStat *EventStat) map[string]string {
	flatMap := make(map[string]string)
	for k, v := range eventStat.DashStats {
		flatMap[k] = v
	}
	flatMap["EventName"] = eventStat.EventName
	flatMap["EventTime"] = eventStat.EventTime
	flatMap["Version"] = eventStat.Version

	return flatMap
}
