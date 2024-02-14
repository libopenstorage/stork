package nomad

import (
	"fmt"
	"github.com/hashicorp/nomad/api"
	"github.com/portworx/torpedo/pkg/log"
	"golang.org/x/crypto/ssh"
	"net"
)

// NomadClient defines structure for nomad client
type NomadClient struct {
	client *api.Client
}

// NewNomadClient creates and returns a new client for nomad cluster
func NewNomadClient() (*NomadClient, error) {
	config := api.DefaultConfig()
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &NomadClient{client: client}, nil
}

// CreateVolume creates a new CSI volume
func (n *NomadClient) CreateVolume(volumeID, pluginID string, capacityMin, capacityMax int64, accessMode, attachmentMode, fsType string, mountFlags []string) error {
	volume := &api.CSIVolume{
		ID:                   volumeID,
		Name:                 volumeID,
		PluginID:             pluginID,
		RequestedCapacityMin: capacityMin,
		RequestedCapacityMax: capacityMax,
		MountOptions: &api.CSIMountOptions{
			FSType:     fsType,
			MountFlags: mountFlags,
		},
		RequestedCapabilities: []*api.CSIVolumeCapability{
			{
				AccessMode:     api.CSIVolumeAccessMode(accessMode),
				AttachmentMode: api.CSIVolumeAttachmentMode(attachmentMode),
			},
		},
	}

	_, _, err := n.client.CSIVolumes().Create(volume, nil)
	return err
}

// ListNodes returns all nodes in the cluster
func (n *NomadClient) ListNodes() ([]*api.NodeListStub, error) {
	nodes, _, err := n.client.Nodes().List(nil)
	return nodes, err
}

// VolumeInfo returns volume object for a given volume ID
func (n *NomadClient) VolumeInfo(volumeID string) (*api.CSIVolume, error) {
	volume, _, err := n.client.CSIVolumes().Info(volumeID, nil)
	if err != nil {
		return nil, err
	}
	return volume, nil
}

// DeleteVolume deletes a volume with given Volume ID
func (n *NomadClient) DeleteVolume(volumeID string) error {
	err := n.client.CSIVolumes().Delete(volumeID, nil)
	if err != nil {
		return err
	}
	return nil
}

// ListAllTasks lists all tasks in all allocations in the Nomad cluster
func (n *NomadClient) ListAllTasks() (map[string][]*api.Task, error) {
	tasksMap := make(map[string][]*api.Task)

	jobs, _, err := n.client.Jobs().List(nil)
	if err != nil {
		return nil, err
	}

	for _, job := range jobs {
		jobID := job.ID

		allocs, _, err := n.client.Jobs().Allocations(jobID, true, nil)
		if err != nil {
			return nil, err
		}

		for _, alloc := range allocs {
			allocInfo, _, err := n.client.Allocations().Info(alloc.ID, nil)
			if err != nil {
				return nil, err
			}

			for _, taskGroup := range allocInfo.Job.TaskGroups {
				if taskGroup == nil {
					continue
				}

				for _, task := range taskGroup.Tasks {
					tasksMap[jobID] = append(tasksMap[jobID], task)
				}
			}
		}
	}

	return tasksMap, nil
}

// CheckJobAllocHealth validates if all allocations in a job are in running state or not
func (n *NomadClient) CheckJobAllocHealth(jobID string) (bool, error) {
	allocs, _, err := n.client.Jobs().Allocations(jobID, true, nil)
	if err != nil {
		return false, err
	}

	for _, alloc := range allocs {
		if alloc.ClientStatus != "running" {
			return false, nil
		}
	}
	return true, nil
}

// ListAllocations returns all allocations in a given job ID
func (n *NomadClient) ListAllocations(jobID string) ([]*api.AllocationListStub, error) {
	allocs, _, err := n.client.Jobs().Allocations(jobID, false, nil)
	if err != nil {
		return nil, err
	}
	return allocs, nil
}

// ExecCommandOnNodeSSH executes a command on a node in Nomad Cluster
func (n *NomadClient) ExecCommandOnNodeSSH(nodeID, command string) (string, error) {
	SSHPassword := "Password1"
	node, _, err := n.client.Nodes().Info(nodeID, nil)
	if err != nil {
		return "", err
	}

	config := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.Password(SSHPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	client, err := ssh.Dial("tcp", net.JoinHostPort(node.HTTPAddr, "22"), config)
	if err != nil {
		return "", fmt.Errorf("failed to dial: %s", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %s", err)
	}
	defer session.Close()

	output, err := session.CombinedOutput(command)
	if err != nil {
		return "", fmt.Errorf("failed to run command: %s, output: %s", err, output)
	}

	return string(output), nil
}

// CreateFioJobSpec creates the FIO Job spec
func (n *NomadClient) CreateFioJobSpec(volumeID, jobID string) *api.Job {
	job := api.NewServiceJob(jobID, "fio", "global", 1)
	taskGroup := api.NewTaskGroup("fio-group", 1)
	volume := &api.VolumeRequest{
		Name:           "fio-volume",
		Type:           "csi",
		Source:         volumeID,
		ReadOnly:       false,
		AccessMode:     "multi-node-multi-writer",
		AttachmentMode: "file-system",
	}
	taskGroup.Volumes = map[string]*api.VolumeRequest{
		"fio-volume": volume,
	}
	task := api.NewTask("fio-task", "docker")
	task.Config = map[string]interface{}{
		"image": "xridge/fio:latest",
		"args": []string{
			"--name=randwrite",
			"--ioengine=libaio",
			"--iodepth=4",
			"--rw=randrw",
			"--bs=4k",
			"--size=1024M",
			"--numjobs=1",
			"--time_based",
			"--runtime=1800",
			"--filename=/mnt/fio-data",
		},
	}
	task.Resources = &api.Resources{
		CPU:      Ptr(1000),
		MemoryMB: Ptr(2048),
	}
	task.VolumeMounts = []*api.VolumeMount{
		{
			Volume:      Ptr("fio-volume"),
			Destination: Ptr("/mnt"),
		},
	}
	taskGroup.Tasks = []*api.Task{task}
	job.AddTaskGroup(taskGroup)
	return job
}

// Ptr is a helper method to convert to pointer type objects
func Ptr[T any](v T) *T {
	return &v
}

// CreateJob creates a job in Nomad cluster
func (n *NomadClient) CreateJob(job *api.Job) error {
	_, _, err := n.client.Jobs().Register(job, nil)
	return err
}

// DeleteJob deletes a job from Nomad
func (n *NomadClient) DeleteJob(jobID string) error {
	_, _, err := n.client.Jobs().Deregister(jobID, true, nil)
	return err
}

// CheckJobStatus checks the status of a job in Nomad
func (n *NomadClient) CheckJobStatus(jobID string) (string, error) {
	allocs, _, err := n.client.Jobs().Allocations(jobID, false, nil)
	if err != nil {
		return "", err
	}
	for _, alloc := range allocs {
		if alloc.ClientStatus != "running" {
			log.Infof("Job ID: %v , Allocation Status is: %v", jobID, alloc.ClientStatus)
			return "not running", nil
		}
		for _, taskState := range alloc.TaskStates {
			if taskState.State != "running" {
				log.Infof("Job ID: %v, Task Status is : %v", jobID, taskState.State)
				return "not running", nil
			}
		}
	}
	return "running", nil
}

// ScaleJob scales a job with job ID to given count
func (n *NomadClient) ScaleJob(jobID string, count int) error {
	job, _, err := n.client.Jobs().Info(jobID, nil)
	if err != nil {
		return err
	}
	job.TaskGroups[0].Count = Ptr(count)
	_, _, err = n.client.Jobs().Register(job, nil)
	return err
}

// ValidateScaling method validates if a job is running with expected scale count or not
func (n *NomadClient) ValidateScaling(jobID string, expectedCount int) error {
	allocs, _, err := n.client.Jobs().Allocations(jobID, false, nil)
	if err != nil {
		return fmt.Errorf("failed to get job allocations: %v", err)
	}
	var runningCount int
	for _, alloc := range allocs {
		if alloc.ClientStatus == "running" {
			runningCount++
		}
	}
	if runningCount != expectedCount {
		return fmt.Errorf("scaling validation failed: expected %d, got %d running instances", expectedCount, runningCount)
	}
	log.Infof("Scaling validation successful: %d instances running", runningCount)
	return nil
}

// CreateSnapshot creates a new snapshot of a CSI volume
func (n *NomadClient) CreateSnapshot(volumeID, name, pluginID string, secrets map[string]string) (*api.CSISnapshotCreateResponse, error) {
	snap := &api.CSISnapshot{
		SourceVolumeID: volumeID,
		Name:           name,
		Secrets:        secrets,
		PluginID:       pluginID,
	}
	resp, _, err := n.client.CSIVolumes().CreateSnapshot(snap, nil)
	if err != nil {
		return nil, err
	}
	if len(resp.Snapshots) > 0 {
		return resp, nil
	}
	return nil, fmt.Errorf("no snapshot created")
}

// ListSnapshots lists all snapshots in the cluster
func (n *NomadClient) ListSnapshots(pluginID string) ([]*api.CSISnapshot, error) {
	queryOptions := &api.QueryOptions{}
	snapshotListResponse, _, err := n.client.CSIVolumes().ListSnapshots(pluginID, "", queryOptions)
	if err != nil {
		return nil, err
	}
	snapshots := snapshotListResponse.Snapshots
	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot
func (n *NomadClient) DeleteSnapshot(snapID, pluginID string) error {
	snap := &api.CSISnapshot{
		ID:       snapID,
		PluginID: pluginID,
	}
	err := n.client.CSIVolumes().DeleteSnapshot(snap, nil)
	return err
}
