package nomad

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/pkg/log"
	"golang.org/x/crypto/ssh"
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
	newClient := &NomadClient{client: client}
	status, err := newClient.ValidateCsiPluginStatus("portworx")
	if (status) && (err == nil) {
		log.Infof("Portworx CSI already registered")
	} else {
		err = newClient.EnablePortworxCsiPlugin("portworx")
		if err != nil {
			return nil, err
		}
		_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
			_, err := newClient.ValidateCsiPluginStatus("portworx")
			if err != nil {
				log.Infof("Retry on error: %v", err)
				return nil, true, err
			}
			return nil, false, nil
		}, 5*time.Minute, 30*time.Second)

		if err != nil {
			return nil, fmt.Errorf("Portworx CSI plugin did not become ready within the expected time: %v", err)
		}
	}
	return newClient, nil
}

// EnablePortworxCsiPlugin enables the CSI plugin for the Portworx job.
func (n *NomadClient) EnablePortworxCsiPlugin(jobName string) error {
	job, _, err := n.client.Jobs().Info(jobName, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch job info for %s: %v", jobName, err)
	}

	for _, group := range job.TaskGroups {
		if *group.Name == "portworx" {
			for _, task := range group.Tasks {
				if task.CSIPluginConfig == nil {
					task.CSIPluginConfig = &api.TaskCSIPluginConfig{
						ID:                  "portworx",
						Type:                api.CSIPluginTypeMonolith,
						MountDir:            "/var/lib/csi",
						HealthTimeout:       30 * time.Minute,
						StagePublishBaseDir: "/var/lib/portworx",
					}
				}

				// Add CSI_ENDPOINT environment variable
				if task.Env == nil {
					task.Env = make(map[string]string)
				}
				task.Env["CSI_ENDPOINT"] = "unix:///var/lib/csi/csi.sock"
			}
		}
	}

	_, _, err = n.client.Jobs().Register(job, nil)
	if err != nil {
		return fmt.Errorf("failed to update job with CSI plugin enabled: %v", err)
	}

	return nil
}

// ValidateCsiPluginStatus checks if the Portworx CSI plugin is properly configured and healthy.
func (n *NomadClient) ValidateCsiPluginStatus(pluginID string) (bool, error) {
	plugins, _, err := n.client.CSIPlugins().List(nil)
	if err != nil {
		return false, fmt.Errorf("failed to list CSI plugins: %v", err)
	}

	for _, plugin := range plugins {
		if plugin.ID == pluginID {
			expected := plugin.ControllersExpected
			healthy := plugin.ControllersHealthy

			if healthy == expected && expected > 0 {
				return true, nil
			}

			return false, fmt.Errorf("plugin found but controllers are not healthy/expected: %d/%d", healthy, expected)
		}
	}

	return false, fmt.Errorf("CSI plugin with ID %s not found", pluginID)
}

// CreateVolume creates a new CSI volume
func (n *NomadClient) CreateVolume(volumeID, pluginID string, capacityMin, capacityMax int64, accessMode, attachmentMode, fsType string, mountFlags []string, parameters map[string]string) error {
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
		Parameters: parameters,
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

	ipOfNode := strings.Split(node.HTTPAddr, ":")[0]
	client, err := ssh.Dial("tcp", net.JoinHostPort(ipOfNode, "22"), config)
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
func (n *NomadClient) CreateFioJobSpec(volumeID, jobID string, directories ...string) *api.Job {
	directory := "/mnt/fio-data"
	if len(directories) > 0 && directories[0] != "" {
		directory = "/mnt/" + directories[0]
	}
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
			"--filename=" + directory,
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

// ExecCommandOnPortworxNode Chooses a node and executes cmd on it
func (n *NomadClient) ExecCommandOnPortworxNode(cmd string) (string, error) {
	nodes, err := n.ListNodes()
	if err != nil {
		return "", err
	}
	// Assuming all nodes are Portworx Nodes
	node := nodes[0]
	return n.ExecCommandOnNodeSSH(node.ID, cmd)
}

// AdjustVolumeReplFactor adjusts the volume repl factor via pxctl
func (n *NomadClient) AdjustVolumeReplFactor(volumeID, newRepl string) error {
	cmd := fmt.Sprintf("pxctl volume ha-update --repl %s %s", newRepl, volumeID)
	output, err := n.ExecCommandOnPortworxNode(cmd)
	if err != nil {
		return fmt.Errorf("failed to adjust replica factor for volume %s: %v. Output: %s", volumeID, err, output)
	}
	log.Infof("Replica factor for volume %s adjusted to %s. Output: %s", volumeID, newRepl, output)
	return nil
}

// ResizeVolume manages the volume size
func (n *NomadClient) ResizeVolume(volumeID, newSize string) error {
	cmd := fmt.Sprintf("pxctl volume update --size %s %s", newSize, volumeID)
	output, err := n.ExecCommandOnPortworxNode(cmd)
	if err != nil {
		return fmt.Errorf("failed to modify size for volume %s: %v. Output: %s", volumeID, err, output)
	}
	log.Infof("Size for volume %s adjusted to %s. Output: %s", volumeID, newSize, output)
	return nil
}

// ValidateVolumeReplFactor method validates volume from pxctl and nomad nodes
func (n *NomadClient) ValidateVolumeReplFactor(volumeID, expectedRepl string) error {
	cmd := fmt.Sprintf("pxctl volume inspect %s", volumeID)

	output, err := n.ExecCommandOnPortworxNode(cmd)
	if err != nil {
		return fmt.Errorf("failed to get volume info for %s: %v", volumeID, err)
	}

	re := regexp.MustCompile(`HA\s+:\s+(\d+)`)
	matches := re.FindStringSubmatch(output)
	if matches == nil || len(matches) < 2 {
		return fmt.Errorf("could not find replica factor in volume info for %s. Output: %s", volumeID, output)
	}

	actualRepl := matches[1]
	if actualRepl != expectedRepl {
		return fmt.Errorf("replica factor for volume %s does not match expected value %s. Actual value: %s", volumeID, expectedRepl, actualRepl)
	}

	log.Infof("Volume %s replica factor validated successfully as %s", volumeID, expectedRepl)
	return nil
}

// ValidateVolumeSize method validates volume size via pxctl and nomad nodes
func (n *NomadClient) ValidateVolumeSize(volumeID, expectedSize string) error {
	cmd := fmt.Sprintf("pxctl volume inspect %s", volumeID)

	output, err := n.ExecCommandOnPortworxNode(cmd)
	if err != nil {
		return fmt.Errorf("failed to get volume info for %s: %v", volumeID, err)
	}

	re := regexp.MustCompile(`Size\s+:\s+(\d+)`)
	matches := re.FindStringSubmatch(output)
	if matches == nil || len(matches) < 2 {
		return fmt.Errorf("could not find replica factor in volume info for %s. Output: %s", volumeID, output)
	}

	actualSize := matches[1]
	if actualSize != expectedSize {
		return fmt.Errorf("size for volume %s does not match expected value %s. Actual value: %s", volumeID, expectedSize, actualSize)
	}

	log.Infof("Volume %s replica factor validated successfully as %s", volumeID, expectedSize)
	return nil
}

// FetchPoolUIDs fetches the UIDs of all storage pools on the specified node.
func (n *NomadClient) FetchPoolUIDs(nodeID string) ([]string, error) {
	cmd := "pxctl service pool show"
	output, err := n.ExecCommandOnNodeSSH(nodeID, cmd)
	if err != nil {
		return nil, fmt.Errorf("error executing '%s' on node %s: %v", cmd, nodeID, err)
	}

	return parsePoolUIDsFromOutput(output), nil
}

// parsePoolUIDsFromOutput parses the output of 'pxctl service pool show' to extract pool UIDs.
func parsePoolUIDsFromOutput(output string) []string {
	var poolUIDs []string
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "UUID:") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				poolUIDs = append(poolUIDs, parts[1])
			}
		}
	}
	return poolUIDs
}

// GeneratePXSecuritySecrets generates secrets for PX-Security and enables it for the Portworx job.
func (n *NomadClient) GeneratePXSecuritySecrets(jobName string, issuerName string) error {
	systemKey, err := generateSecureRandomString(64)
	if err != nil {
		return fmt.Errorf("failed to generate system key: %v", err)
	}

	sharedSecret, err := generateSecureRandomString(64)
	if err != nil {
		return fmt.Errorf("failed to generate shared secret: %v", err)
	}

	if issuerName == "" {
		issuerName = "portworx.io"
	}

	return n.EnablePxSecurity(jobName, systemKey, sharedSecret, issuerName)
}

// generateSecureRandomString generates a secure random string of the specified length.
func generateSecureRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bytes), nil
}

// EnablePxSecurity enables PX-Security for the Portworx job and waits for it to become healthy.
func (n *NomadClient) EnablePxSecurity(jobName string, systemKey, sharedSecret, issuerName string) error {
	job, _, err := n.client.Jobs().Info(jobName, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch job info for %s: %v", jobName, err)
	}

	foundPortworxGroup := false
	for _, group := range job.TaskGroups {
		for _, task := range group.Tasks {
			if task.Name == "portworx" {
				if task.Env == nil {
					task.Env = make(map[string]string)
				}
				task.Env["PORTWORX_AUTH_SYSTEM_KEY"] = systemKey
				task.Env["PORTWORX_AUTH_JWT_SHAREDSECRET"] = sharedSecret
				task.Env["PORTWORX_AUTH_JWT_ISSUER"] = issuerName
				foundPortworxGroup = true
				break
			}
		}
		if foundPortworxGroup {
			break
		}
	}

	if !foundPortworxGroup {
		return fmt.Errorf("portworx task not found in job %s", jobName)
	}

	_, _, err = n.client.Jobs().Register(job, nil)
	if err != nil {
		return fmt.Errorf("failed to update job with PX-Security enabled: %v", err)
	}

	return n.WaitForPortworxToBeHealthy(jobName)
}

// WaitForPortworxToBeHealthy waits until all allocations of the Portworx job are healthy.
func (n *NomadClient) WaitForPortworxToBeHealthy(jobName string) error {
	timeout := 10 * time.Minute
	startTime := time.Now()

	for {
		if time.Since(startTime) > timeout {
			return fmt.Errorf("timeout waiting for Portworx job %s to become healthy", jobName)
		}

		allocations, _, err := n.client.Jobs().Allocations(jobName, false, nil)
		if err != nil {
			return fmt.Errorf("error fetching allocations for job %s: %v", jobName, err)
		}

		allHealthy := true
		for _, alloc := range allocations {
			if alloc.ClientStatus != "running" {
				allHealthy = false
				break
			}
		}

		if allHealthy {
			return nil
		}

		time.Sleep(30 * time.Second)
	}
}
