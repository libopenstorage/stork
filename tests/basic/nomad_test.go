package tests

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/nomad/api"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/nomad"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{ListNodesOnNomadCluster}", func() {
	var client *nomad.NomadClient
	var err error
	JustBeforeEach(func() {
		StartTorpedoTest("ListNodesOnNomadCluster", "List nodes in Nomad Cluster", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
	})
	It("List and print Nodes", func() {
		nodes, err := client.ListNodes()
		log.FailOnError(err, "Failed to list nodes")
		for _, node := range nodes {
			log.Infof("Nodename present : %v", node.Name)
			out, err := client.ExecCommandOnNodeSSH(node.ID, "hostname")
			log.FailOnError(err, "Some error came")
			log.Infof("Output of running hostname on %v is : %v", node.Name, out)
		}
	})
})

var _ = Describe("{CreateAndValidateNomadVolume}", func() {
	var client *nomad.NomadClient
	var err error
	var volumes []*api.CSIVolume

	BeforeEach(func() {
		StartTorpedoTest("CreateAndValidateNomadVolume", "Create and validate a volume in Nomad", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
	})

	It("Should create a volume and validate it's active", func() {
		volumeID := fmt.Sprintf("test-volume-%v", time.Now().Unix())
		pluginID := "portworx"
		capacityMin := int64(1 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "single-node-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "2",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err := client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume")

		// Validate the volume
		volume, err := client.VolumeInfo(volumeID)
		log.FailOnError(err, "Failed to get volume info")
		volumes = append(volumes, volume)
		if volume.ID == volumeID {
			log.Infof("Volume %v created and validated successfully", volumeID)
		} else {
			log.Errorf(" Volume %v not created successfully")
		}
	})
	JustAfterEach(func() {
		for _, volume := range volumes {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volume.ID)
				if err != nil {
					log.Errorf("Retry error: Volume %v could not be deleted with err %v", volume.ID, err)
					return nil, true, err // true indicates a retry should be attempted
				}
				return nil, false, nil // false indicates no further retries are needed
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: Volume %v could not be deleted after retries: %v", volume.ID, err)
			log.Infof("Successfully deleted volume %v after retries", volume.ID)
		}
	})
})

var _ = Describe("{RunFioJobOnNomad}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID string
	var pluginID string

	BeforeEach(func() {
		StartTorpedoTest("RunFioJobOnNomad", "Runs FIO Job on Nomad Cluster", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "single-node-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "2",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}
		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume")
		log.Infof("Volume %v created successfully", volumeID)
		jobID := "fio-job"
		fioJob := client.CreateFioJobSpec(volumeID, jobID)
		err = client.CreateJob(fioJob)
		log.FailOnError(err, "Failed to create Fio job")
		log.Infof("Fio job %v is created successfully", fioJob.ID)
		log.Infof("Sleeping for 10 seconds to absorb startup time of allocation")
		time.Sleep(10 * time.Second)
	})
	It("Run Fio job defined above and monitor its execution", func() {
		const checkInterval = time.Minute * 1
		const runDuration = time.Minute * 10
		endTime := time.Now().Add(runDuration)
		for time.Now().Before(endTime) {
			status, err := client.CheckJobStatus("fio")
			log.FailOnError(err, "Failed to check Fio job status")
			if status != "running" {
				log.Errorf("Fio job is not running")
				return
			}
			log.Infof("Allocation and fio-task is running successfully as of now. Will check in 1 minute again.")
			time.Sleep(checkInterval)
		}
		log.Infof("Fio ran successfully for the duration of the test")
	})
	AfterEach(func() {
		err = client.DeleteJob("fio")
		if err != nil {
			log.Errorf("Failed to delete Fio job: %v", err)
		} else {
			log.Infof("Successfully deleted Job fio")
		}
		err = client.DeleteVolume(volumeID)
		if err != nil {
			log.Errorf("Failed to delete volume with error: %v. Will retry in 5 more seconds", err)
			time.Sleep(5 * time.Second)
			err = client.DeleteVolume(volumeID)
			if err != nil {
				log.Errorf("Again failed to delete volume %v with err %v", volumeID, err)
			} else {
				log.Infof("Successfully deleted volume %v this time", volumeID)
			}
		} else {
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{RunMultipleFioJobsOnNomad}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeIDs []string
	var jobIDs []string
	var volumeID string
	var jobID string
	const numJobs = 3
	pluginID := "portworx"

	BeforeEach(func() {
		StartTorpedoTest("RunMultipleFioJobsOnNomad", "Runs multiple FIO Jobs on Nomad Cluster", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "single-node-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		for i := 0; i < numJobs; i++ {
			volumeID = fmt.Sprintf("fio-test-volume-%d-%v", i, time.Now().Unix())
			jobID = fmt.Sprintf("fio-job-%d", i)
			volumeIDs = append(volumeIDs, volumeID)
			log.Infof("Going ahead to create volume with ID: %v", volumeID)
			err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
			log.FailOnError(err, "Failed to create volume")
			log.Infof("Volume %v created successfully", volumeID)
			fioJob := client.CreateFioJobSpec(volumeID, jobID)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job")
			log.Infof("Fio job %v is created successfully", jobID)
			log.Infof("Sleeping for 10 seconds to absorb startup time of allocation")
			time.Sleep(10 * time.Second)
			jobIDs = append(jobIDs, jobID)
			log.Infof("Created volume and Fio job: %v", jobID)
		}

		log.Infof("Sleeping for 10 seconds to allow allocations to start")
		time.Sleep(10 * time.Second)
	})

	It("Run multiple Fio jobs and monitor their execution", func() {
		const runDuration = time.Minute * 10
		endTime := time.Now().Add(runDuration)
		statusChan := make(chan string, numJobs)
		for _, jobID := range jobIDs {
			go func(jID string) {
				for time.Now().Before(endTime) {
					status, err := client.CheckJobStatus(jID)
					if err != nil || status != "running" {
						statusChan <- fmt.Sprintf("Job %s failed or stopped running", jID)
						return
					}
					log.Infof("Will check again in 1 minute for %v", jID)
					time.Sleep(time.Minute * 1)
				}
				statusChan <- fmt.Sprintf("Job %s completed successfully", jID)
			}(jobID)
		}
		for i := 0; i < numJobs; i++ {
			msg := <-statusChan
			log.Infof(msg)
		}
		close(statusChan)
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete Fio job %v: %v", jobID, err)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v: %v", volumeID, err)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{ScaleFioJobOnNomad}", func() {
	var client *nomad.NomadClient
	var err error
	var jobID string
	var volumeID string
	var pluginID string

	BeforeEach(func() {
		StartTorpedoTest("ScaleFioJobOnNomad", "Scale FIO Job in Nomad", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume")
		log.Infof("Volume %v created successfully", volumeID)
		jobID = "fio-job"
		fioJob := client.CreateFioJobSpec(volumeID, jobID)
		err = client.CreateJob(fioJob)
		log.FailOnError(err, "Failed to create Fio job")
		log.Infof("FIO Job %v created successfully", jobID)
		log.Infof("Sleeping for 10 secodns to let the pre-reqs of job complete (image download, etc)")
		time.Sleep(10 * time.Second)
	})

	It("Scale FIO job up and down", func() {
		// Scale up
		log.Infof("Trying to scale up the %v to 3 allocations now", jobID)
		err = client.ScaleJob(jobID, 3) // Scale to 3 instances
		log.FailOnError(err, "Failed to scale up the job")
		log.Infof("Hard sleep for 10 seconds to let scaling start (Nomad issue)")
		time.Sleep(10 * time.Second)
		err = client.ValidateScaling(jobID, 3) // Validate scaling up
		log.FailOnError(err, "Failed to scale up to 3 allocations for fio job with error")
		log.Infof("Successfully scaled up to 3 allocations. Now letting fio job run for 1 minute")
		time.Sleep(1 * time.Minute)
		status, err := client.CheckJobStatus(jobID)
		log.FailOnError(err, "Failed to check Fio job status")
		if status != "running" {
			log.Errorf("Fio job is not running. Test Failed.")
			return
		}
		log.Infof("Fio job is still running as expected")
		// Scale down
		log.Infof("Trying to scale down to 1 allocation again")
		err = client.ScaleJob(jobID, 1) // Scale back to 1 instance
		log.FailOnError(err, "Failed to scale down the job")
		log.Infof("Hard sleep for 10 seconds to let scaling start (Nomad issue)")
		time.Sleep(10 * time.Second)
		err = client.ValidateScaling(jobID, 1) // Validate scaling down
		log.FailOnError(err, "Failed to scale down to 1 allocation for fio job")
		log.Infof("Successfully scaled down to 1 allocation. Will let the FIO job run for 1 minute and exit the test")
		status, err = client.CheckJobStatus(jobID)
		log.FailOnError(err, "Failed to check Fio job status")
		if status != "running" {
			log.Errorf("Fio job is not running. Test Failed.")
			return
		}
		log.Infof("Allocation and fio-task is running successfully. Test Passed.")
	})

	AfterEach(func() {
		err = client.DeleteJob(jobID)
		log.FailOnError(err, "Failed to delete job %v", jobID)
		log.Infof("Successfully delete job: %v", jobID)

		err = client.DeleteVolume(volumeID)
		if err != nil {
			log.Errorf("Failed to delete volume: %v. Will retry in 5 more seconds.", err)
			time.Sleep(5 * time.Second)
			err = client.DeleteVolume(volumeID)
			if err != nil {
				log.Errorf("Again failed to delete volume %v with err %v", volumeID, err)
			} else {
				log.Infof("Successfully deleted volume %v this time", volumeID)
			}
		} else {
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{RunFioJobWithSnapshotOperationsOnNomad}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, snapshotID, jobID string
	var pluginID string
	BeforeEach(func() {
		StartTorpedoTest("RunFioJobWithSnapshotOperationsOnNomad", "Runs FIO Job on Nomad Cluster with Snapshot Operations", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		snapshotID = "fio-snapshot"
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume")
		log.Infof("Volume %v created successfully", volumeID)
		jobID = "fio-job"
		fioJob := client.CreateFioJobSpec(volumeID, jobID)
		err = client.CreateJob(fioJob)
		log.FailOnError(err, "Failed to create Fio job")
		log.Infof("FIO Job %v created successfully. Letting it run for 30 seconds before triggering snapshot", jobID)
		time.Sleep(30 * time.Second)
	})
	It("Run Fio job and perform snapshot operations", func() {
		createdSnap, err := client.CreateSnapshot(volumeID, "mySnapshot", pluginID, nil)
		log.FailOnError(err, "Failed to create snapshot")
		log.Infof("Snapshot created successfully. Sleeping for 5 seconds to let it complete")
		time.Sleep(5 * time.Second)
		snapshots, err := client.ListSnapshots(pluginID)
		log.FailOnError(err, "Failed to list snapshots")
		log.Infof("Snapshots found are: %v", snapshots)
		found := false
		for _, snap := range snapshots {
			if snap.ID == createdSnap.Snapshots[0].ID {
				log.Infof("Found snapshot: %v", snapshotID)
				found = true
				break
			}
		}
		if !found {
			log.FailOnError(fmt.Errorf("Snapshot %v not found", snapshotID), "Error in finding snpashot")
		}
		log.Infof("Letting FIO run for 1 more minute before deleting snapshot")
		const checkInterval = time.Minute * 1
		const runDuration = time.Minute * 1
		endTime := time.Now().Add(runDuration)
		for time.Now().Before(endTime) {
			status, err := client.CheckJobStatus(jobID)
			log.FailOnError(err, "Failed to check Fio job status")
			if status != "running" {
				log.Errorf("Fio job is not running")
				return
			}
			time.Sleep(checkInterval)
		}
		log.Infof("Fio ran successfully for the duration of the test")
		err = client.DeleteSnapshot(createdSnap.Snapshots[0].ID, pluginID)
		log.FailOnError(err, "Failed to delete snapshot")
		log.Infof("Successfully deleted snapshot with ID: %v", createdSnap.Snapshots[0].ID)
	})
	AfterEach(func() {
		err = client.DeleteJob(jobID)
		if err != nil {
			log.Errorf("Failed to delete Fio job: %v", err)
		} else {
			log.Infof("Successfully deleted job %v", jobID)
		}
		err = client.DeleteVolume(volumeID)
		if err != nil {
			log.Errorf("Failed to delete volume: %v. Will retry in 5 seconds", err)
			time.Sleep(5 * time.Second)
			err = client.DeleteVolume(volumeID)
			if err != nil {
				log.Errorf("Again failed to delete volume %v with err %v", volumeID, err)
			} else {
				log.Infof("Successfully deleted volume %v this time", volumeID)
			}
		} else {
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{RunMultipleFioJobsOnSharedRWXVolume}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string
	var jobIDs []string
	var volumeIDs []string
	var directories []string = []string{"job1", "job2", "job3"}

	BeforeEach(func() {
		StartTorpedoTest("RunMultipleFioJobsOnSharedRWXVolume", "Runs Multiple FIO Jobs on Nomad Cluster", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")
		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(50 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create shared volume")
		log.Infof("Shared volume %v created successfully", volumeID)
		volumeIDs = append(volumeIDs, volumeID)
		for _, directory := range directories {
			jobID := fmt.Sprintf("fio-%s", directory)
			fioJob := client.CreateFioJobSpec(volumeID, jobID, directory)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job for directory "+directory)
			jobIDs = append(jobIDs, jobID)

			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				status, err := client.CheckJobStatus(jobID)
				if err != nil {
					return nil, true, fmt.Errorf("error checking job status for %s: %v", jobID, err)
				}
				if status == "running" {
					return nil, false, nil
				}
				return nil, true, fmt.Errorf("job %s is not running yet", jobID)
			}, 5*time.Minute, 10*time.Second)

			log.FailOnError(err, fmt.Sprintf("Failed to wait for job %s to be running", jobID))
		}
		log.Infof("All jobs are created and running")
	})

	It("Should run multiple Fio jobs on a shared RWX volume", func() {
		var wg sync.WaitGroup
		const runDuration = time.Minute * 10
		endTime := time.Now().Add(runDuration)

		for _, jobID := range jobIDs {
			wg.Add(1)
			go func(jID string) {
				defer wg.Done()
				for time.Now().Before(endTime) {
					status, err := client.CheckJobStatus(jID)
					log.Infof("Status of Job %s is %v", jID, status)
					if err != nil || status != "running" {
						log.Errorf("Job %s failed or stopped running", jID)
						return
					}
					log.Infof("Job %s is still running. Next check in 1 minute.", jID)
					time.Sleep(time.Minute)
				}
				log.Infof("Job %s completed successfully", jID)
			}(jobID)
		}
		wg.Wait()
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)

			log.FailOnError(err, "Final error: failed to delete Fio job %v", jobID)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v", volumeID)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{KillPxWhileAppsAreRunning}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string
	var volumeIDs []string
	var jobIDs []string
	var directories = []string{"job1", "job2", "job3"}

	BeforeEach(func() {
		StartTorpedoTest("KillPxWhileAppsAreRunning", "Runs Multiple FIO Jobs on Nomad Cluster and restarts Portworx", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(50 * 1024 * 1024 * 1024) // 50GB
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create shared volume")
		log.Infof("Shared volume %v created successfully", volumeID)
		volumeIDs = append(volumeIDs, volumeID)

		for _, directory := range directories {
			jobID := fmt.Sprintf("fio-%s", directory)
			jobIDs = append(jobIDs, jobID)
			fioJob := client.CreateFioJobSpec(volumeID, jobID, directory)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job for directory "+directory)
		}
		log.Infof("Sleeping for 10 seconds to allow allocations to start")
		time.Sleep(10 * time.Second)

		for _, jobID := range jobIDs {
			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					return nil, true, fmt.Errorf("job %s is not running yet", jobID)
				}
				return nil, false, nil
			}, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Failed to wait for job to be in running state")
		}
	})

	It("Should run multiple Fio jobs on a shared RWX volume and handle Portworx restart", func() {
		// Kill Portworx on one of the nodes
		nodes, err := client.ListNodes()
		log.FailOnError(err, "Failed to list nodes")
		if len(nodes) > 0 {
			node := nodes[0]
			log.Infof("Killing Portworx on node: %v", node.Name)
			killCmd := `for i in $(ps -ef | grep -i px | grep -i -e pxexec -e px-storage | grep -v grep | awk '{print $2}'); do sudo kill -9 ${i} ; done`
			_, err = client.ExecCommandOnNodeSSH(node.ID, killCmd)
			log.FailOnError(err, "Failed to kill Portworx processes")
			log.Infof("Portworx processes killed on node: %v", node.Name)

			log.Infof("Waiting for 2 minutes for Portworx to restart properly")
			time.Sleep(2 * time.Minute)

			psCmd := `ps -ef | grep -i px | grep -i -e pxexec -e px-storage | grep -v grep`
			out, err := client.ExecCommandOnNodeSSH(node.ID, psCmd)
			log.FailOnError(err, "Failed to execute ps command")
			if out == "" {
				log.FailOnError(fmt.Errorf("Portworx processes are not running"), "")
			}
			log.Infof("Portworx processes are running on node: %v", node.Name)
		}

		const runDuration = time.Minute * 10
		endTime := time.Now().Add(runDuration)
		var wg sync.WaitGroup

		for _, jobID := range jobIDs {
			wg.Add(1)
			go func(jID string) {
				defer wg.Done()
				for time.Now().Before(endTime) {
					status, err := client.CheckJobStatus(jID)
					if err != nil || status != "running" {
						log.Errorf("Job %s failed or stopped running", jID)
						return
					}
					log.Infof("Job %s is still running. Next check in 1 minute.", jID)
					time.Sleep(time.Minute)
				}
				log.Infof("Job %s completed successfully", jID)
			}(jobID)
		}
		wg.Wait()
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete Fio job %v", jobID)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v", volumeID)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{AdjustVolumeReplFactor}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string

	BeforeEach(func() {
		StartTorpedoTest("AdjustVolumeReplFactor", "Test to adjust volume replication factor", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		initialRepl := "2"
		capacityMin := int64(10 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl": initialRepl,
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume with initial replication factor")
		log.Infof("Volume %v created successfully with repl factor %v", volumeID, initialRepl)
	})

	It("Adjusts the replication factor of a volume", func() {
		newRepl := "3"
		log.Infof("Increasing volume replication factor to %v", newRepl)
		err = client.AdjustVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Failed to increase volume replication factor")
		log.Infof(" Sleeping for 1 minute for you to check .....")
		time.Sleep(1 * time.Minute)
		err = client.ValidateVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Volume replication factor validation failed after increase")

		newRepl = "2"
		log.Infof("Decreasing volume replication factor to %v", newRepl)
		err = client.AdjustVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Failed to decrease volume replication factor")

		err = client.ValidateVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Volume replication factor validation failed after decrease")
	})

	AfterEach(func() {
		err = client.DeleteVolume(volumeID)
		log.FailOnError(err, "Failed to delete volume %v", volumeID)
		log.Infof("Successfully deleted volume %v", volumeID)
	})
})

var _ = Describe("{LongStopPxWhileAppsAreRunning}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string
	var volumeIDs []string
	var jobIDs []string
	var directories = []string{"job1", "job2", "job3"}

	BeforeEach(func() {
		StartTorpedoTest("LongStopPxWhileAppsAreRunning", "Runs Multiple FIO Jobs on Nomad Cluster and stops Portworx for long time", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(50 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "2",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create shared volume")
		log.Infof("Shared volume %v created successfully", volumeID)
		volumeIDs = append(volumeIDs, volumeID)
		for _, directory := range directories {
			jobID := fmt.Sprintf("fio-%s", directory)
			jobIDs = append(jobIDs, jobID)
			fioJob := client.CreateFioJobSpec(volumeID, jobID, directory)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job for directory "+directory)
		}
		log.Infof("Sleeping for 10 seconds to allow allocations to start")
		time.Sleep(10 * time.Second)

		for _, jobID := range jobIDs {
			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					return nil, true, fmt.Errorf("job %s is not running yet", jobID)
				}
				return nil, false, nil
			}, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Failed to wait for job to be in running state")
		}
	})

	It("Should stop Portworx, check FIO apps, start Portworx, and validate apps", func() {
		nodes, err := client.ListNodes()
		log.FailOnError(err, "Failed to list nodes")
		if len(nodes) > 0 {
			node := nodes[0]
			log.Infof("Stopping Portworx on node: %v", node.Name)
			stopCmd := `sudo systemctl stop portworx`
			_, err = client.ExecCommandOnNodeSSH(node.ID, stopCmd)
			log.FailOnError(err, "Failed to stop Portworx via systemctl")
			log.Infof("Portworx stopped on node: %v", node.Name)

			// Wait for 5 minutes and check FIO apps
			log.Infof("Waiting for 5 minutes before checking FIO jobs")
			time.Sleep(5 * time.Minute)
			for _, jobID := range jobIDs {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					log.FailOnError(err, fmt.Sprintf("Job %s failed or stopped running during Portworx downtime", jobID))
				}
				log.Infof("Job %s is running absolutely fine", jobID)
			}

			log.Infof("Waiting for 5 more minutes before starting Portworx")
			time.Sleep(5 * time.Minute)
			log.Infof("Starting Portworx on node: %v", node.Name)
			startCmd := `sudo systemctl start portworx`
			_, err = client.ExecCommandOnNodeSSH(node.ID, startCmd)
			log.FailOnError(err, "Failed to start Portworx via systemctl")
			log.Infof("Portworx started on node: %v", node.Name)

			log.Infof("Validating FIO jobs for the next 5 minutes after Portworx restart")
			validateDuration := time.Minute * 5
			validateEndTime := time.Now().Add(validateDuration)
			for time.Now().Before(validateEndTime) {
				for _, jobID := range jobIDs {
					status, err := client.CheckJobStatus(jobID)
					if err != nil || status != "running" {
						log.FailOnError(err, fmt.Sprintf("Job %s failed or stopped running during Portworx downtime", jobID))
					}
					log.Infof("Job %s is running absolutely fine", jobID)
				}
				time.Sleep(time.Minute)
			}
		} else {
			log.FailOnError(fmt.Errorf("no nodes found"), "Failed to find nodes to stop Portworx")
		}
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete Fio job %v", jobID)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v", volumeID)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{RebootNodeWhileAppsAreRunning}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string
	var volumeIDs []string
	var jobIDs []string
	var directories = []string{"job1", "job2", "job3"}

	BeforeEach(func() {
		StartTorpedoTest("RebootNodeWhileAppsAreRunning", "Runs Multiple FIO Jobs on Nomad Cluster and reboots a node", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(50 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "2",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create shared volume")
		log.Infof("Shared volume %v created successfully", volumeID)
		volumeIDs = append(volumeIDs, volumeID)
		for _, directory := range directories {
			jobID := fmt.Sprintf("fio-%s", directory)
			jobIDs = append(jobIDs, jobID)
			fioJob := client.CreateFioJobSpec(volumeID, jobID, directory)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job for directory "+directory)
		}
		log.Infof("Sleeping for 10 seconds to allow allocations to start")
		time.Sleep(10 * time.Second)

		// Ensure all jobs are running
		for _, jobID := range jobIDs {
			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					return nil, true, fmt.Errorf("job %s is not running yet", jobID)
				}
				return nil, false, nil
			}, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Failed to wait for job to be in running state")
		}
	})

	It("Should reboot a node, wait for it to come back, and validate FIO jobs", func() {
		nodes, err := client.ListNodes()
		log.FailOnError(err, "Failed to list nodes")
		if len(nodes) > 0 {
			node := nodes[0]
			log.Infof("Rebooting node: %v", node.Name)
			rebootCmd := `reboot`
			_, err = client.ExecCommandOnNodeSSH(node.ID, rebootCmd)
			if err == nil {
				log.FailOnError(err, "Failed to reboot node")
			}

			log.Infof("Waiting up to 5 minutes for node to come back online")
			nodeBackOnline := false
			for i := 0; i < 5; i++ {
				_, err := client.ExecCommandOnNodeSSH(node.ID, "echo 'Node back online'")
				if err == nil {
					nodeBackOnline = true
					log.Infof("Node %v is back online", node.Name)
					break
				}
				time.Sleep(1 * time.Minute)
			}

			if !nodeBackOnline {
				log.FailOnError(fmt.Errorf("node %s did not come back online within 5 minutes", node.Name), "")
			}

			// Check for Portworx to become operational
			log.Infof("Checking for Portworx to become operational")
			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				pxStatusCmd := `pxctl status | grep "Status: PX is operational"`
				output, err := client.ExecCommandOnNodeSSH(node.ID, pxStatusCmd)
				if err != nil || !strings.Contains(output, "Status: PX is operational") {
					return nil, true, fmt.Errorf("Portworx is not yet operational")
				}
				return nil, false, nil
			}, 5*time.Minute, 30*time.Second)

			for _, jobID := range jobIDs {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					log.FailOnError(err, fmt.Sprintf("Job %s failed or stopped running after node reboot", jobID))
				} else {
					log.Infof("Job %s is still running after node reboot", jobID)
				}
			}
		} else {
			log.FailOnError(fmt.Errorf("no nodes found"), "Failed to find nodes to reboot")
		}
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete Fio job %v", jobID)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v", volumeID)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})

var _ = Describe("{AdjustVolumeReplFactorAndVolumeResize}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string

	BeforeEach(func() {
		StartTorpedoTest("AdjustVolumeReplFactorAndVolumeResize", "Test to adjust volume replication factor", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("volume-%v", time.Now().Unix())
		pluginID = "portworx"
		initialRepl := "2"
		capacityMin := int64(10 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl": initialRepl,
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create volume with initial replication factor")
		log.Infof("Volume %v created successfully with repl factor %v", volumeID, initialRepl)
	})

	It("Adjusts the replication factor of a volume", func() {
		newRepl := "3"
		log.Infof("Increasing volume replication factor to %v", newRepl)
		err = client.AdjustVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Failed to increase volume replication factor")
		log.Infof("Sleeping for 20 seconds before validating the new Repl factor")
		time.Sleep(20 * time.Second)
		err = client.ValidateVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Volume replication factor validation failed after increase")
		newSize := "100"
		log.Infof("Increasing volume size to %v", newSize)
		err = client.ResizeVolume(volumeID, newSize)
		log.FailOnError(err, "Failed to resize volume")
		log.Infof("Sleeping for 20 seconds before validating the new size")
		time.Sleep(20 * time.Second)
		err = client.ValidateVolumeSize(volumeID, newSize)
		log.FailOnError(err, "Volume failed to resize")
		newRepl = "2"
		log.Infof("Decreasing volume replication factor to %v", newRepl)
		err = client.AdjustVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Failed to decrease volume replication factor")
		log.Infof("Sleeping for 20 seconds before validating the new Repl factor")
		time.Sleep(20 * time.Second)
		err = client.ValidateVolumeReplFactor(volumeID, newRepl)
		log.FailOnError(err, "Volume replication factor validation failed after decrease")
	})

	AfterEach(func() {
		err = client.DeleteVolume(volumeID)
		log.FailOnError(err, "Failed to delete volume %v", volumeID)
		log.Infof("Successfully deleted volume %v", volumeID)
	})
})

var _ = Describe("{PoolExpandWhileAppsAreRunning}", func() {
	var client *nomad.NomadClient
	var err error
	var volumeID, pluginID string
	var volumeIDs []string
	var jobIDs []string
	var directories = []string{"job1", "job2", "job3"}

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandWhileAppsAreRunning", "Runs Multiple FIO Jobs on Nomad Cluster and expand pool", nil, 0)
		client, err = nomad.NewNomadClient()
		log.FailOnError(err, "Failed to get Nomad Client")

		volumeID = fmt.Sprintf("fio-test-volume-%v", time.Now().Unix())
		pluginID = "portworx"
		capacityMin := int64(50 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		parameters := map[string]string{
			"repl":        "3",
			"io_profile":  "db_remote",
			"priority_io": "high",
		}

		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags, parameters)
		log.FailOnError(err, "Failed to create shared volume")
		log.Infof("Shared volume %v created successfully", volumeID)
		volumeIDs = append(volumeIDs, volumeID)
		for _, directory := range directories {
			jobID := fmt.Sprintf("fio-%s", directory)
			jobIDs = append(jobIDs, jobID)
			fioJob := client.CreateFioJobSpec(volumeID, jobID, directory)
			err = client.CreateJob(fioJob)
			log.FailOnError(err, "Failed to create Fio job for directory "+directory)
		}
		log.Infof("Sleeping for 10 seconds to allow allocations to start")
		time.Sleep(10 * time.Second)

		for _, jobID := range jobIDs {
			_, err = task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				status, err := client.CheckJobStatus(jobID)
				if err != nil || status != "running" {
					return nil, true, fmt.Errorf("job %s is not running yet", jobID)
				}
				return nil, false, nil
			}, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Failed to wait for job to be in running state")
		}
	})

	It("Should run multiple Fio jobs on a shared RWX volume and handle pool expansion", func() {
		nodes, err := client.ListNodes()
		log.FailOnError(err, "Failed to list nodes")
		if len(nodes) == 0 {
			log.FailOnError(fmt.Errorf("No nodes found"), "Failed to find a node for pool expansion")
		}

		node := nodes[0]
		poolUIDs, err := client.FetchPoolUIDs(node.ID)
		log.FailOnError(err, "Failed to fetch pool UIDs")

		if len(poolUIDs) > 0 {
			poolUID := poolUIDs[0]
			expandPoolCmd := fmt.Sprintf("pxctl service pool expand -o auto -s 300 -u %s", poolUID)
			_, err = client.ExecCommandOnNodeSSH(node.ID, expandPoolCmd)
			log.FailOnError(err, "Failed to expand pool")
			log.Infof("Pool with UID %s expanded on node: %v. Waiting for stabilization.", poolUID, node.Name)
		} else {
			log.FailOnError(fmt.Errorf("No pools found to expand"), "")
		}

		const runDuration = time.Minute * 10
		endTime := time.Now().Add(runDuration)
		var wg sync.WaitGroup

		for _, jobID := range jobIDs {
			wg.Add(1)
			go func(jID string) {
				defer wg.Done()
				for time.Now().Before(endTime) {
					status, err := client.CheckJobStatus(jID)
					if err != nil || status != "running" {
						log.Errorf("Job %s failed or stopped running", jID)
						return
					}
					log.Infof("Job %s is still running. Next check in 1 minute.", jID)
					time.Sleep(time.Minute)
				}
				log.Infof("Job %s completed successfully", jID)
			}(jobID)
		}
		wg.Wait()
	})

	AfterEach(func() {
		for _, jobID := range jobIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteJob(jobID)
				if err != nil {
					log.Errorf("Retry error: failed to delete Fio job %v: %v", jobID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete Fio job %v", jobID)
			log.Infof("Successfully deleted Fio job %v", jobID)
		}

		// Delete volumes with retry
		for _, volumeID := range volumeIDs {
			_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
				err := client.DeleteVolume(volumeID)
				if err != nil {
					log.Errorf("Retry error: failed to delete volume %v: %v", volumeID, err)
					return nil, true, err
				}
				return nil, false, nil
			}, 2*time.Minute, 20*time.Second)
			log.FailOnError(err, "Final error: failed to delete volume %v", volumeID)
			log.Infof("Successfully deleted volume %v", volumeID)
		}
	})
})
