package tests

import (
	"fmt"
	"github.com/hashicorp/nomad/api"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/nomad"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"time"
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
		volumeID := "test-volume"
		pluginID := "portworx"
		capacityMin := int64(1 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "single-node-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}

		err := client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags)
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
			err = client.DeleteVolume(volume.ID)
			if err != nil {
				log.Errorf("Volume %v could not be deleted with err %v", volume.ID, err)
			}
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
		volumeID = "fio-test-volume"
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "single-node-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags)
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

		for i := 0; i < numJobs; i++ {
			volumeID = fmt.Sprintf("fio-test-volume-%d", i)
			jobID = fmt.Sprintf("fio-job-%d", i)
			volumeIDs = append(volumeIDs, volumeID)
			log.Infof("Going ahead to create volume with ID: %v", volumeID)
			err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags)
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
		// Delete Fio jobs
		for _, jobID := range jobIDs {
			err = client.DeleteJob(jobID)
			if err != nil {
				log.Errorf("Failed to delete %v with error %v", jobID, err)
			} else {
				log.Infof("Successfully deleted %v", jobID)
			}
		}
		// Delete volumes
		for _, volumeID := range volumeIDs {
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
		volumeID = "fio-test-volume"
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags)
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
		if err != nil {
			log.Errorf("Failed to delete job: %v", err)
		} else {
			log.Infof("Successfully deleted job %v", jobID)
		}
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
		volumeID = "fio-test-volume"
		snapshotID = "fio-snapshot"
		pluginID = "portworx"
		capacityMin := int64(20 * 1024 * 1024 * 1024)
		capacityMax := capacityMin
		accessMode := "multi-node-multi-writer"
		attachmentMode := "file-system"
		fsType := "ext4"
		mountFlags := []string{}
		err = client.CreateVolume(volumeID, pluginID, capacityMin, capacityMax, accessMode, attachmentMode, fsType, mountFlags)
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
