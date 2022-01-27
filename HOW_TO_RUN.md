## Pre-requisites:
* Minimum 3-node Kubernetes cluster
* Portworx installed on the Kubernetes cluster
* Torpedo currently requires root username/password to access the physical nodes.

## Running using a Kubernetes pod

### Kubernetes with ssh node driver

To run torpedo with ssh node driver run the following script:

```
$ TORPEDO_SSH_PASSWORD=[your_root_password] deployments/deploy-ssh.sh
```

### Kubernetes with aws node driver

To run torpedo under AWS environment with aws node driver run the following script:

```
$ deployments/deploy-aws.sh
```

Make sure you change `image: portworx/torpedo:latest` to your torpedo docker image.

The above command starts Torpedo by deploying a k8s `Pod` in your kubernetes cluster.  It also specified Portworx (`pxd`) as the volume driver and `ssh` as the node driver to.

You can look at status of torpedo by viewing logs using: `kubectl logs -f torpedo`

## Running directly using ginkgo
First expose KUBECONFIG to torpedo can talk to the k8s API.

`export KUBECONFIG=<location_of_k8s_cluster_kubeconfig_file>`

It is highly suggested to run your new test against a single app

For example:
``ginkgo --focus SetupTeardown -v bin/basic.test -- -spec-dir `pwd`/drivers/scheduler/k8s/specs --app-list elasticsearch``

This will run the basic test against just elastic search

To run all tests: ``ginkgo -v bin/*.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

To run just the reboot tests: ``ginkgo -v bin/reboot.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

To dry-run all tests: ``ginkgo -dryRun   -v bin/*.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

### Running torpedo on EKS

```text
NOTE: perform the steps below if cluster was created using eksctl or any tool, other than dedicated eks jenkins job 
```

(if cluster was not provisioned with spawn)

1. Provision a cluster of nodes n+1 (more than you need)
2. Create a "master" node. Torpedo contains a test which reboots all px nodes, hence it needs to be alive. We don't have a master node on EKS, so we just `simulate` it :
    1. Pick a node
    2. disable PX on it
    3. taint

    ```
    kubectl get nodes '--output=jsonpath={.items[0].metadata.name}'
    
    kubectl label node <node_name> px/enabled=false
    
    kubectl taint node <node_name> apps=false:NoSchedule
    ```

3. Install PX in a usual way
4. Add the following parameters to the `[deploy-ssh.sh](http://deploy-ssh.sh)` script at the very beginning of the file

```jsx
#KUBECONFIG=/tmp/kubeconfig # if you don't specify the config, torpedo will pick up the system config
TORPEDO_IMG=portworx/torpedo:master # override with your image if needed
VERBOSE=true
FOCUS_TESTS=SetupTeardown,AppTasksDown,VolumeDriverDown,VolumeDriverAppDown,VolumeDriverDownAttachedNode,VolumeDriverCrash,AppScaleUpAndDown,VolumeUpdate
SCALE_FACTOR=1
FAIL_FAST=true
APP_LIST=postgres,sysbench,nginx-sharedv4,vdbench-sharedv4
K8S_VENDOR=eks
```

5. In the same file search for the test list and disable tests you don't need by removing `.test` files

```jsx
TEST_SUITE='"bin/asg.test",
            "bin/autopilot.test",
            "bin/basic.test",
            "bin/backup.test",
            "bin/reboot.test",
            "bin/upgrade.test",
            "bin/drive_failure.test",
            "bin/volume_ops.test",
            "bin/sched.test",
            "bin/scheduler_upgrade.test",
            "bin/node_decommission.test",
            "bin/license.test",
            "bin/upgrade_cluster.test",
            "bin/sharedv4.test",
            "bin/telemetry.test",
            "bin/upgrade_cluster.test",
            "bin/pxcentral.test",'
```
6. Add your test to the `FOCUS_TESTS`:  
   `FOCUS_TESTS=SetupTeardown,AppTasksDown,...,<YOUR_TEST_NAME>`
   
7. Run `./deploy-ssh.sh`
