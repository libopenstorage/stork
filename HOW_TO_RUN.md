## Pre-requisites:
* Minimum 3-node Kubernetes cluster
* Portworx installed on the Kubernetes cluster
* A root user on each node created as follows. The password should be `t0rped0`. If not already available a group called sudo should be created too.
```
# adduser torpedo
Adding user `torpedo' ...
Adding new group `torpedo' (1001) ...
Adding new user `torpedo' (1001) with group `torpedo' ...
Creating home directory `/home/torpedo' ...
Copying files from `/etc/skel' ...
Enter new UNIX password:
Retype new UNIX password:
passwd: password updated successfully
Changing the user information for torpedo
Enter the new value, or press ENTER for the default
	Full Name []: torpedo
	Room Number []:
	Work Phone []:
	Home Phone []:
	Other []:

# groupadd sudo
# usermod -aG sudo torpedo
# sudo sh -c "echo 'torpedo ALL=NOPASSWD: ALL' >> /etc/sudoers"
```

## Running using a Kubernetes pod
To run on kubernetes with ssh node driver: `kubectl create -f deployments/torpedo-k8s-ssh.yaml`

To run on kubernetes with aws node driver: `kubectl create -f deployments/torpedo-k8s-aws.yaml`

Make sure you change `image: harshpx/torpedo:latest` to your torpedo docker image.

The above command starts Torpedo by deploying a k8s `Pod` in your kubernetes cluster.  It also specified Portworx (`pxd`) as the volume driver and `ssh` as the node driver to.

You can look at status of torpedo by viewing logs using: `kubectl logs -f torpedo`

## Running directly using ginkgo

First expose KUBECONFIG to torpedo can talk to the k8s API.

`export KUBECONFIG=<location_of_k8s_cluster_kubeconfig_file>`

To run all tests: ``ginkgo -v bin/*.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

To run just the basic tests: ``ginkgo -v bin/basic.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

To run just the reboot tests: ``ginkgo -v bin/reboot.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``

To dry-run all tests: ``ginkgo -dryRun   -v bin/*.test --  -spec-dir `pwd`/drivers/scheduler/k8s/specs``
