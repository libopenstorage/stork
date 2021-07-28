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
