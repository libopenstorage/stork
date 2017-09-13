# Torpedo

[![Travis branch](https://img.shields.io/travis/portworx/torpedo/master.svg)](https://travis-ci.org/portworx/torpedo)
[![Go Report Card](https://goreportcard.com/badge/github.com/portworx/torpedo)](https://goreportcard.com/report/github.com/portworx/torpedo)


Torpedo is a test suite to qualify storage providers for stateful containers running in a distributed environment.  It tests various scenarios that applications encounter when running in Linux containers and deployed via schedulers such as Kubernetes, Marathon or Swarm.

<div style="text-align:center"><img src="images/torpedo.png" alt="Drawing" style="width: 50px;"/></div>

## CSI
CSI is a specification for Linux Container Storage Interfaces.  It defines the control plane interraction between a cloud native scheduler such as Kubernetes, and a cloud native storage provider.  The specification is available [here](https://github.com/container-storage-interface/spec/blob/master/spec.md).

The Torpedo test suite natively supports the CSI specification for external volume support into Kubernetes and Mesosphere.  It operates as a CSI enabled orchestrator (scheduler) to communicate with external storage providers that support CSI.

Torpedo tests cover the various scheduler-storage integration points that are being addressed by the CSI specification (https://docs.google.com/document/d/1JMNVNP-ZHz8cGlnqckOnpJmHF-DNY7IYP-Di7iuVhQI/edit#) and how external volume providers like Portworx are able to support production level operational scenarios when it comes to storage, server, software or network failures.

### Legacy support
Since CSI is currently still work in progress, most schedulers provide external volume support to Mesosphere or Kubernetes via DVDI or the Kubernetes native driver interface.

Docker volume driver interface (DVDI) provides the control path operations to create, mount, unmount and eventually delete an external volume and is documented [here](https://docs.docker.com/engine/extend/plugins_volume/).

In order to support legacy storage drivers, Torpedo can also work with schedulers that still use the Docker volume driver interface.

## Scenarios to Consider when Deploying Stateful Applications
Deploying ephemeral applications require less consideration when compared to stateful applications.  When running stateful applications in production, administrators should take into account various runtime scenarios that may occur and chose an external storage provider that is capable of dealing with these situations.  Examples of these scenarios are:

### Runtime software hangs and crashes
* Container runtime engine (or scheduler) software failure, hang or crash:  When a daemon, like Docker crashes, it can induce errors with an application's connectivity to the external storage.  This problem is compounded when the storage provider itself, runs as a container.  In general, you need to assume that user space code will either hang or crash, and the storage system needs to gracefully deal with this, without data loss, unavailability or corruption.
* External storage driver software failure, hang or crash:  When the storage software itself crashes, the overall solution needs to make sure that there are no lost IOs (data loss), unavailability or corruption.

### Network and host issues
* Network disconnect from the storage provider to the external environment:  If a node on which the storage volume driver is running were to become disconnected from the network, the overall solution needs to make sure that the volume can be used on another node, and that there is no data loss or corruption.
* A node running a stateful application becomes permanently (or for a prolonged period of time) unreachable:  In many cases, a node can become permanently unusable.  In cases, such as AWS, when an EBS volume is attached to such a node, the overall solution needs to make sure that the volume or the data can somehow still be used on some other node in the cluster.
* A network partition in the cluster:  When the scheduler cluster or the storage cluster gets partitioned in such a way that quorum is lost, the nodes that are still part of the quorum need to be able to use all of the data that was in the original cluster.  Otherwise, this would lead to data unavailability.

### Scheduler software issues
* Scheduler software attempts to deploy a stateful container on a node that is not part of the storage cluster:  It is possible that the storage cluster and the scheduler cluster do not comprise of the same machines.  The overall solution must prevent, or somehow make sure that when a stateful application is deployed on a non-storage node, that the application's storage requirements are fulfilled.  Some approaches to handle this include the use of scheduler constraints and labels.
* Scheduler software attempts to bring up a new container/pod/task to use a storage volume prior to properly terminating the previous container/pod/task on a different host:  Scheduler software, perhaps due to bugs or timing issues, may launch a new application stack on a new set of nodes that refer to a volume currently in use by an application stack being torn down.  The overall solution must be capable of dealing with these transition scenarios, without application data loss or corruption.

## Test Cases Run by Torpedo
| Test/Scenario                                                                                                                                                                                                                                                      | Acceptance vs Runtime Test | Expected Result                                                                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Create dynamic volumes                                                                                                                                                                                                                                             | Runtime                    | Expected to be able to create a volume with arbitrary parameters at runtime                                                                                                                       |
| Verify that the volume driver can deal with an uneven number of mounts and unmounts and allow the volume to get mounted on another node.                                                                                                                           | Runtime                    | Expected to pass                                                                                                                                                                                  |
| Volume Driver Plugin is down, unavailable - and the client container should not be impacted.                                                                                                                                                                       | Acceptance                 | Client container does not get an IO error.                                                                                                                                                        |
| Volume driver plugin is down and the client container gets terminated. There is a lost unmount call in this case, but the container should be able to come up on another system and use the volume.                                                                | Acceptance                 | Expected to pass.                                                                                                                                                                                 |
| A container is using a volume on node X. Node X is now powered off.                                                                                                                                                                                                | Acceptance                 | The system must be able to create a new container on node Y and use the same volume using pod replace.                                                                                            |
| Storage plugin is down. Scheduler tries to create a container using the provider’s volume.                                                                                                                                                                         | Acceptance                 | This should fail.,The container should not start and the scheduler should receive an error.                                                                                                       |
| A container is running on node X. Node X looses network access and is partitioned away. Node Y that is in the cluster can use the volume for another container.                                                                                                    | Acceptance                 | When node X re-joins the network and hence joins the cluster, it is expected that the application that is running will get I/O errors since the block volume is attached on another node.         |
| A container is running on node X. Node X can only see a subset of the storage cluster. That is, it can see the entire DC/OS cluster, but just the storage cluster gets a network partition. Node Y that is in the cluster can use the volume for another container. | Acceptance                 | When node X re-joins the storage network and hence joins the cluster, it is expected that the application that is running will get I/O errors since the block volume is attached on another node. |
| Docker daemon crashes and live restore is disabled.                                                                                                                                                                                                                | Acceptance                 | The agent detects that the task has died and it brings it up on another node and the task can re-use the volume.                                                                                  |
| Docker daemon crashes and live restore is enabled. This scenario should be a noop. Container does not crash.                                                                                                                                                       | Acceptance                 | Expected to pass                                                                                                                                                                                  |

## Qualified External Storage Providers
To submit an external storage provider, please submit a PR with the output of the Torpedo test program and the specifics of the environment used.

| Provider                         | Information              | Test Coverage Status |
|----------------------------------|--------------------------|----------------------|
|                                  |                          |                      |

## Usage

>**Note:**<br/> Torpedo is in pre-alpha stage at this moment and under active development. Currently it only supports Kubernetes as the scheduler.

### Build
Torpedo is written in Golang.

To build Torpedo:
```
# git clone git@github.com:portworx/torpedo.git
# make
```

To deploy Torpedo image in your docker repository:
```
# export DOCKER_HUB_REPO=harshpx
# export DOCKER_HUB_TORPEDO_IMAGE=torpedo
# export DOCKER_HUB_TAG=latest
# make deploy
```

Make sure you change the environment variables above to match your docker repository.

### Run

#### Pre-requisites:
* Minimum 3-node Kubernetes cluster
* Portworx installed on the Kubernetes cluster
* A root user on each node created as follows. The password should be `t0rped0`. If not already available a group called sudo should be created too.
```
# useradd torpedo
# groupadd sudo
# usermod -aG sudo torpedo
# sudo sh -c "echo 'torpedo ALL=NOPASSWD: ALL' >> /etc/sudoers"
```

To run on kubernetes:
```
# kubectl apply -f deployments/torpedo-k8s.yaml
```

Make sure you change `image: harshpx/torpedo:latest` to your torpedo docker image.

The above command starts Torpedo by deploying a k8s `Pod` in your kubernetes cluster.  It also specified Portworx (`pxd`) as the volume driver and `ssh` as the node driver to.

You can look at status of torpedo by viewing logs using: `kubectl logs -f torpedo`

## Contributing

The specification and code is licensed under the Apache 2.0 license found in 
the `LICENSE` file of this repository.  

See the [Style Guide](STYLEGUIDE.md).

### Sign your work

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below (from
[developercertificate.org](http://developercertificate.org/)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
660 York Street, Suite 102,
San Francisco, CA 94110 USA

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

then you just add a line to every git commit message:

    Signed-off-by: Joe Smith <joe@gmail.com>

using your real name (sorry, no pseudonyms or anonymous contributions.)

You can add the sign off when creating the git commit via `git commit -s`.
