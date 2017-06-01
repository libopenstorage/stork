# Torpedo

[![Travis branch](https://img.shields.io/travis/portworx/torpedo/master.svg)](https://travis-ci.org/portworx/torpedo)
[![Go Report Card](https://goreportcard.com/badge/github.com/portworx/torpedo)](https://goreportcard.com/report/github.com/portworx/torpedo)


Torpedo is a test suite to qualify storage providers for stateful containers running in a cluster.  It tests various scenarios that applications encounter when running in Linux containers and deployed via schedulers such as Marathon, Kubernetes or Swarm.

## Background
This test suite will natively support the CSI specification for external volume support into Mesosphere and Kubernetes.

However, at the time of this writing, CSI is currently still work in progress, so most schedulers provide external volume support to Mesosphere or Kubernetes via DVDI or Kubernetes native driver interface.

Docker volume driver interface (DVDI) provides the control path operations to create, mount, unmount and eventually delete an external volume.

The core interaction between the Docker runtime and the Portworx endpoint are documented here: https://github.com/portworx/px-docs/blob/gh-pages/scheduler/docker/volume_plugin.md

This document covers the various scheduler-storage integration points that are being addressed by the CSI specification (https://docs.google.com/document/d/1JMNVNP-ZHz8cGlnqckOnpJmHF-DNY7IYP-Di7iuVhQI/edit#) and how external volume providers like Portworx are able to support CSI with DVDI today.

## Scenarios to Consider when Deploying Stateful Applications
Deploying ephemeral applications require less consideration when compared to stateful applications.  Administrators should take into account for various production runtime scenarious that may occur when running stateful applications in production, and chose an external storage provider that is capable of dealing with these situations.  Examples of these scenarious are:

### Runtime software hangs and crashes
* Container runtime engine (or scheduler) software failure, hang or crash:  When a daemon, like Docker crashes, it can induce errors with an application's connectivity to the external storage.  This problem is compounded when the storage provider itself, runs as a container.  In general, you need to assume that user space code will either hang or crash, and the storage system needs to gracefully deal with this, without data loss, unavailability or corruption.
* External storage driver software failure, hang or crash:  When the storage software itself crashes, the overall solution needs to make sure that there are no lost IOs (data loss), unavailability or corruption.

### Network and host issues
* Network disconnect from the storage provider to the external environment:  If a node on which the storage volume driver is running were to become disconnected from the network, the overall solution needs to make sure that the volume can be used on another node, and that there is no data loss or corruption.
* A node running a stateful application becomes permanently (or for a prolongued period of time unreachable):  In many cases, a node can become permanently unusable.  In cases, such as AWS, when an EBS volume is attached to such a node, the over all solution needs to make sure that the volume or the data can somehow still be used on some other node in the cluster.
* A network partition in the cluster:  When the scheduler cluster or the storage cluster gets partitioned in such a way that chorum is lost, the nodes that are still part of the chorum need to be able to use all of the data that was in the original cluster.  Otherwise, this would lead to data unavailability.

### Scheduler software issues
* Scheduler software attempts to deploy a stateful container on a node that is not part of the storage cluster:  It is possible that the storage cluster and the scheduler cluster do not comprise of the same machines.  The overall solution must prevent, or somehow make sure that when a stateful application is deployed on a non-storage node, that the application's storage requirements are fullfilled.  Some approaches to handle this include the use of scheduler constraints and labels.
* Scheduler software attempts to bring up a new container (pod) to use a storage volume prior to properly terminating the previous container or pod on a different host:  Scheduler software, perhaps due to bugs or timing issues, may launch a new application stack on a new set of nodes that refer to a volume currently in use by an application stack being torn down.  The overall solution must be capable of dealing with these transition scenarious, without application data loss or corruption.

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
| Provider                         | Information              |
|----------------------------------|--------------------------|
| Portworx                         | http://docs.portworx.com |

## Usage
Torpedo is written in Golang.  To build Torpedo:

```
# git clone git@github.com:portworx/torpedo.git
# make
```

### Important
Some Torpedo volume drivers such as the Portworx driver need to be able to talk to the Docker daemon during the tests.  This requires the Docker daemon to be configured to allow the a Docker client to connect on the TCP port.

Start Docker with
```
ExecStart=/usr/bin/dockerd -H fd:// -H tcp://0.0.0.0:2375
```

Torpedo can be run as follows:

```
# export CLUSTER_NODES="192.168.1.100,192.168.1.101,192.168.1.102"
# torpedo docker pxd
```

The above command starts Torpedo directly using the Docker daemon for the tests.  It also specified Portworx (`pxd`) as the volume driver.

Torpedo can also run as a Docker container (although some tests may not work, since they involve restarting or killing the Docker Daemon itself):

```
# docker run                                                        \
    --privileged=true                                               \
    --net=host                                                      \
    -e CLUSTER_NODES="192.168.1.100,192.168.1.101,192.168.1.102"    \
    torpedo <scheduler> <storage driver>
```

Where:

|  Argument | Description
|:---------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| --privileged=true | This must be provided as Torpedo will connect to the docker daemon and also kill the daemon during the negative testing.
| --net=host | This must be provided as Torpedo will attempt to communicate with the scheduler agents outside the container network.
| DOCKER_HOST | This is optional.  When specified, the Docker driver will use this variable to talk to the Docker daemon.  By default, it will use `unix:///var/run/docker.sock`.
| CLUSTER_NODES | This is a list of all the members in this cluster.  Some tests require a minimum cluster size and may not pass if there are not enough hosts in the cluster.

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
