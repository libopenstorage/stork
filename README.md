# Torpedo
Torpedo is a test suite to qualify storage providers for stateful containers running in a cluster.  It tests various scenarios that applications encounter when running in Linux containers and deployed via schedulers such as Marathon, Kubernetes or Swarm.

## Background
This test suite will natively support the CSI specification for external volume support into Mesosphere and Kubernetes.

However, at the time of this writing, CSI is currently still work in progress, so most schedulers provide external volume support to Mesosphere or Kubernetes via DVDI or Kubernetes native driver interface.

Docker volume driver interface (DVDI) provides the control path operations to create, mount, unmount and eventually delete an external volume.

The core interaction between the Docker runtime and the Portworx endpoint are documented here: https://github.com/portworx/px-docs/blob/gh-pages/scheduler/docker/volume_plugin.md

This document covers the various scheduler-storage integration points that are being addressed by the CSI specification (https://docs.google.com/document/d/1JMNVNP-ZHz8cGlnqckOnpJmHF-DNY7IYP-Di7iuVhQI/edit#) and how external volume providers like Portworx are able to support CSI with DVDI today.

## Common Error Scenarios
| Test/Scenario                                                                                                                                                                                                                                                      | Acceptance vs Runtime Test | Expected Result                                                                                                                                                                                   |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Create dynamic volumes                                                                                                                                                                                                                                             | Runtime                    | Expected to be able to create a volume with arbitrary parameters at runtime                                                                                                                       |
| Verify that the volume driver can deal with an uneven number of mounts and unmounts and allow the volume to get mounted on another node.                                                                                                                           | Runtime                    | Expected to pass                                                                                                                                                                                  |
| Volume Driver Plugin is down, unavailable - and the client container should not be impacted.                                                                                                                                                                       | Acceptance                 | Client container does not get an IO error.                                                                                                                                                        |
| Volume driver plugin is down and the client container gets terminated.,There is a lost unmount call in this case, but the container should be able to come up on another system and use the volume.                                                                | Acceptance                 | Expected to pass.                                                                                                                                                                                 |
| A container is using a volume on node X.,Node X is now powered off.                                                                                                                                                                                                | Acceptance                 | The system must be able to create a new container on node Y and use the same volume using pod replace.                                                                                            |
| Storage plugin is down.,Scheduler tries to create a container using the provider’s volume.                                                                                                                                                                         | Acceptance                 | This should fail.,The container should not start and the scheduler should receive an error.                                                                                                       |
| A container is running on node X.,Node X looses network access and is partitioned away.,Node Y that is in the cluster can use the volume for another container.                                                                                                    | Acceptance                 | When node X re-joins the network and hence joins the cluster, it is expected that the application that is running will get I/O errors since the block volume is attached on another node.         |
| A container is running on node X.,Node X can only see a subset of the storage cluster.,That is, it can see the entire DC/OS cluster, but just the storage cluster gets a network partition.Node Y that is in the cluster can use the volume for another container. | Acceptance                 | When node X re-joins the storage network and hence joins the cluster, it is expected that the application that is running will get I/O errors since the block volume is attached on another node. |
| Docker daemon crashes and live restore is disabled.                                                                                                                                                                                                                | Acceptance                 | The agent detects that the task has died and it brings it up on another node and the task can re-use the volume.                                                                                  |
| Docker daemon crashes and live restore is enabled.,This scenario should be a noop.,Container does not crash.                                                                                                                                                       | Acceptance                 | Expected to pass                                                                                                                                                                                  |

## Qualified External Storage Providers
| Provider                         | Information              |
|----------------------------------|--------------------------|
| Portworx                         | http://docs.portworx.com |

## Usage

Torpedo runs as a Docker container:

```
# docker run 														\
	-e DOCKER_HOST=127.0.0.1										\
	-e CLUSTER_NODES="192.168.1.100,192.168.1.101,192.168.1.102"	\
	torpedo <scheduler> <storage driver>
```

Where:
* DOCKER_HOST is optional.  When specified, the Docker driver will use this variable to talk to the Docker daemon.  By default, it will use `unix:///var/run/docker.sock`.
* CLUSTER_NODES is a list of all the members in this cluster.  Some tests require a minimum cluster size and may not pass if there are not enough hosts in the cluster.

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
