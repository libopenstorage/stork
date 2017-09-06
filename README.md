# Stork - A Storage Orchestration Runtime
Stork is a Cloud Native storage orchestration runtime scheduler plugin.  It translates a scheduler's orchestration decisions into someting that an external cloud native storage solution can act upon.

![alt text](images/stork.png =100x20)

Stork is intended to allow storage operators such as Portworx, EMC-RexRay, and Kubernetes Local Storage to extend upon scheduler actions and allow for a storage-implementation specific orchestration actions around what the orchestrator is trying to do.  The most basic example is when the scheduler is trying to spawn a container that is part of a pod - Stork will allow for the storage provider to specify an appropriate node on which that container needs to run such that it's data access is local to the runtime of the contaner.  This is one of many orchestration scenarios that is adressed by this project.

## Stork as a Kubernetes Scheduler Extension
Stork is implemented as a Kubernetes native scheduler extension.  It interfaces with the Kubernetes state machine to provide the storage provider with the [predicates defined here](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase).  Stork will then translate those predicates to the following storage orchestration actions:

| Cointainer State                                                                                                                                                                                                                                                   | Storage Runtime Input      | Storage Runtime Output |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|------------------------| 
| A POD is in state `pending`                                                                                                                                                                                                                                        | Runtime                    |                        |
| A POD is in state `running`                                                                                                                                                                                                                                        | Runtime                    |                        |
| A POD is in state `succeeded`                                                                                                                                                                                                                                      | Acceptance                 |                        |
| A POD is in state `failed`                                                                                                                                                                                                                                         | Acceptance                 |                        |
| A POD is in state `unknown`                                                                                                                                                                                                                                        | Acceptance                 |                        |

# Building Stork
Stork is written in Golang. To build Torpedo:

```
# git clone git@github.com:portworx/torpedo.git
# make
```

# Running Stork


# Contributing
The specification and code is licensed under the Apache 2.0 license found in 
the `LICENSE` file of this repository.  

See the [Style Guide](STYLEGUIDE.md).

### Protoeasy quick start

https://go.pedge.io/protoeasy

```
docker pull quay.io/pedge/protoeasy
```

Add to your ~/.bashrc (or equivalent):

```
# to use protoeasy for now, you must have docker installed locally or in a vm
# if running docker using docker-machine etc, replace 192.168.10.10 with the ip of the vm
# if running docker locally, replace 192.168.10.10 with 0.0.0.0
export PROTOEASY_ADDRESS=127.0.0.1:6789

launch-protoeasy() {
  docker rm -f protoeasy || true
  docker run -d -p 6789:6789 --name=protoeasy quay.io/pedge/protoeasy
}
```

Then just run `launch-protoeasy` before compiling the protocol buffers files, and then to compile:

```
make proto
```

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
