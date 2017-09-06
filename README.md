# Stork - A Storage Orchestration Runtime
Stork is a Cloud Native storage orchestration runtime scheduler plugin.  It translates a scheduler's orchestration decisions into someting that an external cloud native storage solution can act upon.

<div style="text-align:center"><img src="images/stork.png" alt="Drawing" style="width: 50px;"/></div>

Stork is intended to allow storage operators such as Portworx, EMC-RexRay, and Kubernetes Local Storage to extend upon scheduler actions and allow for a storage-implementation specific orchestration actions around what the orchestrator is trying to do.  The most basic example is when the scheduler is trying to spawn a container that is part of a pod - Stork will allow for the storage provider to specify an appropriate node on which that container needs to run such that it's data access is local to the runtime of the contaner.  This is one of many orchestration scenarios that is adressed by this project.

## Stork as a Kubernetes Scheduler Extension
Stork is implemented as a Kubernetes native scheduler extension.  It interfaces with the Kubernetes state machine to provide the storage provider with the [predicates defined here](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase).  Stork will then translate those predicates to the following storage orchestration actions:

| Cointainer State                                         | Storage Runtime Input      | Storage Runtime Output |
|----------------------------------------------------------|----------------------------|------------------------| 
| A POD is in state `pending`                              | POD STATE, POD SPEC        | NEXT STATE, NODE       |
| A POD is in state `running`                              | POD STATE, POD SPEC        | NOOP                   |
| A POD is in state `succeeded`                            | POD STATE, POD SPEC        | NOOP                   |
| A POD is in state `failed`                               | POD STATE, POD SPEC        | NEXT STATE, NODE       |
| A POD is in state `unknown`                              | POD STATE, POD SPEC        | NEXT STATE, NODE       |

# Building Stork
Stork is written in Golang. To build Torpedo:

```
# git clone git@github.com:libopenstorage/stork.git
# export DOCKER_HUB_REPO=myrepo
# export DOCKER_HUB_STORK_IMAGE=stork
# export DOCKER_HUB_TAG=latest
# make
```

This will create the Docker image `$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_TAG)`.

# Running Stork
Now that you have the stork scheduler in a container image, you can just create a pod config for it and run it in your Kubernetes cluster.  We do this via a deployment.

## Create a Deployment
A Deployment manages a Replica Set which in turn manages the pods, thereby making the scheduler resilient to failures.  Here is the deployment config.  Change the docker hub image Save it as stork.yaml:

```yaml
apiVersion: apps/v1beta1
kind: Deployment
metadata:
  labels:
    component: scheduler
    tier: control-plane
  name: stork
  namespace: kube-system
spec:
  replicas: 1
  template:
    metadata:
      labels:
        component: scheduler
        tier: control-plane
        version: second
    spec:
      containers:
      - command:
        - /usr/local/bin/stork
        - --address=0.0.0.0
        - --leader-elect=false
        - --scheduler-name=stork
        image: $(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_TAG)
        livenessProbe:
          httpGet:
            path: /healthz
            port: 10251
          initialDelaySeconds: 15
        name: kube-stork-scheduler
        readinessProbe:
          httpGet:
            path: /healthz
            port: 10251
        resources:
          requests:
            cpu: '0.1'
        securityContext:
          privileged: false
        volumeMounts: []
      hostNetwork: false
      hostPID: false
      volumes: []
```

## Run the Stork Scheduler in the Cluster
In order to run the stork scheduler in your Kubernetes cluster, just create the deployment specified in the config above in a Kubernetes cluster:

```
# kubectl create -f stork.yaml
```

Verify that the scheduler pod is running:

```
# kubectl get pods --namespace=kube-system
NAME                                           READY     STATUS    RESTARTS   AGE
....
stork-lnf4s-4744f                              1/1       Running   0          2m
...
```

## Specify the Stork Schedulers for Pods
In order to schedule a given pod using the Stork scheduler, specify the name of the scheduler in that pod spec:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: annotation-stork-scheduler
  labels:
    name: stork-scheduler-example
spec:
  schedulerName: stork
  containers:
  - name: pod-with-annotation-container
    image: gcr.io/google_containers/pause:2.0
```

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
