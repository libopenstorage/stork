[![Build Status](https://travis-ci.org/libopenstorage/stork.svg?branch=master)](https://travis-ci.org/libopenstorage/stork)
[![Go Report Card](https://goreportcard.com/badge/github.com/libopenstorage/stork)](https://goreportcard.com/report/libopenstorage/stork)
[![Docker Pulls](https://img.shields.io/docker/pulls/openstorage/stork.svg)](https://hub.docker.com/r/openstorage/stork)
[![Code Coverage](https://codecov.io/gh/libopenstorage/stork/branch/master/graph/badge.svg)](https://codecov.io/gh/libopenstorage/stork)

# Stork - Storage Orchestration Runtime for Kubernetes
Stork is a Cloud Native storage orchestration runtime scheduler plugin. It translates a scheduler's orchestration decisions into someting that an external cloud native storage solution can act upon. By doing so, it extends Kubernetes with more stateful awareness of the underlying storage provider, it's capabilities and state.

<div style="text-align:center"><img src="images/stork.png" alt="Drawing" style="width: 50px;"/></div>

Stork is intended to allow storage operators such as Portworx, EMC-RexRay, and Kubernetes Local Storage to extend upon scheduler actions and allow for a storage-implementation specific orchestration actions around what the orchestrator is trying to do. The most basic example is when the scheduler is trying to spawn a container that is part of a pod - Stork will allow for the storage provider to specify an appropriate node on which that container needs to run such that it's data access is local to the runtime of the contaner. This is one of many orchestration scenarios that is adressed by this project.

# Features
## Hyper-Convergence
Stork can be used to co-locate pods with where their data is located. This is achieved by using a
[kubernetes scheduler extender](https://github.com/kubernetes/kubernetes/issues/11470).
The scheduler is configured to use stork as an extender. So every time a pod is being scheduled,
the scheduler will send filter and prioritize requests to stork. Stork will then
check with the storage driver
You can either configure the default kubernetes scheduler to communicate with
stork or launch another instance of kube-scheduler.

### Initializer (Experimental)
If you are not able to update the schedulerName for you applications to use
stork, you can enable the app-initializer feature. This uses the Kubernetes
[AdmissionController Initializer](https://kubernetes.io/docs/admin/extensible-admission-controllers/#initializers)
feature to automatically update the scheduler to stork if your application
(deployment or statefulset) is using volumes backed by the configured driver.

To enable the Initializer you need to:
* [Enable the Intializer feature in your Kubernetes cluster](https://kubernetes.io/docs/admin/extensible-admission-controllers/#enable-initializers-alpha-feature)
since it is an alpha feature.
* Add "--app-initializer=true" option to stork (in either the deployment or daemonset spec file)
* Add the [stork-initializer spec](specs/stork-initializer.yaml) to you Kubernetes cluster using `kubectl create -f stork-initializer.yaml`

## Health Monitoring
Stork will monitor the health of the volume driver on the different nodes. If the volume driver on a node becomes
unhealthy pods on that node using volumes from the driver will not be able to access their data. In this case stork will
relocate  pods on to other nodes so that they can continue running.

## Volume Snapshots

Stork uses the external-storage project from [kubernetes-incuabator](https://github.com/kubernetes-incubator/external-storage)
to add support for snapshots.

Refer to [Snapshots with Stork](doc/snaps.md) for instructions on creating and using snapshots with Stork.

### 3DSnaps

3DSnaps provides app-consistent cluster wide snapshots. It allows you to specify pre and post rules that are run on the application pods using the volumes. This allows users to quiesce the applications before the snapshot is taken and resume I/O after the snapshot is taken. The commands will be run in pods which are using the PVC being snapshotted.

Read [Configuring 3DSnaps](/doc/snaps-3d.md) for further details on 3DSnaps.


# Building Stork
Stork is written in Golang. To build Stork:

```
# git clone git@github.com:libopenstorage/stork.git
# export DOCKER_HUB_REPO=myrepo
# export DOCKER_HUB_STORK_IMAGE=stork
# export DOCKER_HUB_STORK_TAG=latest
# make
```

This will create the Docker image `$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_TAG)`.

# Running Stork
Now that you have stork in a container image, you can just create a pod config for it and run it in your Kubernetes cluster. We do this via a deployment.

## Create a Deployment
A Deployment manages a Replica Set which in turn manages the pods, thereby making stork resilient to failures. The deployment spec is defined in [specs/stork-deployment.yaml](https://raw.githubusercontent.com/libopenstorage/stork/master/specs/stork-deployment.yaml).
By default the deployment does the following
* Uses the latest stable image of stork to start a pod. You can update the tag to use a specific version or use your own stork image.
* Creates a service to provide an endpoint that can be used to reach the extender. 
* Creates a ConfigMap which can be used by a scheduler to communicate with stork.
* Uses the Portworx (pxd) driver for stork.

## Run Stork in your Kubernetes cluster
You can either update the default kube scheduler to use stork or start a new
scheduler instance which can use stork. 
Once this has been deployed the scheduler can be used to schedule any pods with the added advantage that it will
also try to optimize the storage requirements for the pod.

### Start a new scheduler instance and configure it with Stork (recommended)
You might not always have access to your default scheduler to update it's config options.
So the recommended way to start stork is to launch another instance of the scheduler and configure it to use stork

In order to run stork in your Kubernetes cluster, just create the deployment specified in the config above in a Kubernetes cluster:

```
# kubectl create -f stork-deployment.yaml
```

Verify that the stork pod is running:

```
# kubectl get pods --namespace=kube-system
NAME                              READY     STATUS    RESTARTS   AGE
....
stork-6dc5d66997-4rs2w            1/1       Running   1          27m
stork-6dc5d66997-fl8wr            1/1       Running   1          27m
stork-6dc5d66997-xvnbj            1/1       Running   1          27m
....
```

We will then start a new scheduler instance here and configure it to use stork. We will call the new scheduler 'stork'.
This new scheduler instance is defined in [specs/stork-scheduler.yaml](https://raw.githubusercontent.com/libopenstorage/stork/master/specs/stork-scheduler.yaml).
This spec starts 3 replicas of the scheduler.

You will need to update the version of kube scheduler that you want to use. This should be the same version as your kubernetes cluster. 
Example for Kubernetes v1.8.1 it would be:
```
image: gcr.io/google_containers/kube-scheduler-amd64:v1.8.1
```

You can deploy it by running the following command:
```
# kubectl create -f stork-scheduler.yaml
```
Verify that the scheduler pods are running:

```
# kubectl get pods --namespace=kube-system
NAME                              READY     STATUS    RESTARTS   AGE
....
stork-scheduler-9d6cb4546-gqdq2   1/1       Running   0          32m
stork-scheduler-9d6cb4546-k4z8t   1/1       Running   0          32m
stork-scheduler-9d6cb4546-tfkh4   1/1       Running   0          30m
....
```

### Configure your default scheduler with Stork
When using stork with the default scheduler, stork needs to be run as a deamon set. This is to avoid a deadlock
when trying to schedule the stork pods from the scheduler.

First create the stork daemonset defined in [specs/stork-daemonset.yaml](https://raw.githubusercontent.com/libopenstorage/stork/master/specs/stork-daemonset.yaml)

```
# kubectl create -f stork-daemonset.yaml
```

Verify that the stork pod is running:

```
# kubectl get pods --namespace=kube-system
NAME                              READY     STATUS    RESTARTS   AGE
....
stork-6dc5d66997-4rs2w            1/1       Running   1          27m
stork-6dc5d66997-fl8wr            1/1       Running   1          27m
stork-6dc5d66997-xvnbj            1/1       Running   1          27m
....
```

To configure your default scheduler to use stork add the following arguments to the scheduler and restart the scheduler if required:
```
--policy-configmap=stork-config --policy-configmap-namespace=kube-system
```

You will also need to make sure that the kube-scheduler clusterrole has permissions to read config maps. If not, run the following command:
```
# kubectl edit clusterrole -n kube-system system:kube-scheduler
```

And add the following permissions:
```
- apiGroups: ['']
  resources: ['configmaps']
  verbs: ['get']
```

## Specify the Stork scheduler for pods
In order to schedule a given pod using the Stork scheduler, specify the name of the scheduler in that pod spec:

```yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
   name: mysql-data
   annotations:
     volume.beta.kubernetes.io/storage-class: px-mysql-sc
spec:
   accessModes:
     - ReadWriteOnce
   resources:
     requests:
       storage: 2Gi
---
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
    name: px-mysql-sc
provisioner: kubernetes.io/portworx-volume
parameters:
   repl: "2"
---
apiVersion: apps/v1beta1
kind: Deployment
metadata:
  name: mysql
spec:
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 1
    type: RollingUpdate
  replicas: 1
  template:
    metadata:
      labels:
        app: mysql
        version: "1"
    spec:
      schedulerName: stork
      containers:
      - image: mysql:5.6
        name: mysql
        env:
        - name: MYSQL_ROOT_PASSWORD
          value: password
        ports:
        - containerPort: 3306
        volumeMounts:
        - name: mysql-persistent-storage
          mountPath: /var/lib/mysql
      volumes:
      - name: mysql-persistent-storage
        persistentVolumeClaim:
          claimName: mysql-data
```

The above spec will create a mysql pod with a Portworx volume having 2 replicas.
The pod will then get scheduled on a node in the cluster where one of the replicas is located.
If one of those nodes does not have enough cpu or memory resources then it will get scheduled on any other node in the cluster
where the driver (in this case Portworx) is running.

