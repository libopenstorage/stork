[![Build Status](https://travis-ci.org/libopenstorage/stork.svg?branch=master)](https://travis-ci.org/libopenstorage/stork)
[![Go Report Card](https://goreportcard.com/badge/github.com/libopenstorage/stork)](https://goreportcard.com/report/libopenstorage/stork)

# Stork - Storage Orchestration Runtime for Kubernetes
Stork is a Cloud Native storage orchestration runtime scheduler plugin. It translates a scheduler's orchestration decisions into someting that an external cloud native storage solution can act upon. By doing so, it extends Kubernetes with more stateful awareness of the underlying storage providor, it's capabilities and state.

<div style="text-align:center"><img src="images/stork.png" alt="Drawing" style="width: 50px;"/></div>

Stork is intended to allow storage operators such as Portworx, EMC-RexRay, and Kubernetes Local Storage to extend upon scheduler actions and allow for a storage-implementation specific orchestration actions around what the orchestrator is trying to do. The most basic example is when the scheduler is trying to spawn a container that is part of a pod - Stork will allow for the storage provider to specify an appropriate node on which that container needs to run such that it's data access is local to the runtime of the contaner. This is one of many orchestration scenarios that is adressed by this project.

## Stork as a Kubernetes Scheduler Extension
Stork is implemented as a Kubernetes native scheduler extension. It registers with the Kubernetes scheduler and helps filter and prioritize pods to provide optimal placement for storage providers.

# Building Stork
Stork is written in Golang. To build Stork:

```
# git clone git@github.com:libopenstorage/stork.git
# export DOCKER_HUB_REPO=myrepo
# export DOCKER_HUB_STORK_IMAGE=stork
# export DOCKER_HUB_TAG=latest
# make
```

This will create the Docker image `$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_TAG)`.

# Running Stork
Now that you have stork in a container image, you can just create a pod config for it and run it in your Kubernetes cluster. We do this via a deployment.

## Create a Deployment
A Deployment manages a Replica Set which in turn manages the pods, thereby making stork resilient to failures. The deployment spec is defined in [specs/stork-deployment.yaml](specs/stork-deployment.yaml).
By default the deployment does the following
* Uses the latest stable image of stork to start a pod. You can update the tag to use a specific version or use your own stork image.
* Creates a service to provide an endpoint that can be used to reach the extender. 
* Creates a ConfigMap which can be used by a scheduler to communicate with stork.
* Uses the Portworx (pxd) driver for stork.

## Run Stork in the Kubernetes
In order to run stork in your Kubernetes cluster, just create the deployment specified in the config above in a Kubernetes cluster:

```
# kubectl create -f stork-deployment.yaml
```

Verify that the stork pods are running:

```
# kubectl get pods --namespace=kube-system
NAME                              READY     STATUS    RESTARTS   AGE
....
stork-98b64f47f-5wh2b             1/1       Running   0          27m
stork-98b64f47f-fjknb             1/1       Running   0          27m
stork-98b64f47f-wdzmj             1/1       Running   0          27m
....
```

## Specify Stork to be used by a scheduler
You can either update the default kube scheduler to use stork or start a new
scheduler instance which can use stork. We will start a new scheduler instance here and
configure it to use stork. We will call the new scheduler 'stork'.

This new scheduler instance is defined in [specs/stork-scheduler.yaml](specs/stork-scheduler.yaml).
You will just need to update the version of kube scheduler that you want to use. This should be the same version as your kubernetes cluster. 
Example for Kubernetes v1.8.1 it would be:
```
image: gcr.io/google_containers/kube-scheduler-amd64:v1.8.1
```

You can deploy it by running the following command:
```
# kubectl create -f stork-scheduler.yaml
```
Verify that the scheduler pod is running:

```
# kubectl get pods --namespace=kube-system
NAME                              READY     STATUS    RESTARTS   AGE
....
stork-scheduler-b77668f67-tjrfr   1/1       Running   0          19m
....
```

Once this has been deployed the scheduler can be used to schedule any pods with the added advantage that it will
also try to optimize the storage requirements for the pod.

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

