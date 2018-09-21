Stork supports specifying pre and post rules that are run on the application pods using the volumes being snapshotted. This allows users to quiesce the applications before the snapshot is taken and resume I/O after the snapshot is taken.

The high level workflow for configuring 3DSnaps involves creating rules and later on referencing the rules when creating the snapshots.

## Step 1: Create Rules

A Stork `Rule` is a Custom Resource Definition (CRD) that allows to define actions that get performed on pods matching selectors. Below are the supported fields:

* **podSelector**: The actions will get executed on pods that only match the label selectors given here. 
* **actions**: This contains a list of actions to be performed. Below are supported fields under actions:
    * **type**: The type of action to run. Only type _command_ is supported.
    * **background**: If _true_, the action will run in background and will be terminated by Stork after the snapshot has been initiated. If false, the action will first complete and then the snapshot will get initiated.
      * If background is set to _true_, add `${WAIT_CMD}` as shown in the examples below. This is a placeholder and Stork will replace it with an appropriate command to wait for the command is done.
    * **value**: This is the actual action content. For example, the command to run.
    * **runInSinglePod**: If _true_, the action will be run on a single pod that matches the selectors.

## Step 2: Create VolumeSnapshots that reference the rules

Once you have the rules applied in your cluster, you can reference them in the `VolumeSnapshot` using the following annotations.

* __stork.rule/pre-snapshot__: Stork will execute the rule which is given in the value of this annotation _before_ taking the snapshot.
* __stork.rule/post-snapshot__: Stork will execute the rule which is given in the value of this annotation _after_ taking the snapshot.

## Examples

This section covers examples of creating 3DSnapshots for various applications.

### Hello world

Below rule is a generic example on how to run an echo command on a single pod that matches the label selector app=foo.
```
apiVersion: stork.libopenstorage.org/v1alpha1
kind: Rule
metadata:
  name: hello-world-rule
spec:
  - podSelector:
      app: foo
    actions:
    - type: command
      value: echo "hello world"
      runInSinglePod: true
```

### Mysql

**Pre-snapshot rule**

Below rule will flush tables on all mysql pods that match label app=mysql and take a read lock on the tables.
```
apiVersion: stork.libopenstorage.org/v1alpha1
kind: Rule
metadata:
  name: presnap-rule
spec:
  - podSelector:
      app: mysql
    actions:
    - type: command
      background: true
      # this command will flush tables with read lock
      value: mysql --user=root --password=$MYSQL_ROOT_PASSWORD -Bse 'flush tables with read lock;system ${WAIT_CMD};'
```

**Snapshot**

Creating the below VolumeSnapshot will do the following:

* Stork will run the _presnap-rule_ rule on the pod that's using the _mysql-data_ PVC.
* Once the rule is executed, Stork will take a snapshot of the _mysql-data_ PVC.
* After the snapshot has been triggered, Stork will terminate any background actions that may exist in the rule _presnap-rule_.

```
apiVersion: volumesnapshot.external-storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: mysql-3d-snapshot
  annotations:
    stork.rule/pre-snapshot: presnap-rule
spec:
  persistentVolumeClaimName: mysql-data
```

### MongoDB

**Pre-snapshot rule**

Below pre-snapshot rule forces the mongod to flush all pending write operations to disk and locks the entire mongod instance to prevent additional writes until the user releases the lock with a corresponding db.fsyncUnlock() command. (See [reference](https://docs.mongodb.com/manual/reference/method/db.fsyncLock/))
```
apiVersion: stork.libopenstorage.org/v1alpha1
kind: Rule
metadata:
  name: mongodb-presnap-rule
spec:
  - podSelector:
      app: mongo-mongodb
    actions:
    - type: command
      value: mongo --eval "printjson(db.fsyncLock())"
```

**Post-snapshot rule**

Below post-snapshot rule reduces the lock taken by db.fsyncLock() on a mongod instance by 1. (See [reference](https://docs.mongodb.com/manual/reference/method/db.fsyncUnlock/#db.fsyncUnlock))
```
apiVersion: stork.libopenstorage.org/v1alpha1
kind: Rule
metadata:
  name: mongodb-postsnap-rule
spec:
  - podSelector:
      app: mongo-mongodb
    actions:
    - type: command
      value: mongo --eval "printjson(db.fsyncUnlock())"
```

**Snapshot**

Creating the below VolumeSnapshot will do the following:

* Stork will run the _mongodb-presnap-rule_ rule on pod using the _mongo-pvc_ PVC.
* Once the rule is executed, Stork will take a snapshot of the _mongo-pvc_ PVC.
* After the snapshot has been triggered, Stork will run the _mongodb-postsnap-rule_ rule on pod using the _mongo-pvc_ PVC.

```
apiVersion: volumesnapshot.external-storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: mongodb-3d-snapshot
  annotations:
    stork.rule/pre-snapshot: mongodb-presnap-rule
    stork.rule/post-snapshot: mongodb-postsnap-rule
spec:
  persistentVolumeClaimName: mongo-pvc
```

### Cassandra

**Pre-snapshot rule**

Below rule flushes the tables from the memtable on all cassandra pods.
```
apiVersion: stork.libopenstorage.org/v1alpha1
kind: Rule
metadata:
  name: cassandra-rule
spec:
  - podSelector:
      app: cassandra
    actions:
    - type: command
      value: nodetool flush
```

**Snapshot**

With this snapshot, Stork will run the _cassandra-rule_ rule on all pods that are using PVCs that match labels _app=cassandra_. Hence this will be a [group snapshot](/scheduler/kubernetes/snaps-group.html).

```
apiVersion: volumesnapshot.external-storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: cassandra-3d-snapshot
  annotations:
    portworx.selector/app: cassandra
    stork.rule/pre-snapshot: cassandra-rule
spec:
  persistentVolumeClaimName: cassandra-data-1
```

To create PVCs from existing snapshots, read [Creating PVCs from snapshots](/scheduler/kubernetes/snaps-local.html#pvc-from-snap).