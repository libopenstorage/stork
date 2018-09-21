# Snapshots with Stork

## Creating snapshots

This document will show you how to create snapshot of a PVC.

## Creating snapshot within a single namespace

If you have a PVC called mysql-data, you can create a snapshot for that PVC by using the following spec:

```
apiVersion: volumesnapshot.external-storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: mysql-snapshot
  namespace: default
spec:
  persistentVolumeClaimName: mysql-data
```

Once you apply the above object you can check the status of the snapshots using `kubectl`:

```
$ kubectl get volumesnapshot
NAME                             AGE
volumesnapshots/mysql-snapshot   2s
```

```
$ kubectl get volumesnapshotdatas
NAME                                                                            AGE
volumesnapshotdatas/k8s-volume-snapshot-2bc36c2d-227f-11e8-a3d4-5a34ec89e61c    1s
```

The creation of the volumesnapshotdatas object indicates that the snapshot has been created. If you describe the volumesnapshotdatas object you can see the Volume Snapshot ID and the PVC for which the snapshot was created.

```
$ kubectl describe volumesnapshotdatas 
Name:         k8s-volume-snapshot-2bc36c2d-227f-11e8-a3d4-5a34ec89e61c
Namespace:    
Labels:       <none>
Annotations:  <none>
API Version:  volumesnapshot.external-storage.k8s.io/v1
Kind:         VolumeSnapshotData
Metadata:
  Cluster Name:                   
  Creation Timestamp:             2018-03-08T03:17:02Z
  Deletion Grace Period Seconds:  <nil>
  Deletion Timestamp:             <nil>
  Resource Version:               29989636
  Self Link:                      /apis/volumesnapshot.external-storage.k8s.io/v1/k8s-volume-snapshot-2bc36c2d-227f-11e8-a3d4-5a34ec89e61c
  UID:                            2bc3a203-227f-11e8-98cc-0214683e8447
Spec:
  Persistent Volume Ref:
    Kind:  PersistentVolume
    Name:  pvc-f782bf5c-20e7-11e8-931d-0214683e8447
  Portworx Volume:
    Snapshot Id:  991673881099191762
  Volume Snapshot Ref:
    Kind:  VolumeSnapshot
    Name:  default/mysql-snapshot-2b2150dd-227f-11e8-98cc-0214683e8447
Status:
  Conditions:
    Last Transition Time:  <nil>
    Message:               
    Reason:                
    Status:                
    Type:                  
  Creation Timestamp:      <nil>
Events:                    <none>
```

To create PVCs from existing snapshots, read [Creating PVCs from snapshots](/scheduler/kubernetes/snaps-local.html#pvc-from-snap).

## Creating snapshots across namespaces

* When creating snapshots, you can provide comma separated regexes with `stork/snapshot-restore-namespaces` annotation to specify which namespaces the snapshot can be restored to.
* When creating PVC from snapshots, if a snapshot exists in another namespace, the snapshot namespace should be specified with `stork/snapshot-source-namespace` annotation.

Let's take an example where we have 2 namespaces _dev_ and _prod_. We will create a PVC and snapshot in the _dev_ namespace and then create a PVC in the _prod_ namespace from the snapshot.

Create the namespaces

```
apiVersion: v1
kind: Namespace
metadata:
  name: dev
  labels:
    name: dev
---
apiVersion: v1
kind: Namespace
metadata:
  name: prod
  labels:
    name: prod
```

Create the PVC

```
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: mysql-data
  namespace: dev
  annotations:
    volume.beta.kubernetes.io/storage-class: mysql-sc
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
  name: mysql-sc
provisioner: kubernetes.io/portworx-volume
parameters:
  repl: "2"
```

Create the snapshot

```
apiVersion: volumesnapshot.external-storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: mysql-snapshot
  namespace: dev
  annotations:
    stork/snapshot-restore-namespaces: "prod"
spec:
  persistentVolumeClaimName: mysql-data

```

Create a PVC in a different namespace from the snapshot

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-clone
  namespace: prod
  annotations:
    snapshot.alpha.kubernetes.io/snapshot: mysql-snapshot
    stork/snapshot-source-namespace: dev
spec:
  accessModes:
     - ReadWriteOnce
  storageClassName: stork-snapshot-sc
  resources:
    requests:
      storage: 2Gi
```
## Creating PVCs from snapshots

When you install STORK, it also creates a storage class called _stork-snapshot-sc_. This storage class can be used to create PVCs from snapshots.

To create a PVC from a snapshot, you would add the `snapshot.alpha.kubernetes.io/snapshot` annotation to refer to the snapshot
name.

For the above snapshot, the spec would like this:
```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-snap-clone
  annotations:
    snapshot.alpha.kubernetes.io/snapshot: mysql-snapshot
spec:
  accessModes:
     - ReadWriteOnce
  storageClassName: stork-snapshot-sc
  resources:
    requests:
      storage: 2Gi
```

Once you apply the above spec you will see a PVC created by STORK.

```
$ kubectl get pvc  
NAMESPACE   NAME                                   STATUS    VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                AGE
default     mysql-data                             Bound     pvc-f782bf5c-20e7-11e8-931d-0214683e8447   2Gi        RWO            mysql-sc                 2d
default     mysql-snap-clone                       Bound     pvc-05d3ce48-2280-11e8-98cc-0214683e8447   2Gi        RWO            stork-snapshot-sc           2s
```

If you had taken snapshots of a group of PVCs, the process is the same as above. So corresponding to each volumesnapshot, you will create a PVC.