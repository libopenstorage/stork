## Main controller loop

Below is the high level controller loop

* End user updates the Portworx custom resource using clients (e.g kubectl)
* Portworx operator is watching on changes to the Portworx custom resources using a watch
* There are 3 kinds of changes
    1. Add
    2. Update
    3. Delete
* For each of the above, the controller loop parses the required spec change and pushes a task in the workqueue
* Based on configured threadiness, N workers are constanly pulling tasks off the work queue
* When a task is pulled off the work queue, the worker checks the existing Portworx cluster state and updates the Portworx objects. For example:
    * If the PX cluster doesn't exist, it will deploy the PX DaemonSet
    * If the PX cluster version changed, it will perform all actions needed for a Rolling upgrade
    * If the PX cluster deleted, it will delete all PX components (DaemonSet, PV binder, stork, PX mon etc)

```

       ┌─────────┐
       │End user │
       └─────────┘
            │
            │
            │
        Update PX
         Cluster
            │
   ┌────────▼───────┐
   │████████████████│
   │█Kubernetes API │
   │█████server█████│◀──────────────Create/Update/Delete PX objects────────────────────────────────┐
   │████████████████│                                                                              │
   └────────┬───────┘                                                                              │
            │                                                                                      │
            │                                                                                      │
            │                                                                                      │
            │                                                                                      │
            │                                                                                      │
            │                                                                                      │
            │                   ┌ ─ ─ ─ ┐  │                                                       │
     ┌──────▼─────┐    ┌ ─ ─ ─ ▶   Add                                                             │
     │            │             └ ─ ─ ─ ┘  │                  ┌────────────────┐             ┌──────────┐
     │Watch on PX │    │        ┌ ─ ─ ─ ┐                     │   Work queue   │             │ ┌────────┴─┐
     │    CRD     │────◎─ ─ ─ ─▶ Update    │─────────────────▶│                │────────────▶│ │          │
     │            │    │        └ ─ ─ ─ ┘        Enqueue      └────────────────┘             │ │ Workers  │
     └────────────┘             ┌ ─ ─ ─ ┐  │     task in                                     └─┤          │
                       └ ─ ─ ─ ▶ Delete           queue                                        └──────────┘
                                └ ─ ─ ─ ┘  │
```
