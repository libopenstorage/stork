# Litmus Test

This program is the most basic program you should run before even running a complete torpedo test suite.  It performs the most basic sanity check - does the storage backend even honor the `sync` command.  That is, will it persist data to disk, or is it cheating by holding data in memory for performance.  If a storage backend fails with this test, it is unsuitable for any production workload. 

# Running Litmus in Docker
#
### Step 1: Execute the test


Create a volume for your storage backend.  Let's call the volume `vol` in this example.
```
$ docker volume create --driver=XYZ vol
```

Now run the test.  Note: This will panic and restart your server!
```
$ docker run --privileged --rm -it -v vol:/test gourao/litmus litmus run /test/foo.txt
```

### Step 2: Ensure that you do not have any data loss
After your machine restarts, verify that there is no data loss or corruption

```
$ docker run --privileged --rm -it -v vol:/test gourao/litmus litmus verify /test/foo.txt
```

# Running Litmus in Kubernetes

An example of running the litmus test is available via `litmus-run.yaml` and `litmus-verify.yaml`

### How to run

```
kubectl create -f litmus-run.yaml
```

You should see the `ContainerCreating`

> Note: This pod will likely `Error` out with some arbitrary error as the run will make the node panic, and therefore the pod never fully completes, this is ok and expected.

```
▶ kubectl get po
NAME                     READY     STATUS              RESTARTS   AGE
litmus-run-q28zl             0/1       ContainerCreating   0          3m
```

You should also notice a worker node in `NotReady` state as it reboots.

```
▶ kubectl get no
NAME                            STATUS     ROLES     AGE       VERSION
ip-172-31-37-92.ec2.internal    Ready      node      2h        v1.10.2
ip-172-31-48-7.ec2.internal     NotReady   node      2h        v1.10.2
ip-172-31-51-193.ec2.internal   Ready      master    2h        v1.10.2
ip-172-31-59-143.ec2.internal   Ready      master    2h        v1.10.2
ip-172-31-59-61.ec2.internal    Ready      master    2h        v1.10.2
ip-172-31-79-153.ec2.internal   Ready      node      2h        v1.10.2
```

### How to verify

Once the node is back in `Ready` state, you can run the verify step.

```
kubectl delete po litmus-run-q28zl
kubectl create -f litmus-verify.yaml
```

Verify the output of the test.

```
▶ kubectl logs litmus-verify-d4kkb
/test/foo.txt has no corruption
```

# Building from source
If you want to build the binaries from source, follow these instructions.

## Building
```
$ gcc litmus.c -o litmus
```

## Running
Note - your system *will* panic during the test.  When it restarts, you need to ensure that the contents of your target file contain the string `Hello World`.  If it does not, you should not use this storage backend.


```
$ ./litmus run <path-to-target-storage/test.file
# --- system will panic and reboot here
# --- after reboot, verify contents of test.file
$ cat test.file
Hello World
$ --- or
$ ./litmus verify <path-to-target-storage/test.file
```
