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
