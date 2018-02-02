# Litmus Test

This program is the most basic program you should run before even running a complete torpedo test suite.  It performs the most basic sanity check - does the storage backend even honor the `sync` command.  That is, will it persist data to disk, or is it cheating by holding data in memory for performance.  If a storage backend fails with this test, it is unsuitable for any production workload. 
