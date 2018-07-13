% STORK (1) Container Image Pages
% Portworx Inc.
% March 19, 2018

# NAME
STORK - Storage Orchestration Runtime for Kubernetes

# DESCRIPTION
Stork is a Cloud Native storage orchestration runtime scheduler plugin. It translates a scheduler's
orchestration decisions into someting that an external cloud native storage solution can act upon.
By doing so, it extends Kubernetes with more stateful awareness of the underlying storage provider,
it's capabilities and state.

# USAGE
Steps to run STORK can be found here:
https://github.com/libopenstorage/stork#run-stork-in-your-kubernetes-cluster

# SECURITY IMPLICATIONS
Stork is a scheduler extender for kubernetes and needs to listen on a port for
requests from the scheduler. The following docker options needs to be passed in
to map ports between the container and host:

-p 8099:8099
    Opens container port 8099 and maps it to the same port on the Host.

# SEE ALSO
Source code: https://github.com/libopenstorage/stork
