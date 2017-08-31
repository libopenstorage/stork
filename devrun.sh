# This sets up the env vars to test torpedo directly as a binary.

export CLUSTER_NODES="172.31.18.146,172.31.19.148,172.31.5.96"

./torpedo swarm pxd
