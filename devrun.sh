# This sets up the env vars to test torpedo directly as a binary.

export DOCKER_HOST="unix:///var/run/docker.sock"
export CLUSTER_NODES="172.31.18.146,72.31.19.148,172.31.5.96"

./torpedo docker pxd
