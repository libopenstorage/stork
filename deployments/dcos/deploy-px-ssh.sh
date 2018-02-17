#!/bin/bash -x

if [ -n "${VERBOSE}" ]; then
    VERBOSE="--v"
fi

if [ -n "${SKIP_TESTS}" ]; then
    SKIP_ARG="--skip='$SKIP_TESTS'"
fi

if [ -n "${FOCUS_TESTS}" ]; then
    FOCUS_ARG="--focus='$FOCUS_TESTS'"
fi

if [ -z "${TORPEDO_IMG}" ]; then
    TORPEDO_IMG="portworx/torpedo:latest"
    echo "Using default torpedo image: ${TORPEDO_IMG}"
fi

if [ -z "${TORPEDO_SSH_USER}" ]; then
    TORPEDO_SSH_USER="vagrant"
fi

if [ -z "${TORPEDO_SSH_PASSWORD}" ]; then
    TORPEDO_SSH_PASSWORD="vagrant"
fi

dcos_version=`dcos --version | grep "dcos.version"| cut -d'=' -f2`
if [ "${dcos_version}" = "`echo -e "${dcos_version}\n1.10.4" | sort -V | tail -n1`" ]; then
    FORCE_PULL_IMAGE=", \"forcePullImage\": true"
fi

echo "Deploying torpedo job..."
cat <<EOF > torpedo.json
{
    "id": "torpedo",
    "description": "Run Torpedo on DC/OS",
    "run": {
        "cpus": 0.5,
        "mem": 512,
        "docker": {
            "image": "${TORPEDO_IMG}" $FORCE_PULL_IMAGE
        },
        "cmd": "ginkgo $VERBOSE --trace --failFast $SKIP_ARG $FOCUS_ARG --slowSpecThreshold 600 bin/basic.test bin/reboot.test bin/scale.test bin/upgrade.test bin/drive_failure.test -- --spec-dir ../drivers/scheduler/dcos/specs --scheduler dcos --node-driver ssh",
        "env": {
            "TORPEDO_SSH_USER": "${TORPEDO_SSH_USER}",
            "TORPEDO_SSH_PASSWORD": "${TORPEDO_SSH_PASSWORD}"
        },
        "volumes": [
            {
                "containerPath": "/var/run/docker.sock",
                "hostPath": "/var/run/docker.sock",
                "mode": "RW"
            }
        ],
        "restart": {
            "policy": "NEVER"
        }
    }
}
EOF

dcos job add torpedo.json
dcos job run torpedo
