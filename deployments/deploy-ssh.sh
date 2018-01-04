#!/bin/bash -x

if [ -n "${VERBOSE}" ]; then
    VERBOSE="--v"
fi

if [ -z "${SCALE_FACTOR}" ]; then
    SCALE_FACTOR="10"
fi

SKIP_ARG=""
if [ -n "${SKIP_TESTS}" ]; then
    SKIP_ARG="--skip=$SKIP_TESTS"
fi

FOCUS_ARG=""
if [ -n "${FOCUS_TESTS}" ]; then
    FOCUS_ARG="--focus=$FOCUS_TESTS"
fi

UPGRADE_VERSION_ARG=""
if [ -n "${STORAGE_UPGRADE_VERSION}" ]; then
    UPGRADE_VERSION_ARG="--storage-driver-upgrade-version=$STORAGE_UPGRADE_VERSION"
fi

UPGRADE_BASE_VERSION_ARG=""
if [ -n "${STORAGE_BASE_VERSION}" ]; then
    UPGRADE_BASE_VERSION_ARG="--storage-driver-base-version=$STORAGE_BASE_VERSION"
fi

if [ -z "${TORPEDO_IMG}" ]; then
    TORPEDO_IMG="portworx/torpedo:latest"
    echo "Using default torpedo image: ${TORPEDO_IMG}"
fi

kubectl delete pod torpedo || true

echo "Deploying torpedo pod..."
cat <<EOF | kubectl create -f -
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: torpedo-account
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
   name: torpedo-role
rules:
  -
    apiGroups:
      # have access to everything except Secrets
      - "*"
    resources: ["*"]
    verbs: ["*"]
  - nonResourceURLs: ["*"]
    verbs: ["*"]
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: torpedo-role-binding
subjects:
- kind: ServiceAccount
  name: torpedo-account
  namespace: default
roleRef:
  kind: ClusterRole
  name: torpedo-role
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: v1
kind: Pod
metadata:
  name: torpedo
spec:
  tolerations:
  - key: node-role.kubernetes.io/master
    operator: Equal
    effect: NoSchedule
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: node-role.kubernetes.io/master
            operator: Exists
  containers:
  - name: torpedo
    image: ${TORPEDO_IMG}
    command: [ "ginkgo" ]
    args: [ "$VERBOSE",
            "--trace",
            "--failFast",
            "$SKIP_ARG",
             "--slowSpecThreshold", "600",
            "bin/basic.test",
            "bin/reboot.test",
            "bin/scale.test",
            "bin/upgrade.test",
            "--",
            "--spec-dir", "../drivers/scheduler/k8s/specs",
            "--node-driver", "ssh",
            "--scale-factor", "$SCALE_FACTOR",
            "$UPGRADE_VERSION_ARG",
            "$UPGRADE_BASE_VERSION_ARG" ]
    tty: true
    env:
    - name: TORPEDO_SSH_USER
      value: "root"
    - name: TORPEDO_SSH_PASSWORD
      value: "${TORPEDO_SSH_PASSWORD}"
  restartPolicy: Never
  serviceAccountName: torpedo-account
EOF

echo "Waiting for torpedo to start running"

function debug_log_then_exit {
  echo "Pod description:"
  kubectl describe pod torpedo
  echo "Pod logs:"
  kubectl logs torpedo
  exit 1
}

for i in $(seq 1 300) ; do
  printf .
  state=`kubectl get pod torpedo | grep -v NAME | awk '{print $3}'`
  if [ "$state" == "Running" ] || [ "$state" == "Completed" ]; then
    echo ""
    kubectl logs -f torpedo
    sleep 5
    endState=`kubectl get pod torpedo | grep -v NAME | awk '{print $3}'`
    if [ "$endState" == "Running" ] || [ "$endState" == "Completed" ]; then
        echo "Success: Torpedo finished with $endState state"
        exit 0
    else
        echo "Error: Torpedo finished with $endState state"
        debug_log_then_exit
    fi
  fi

  sleep 1
done

echo "Error: Failed to wait for torpedo to start running..."
debug_log_then_exit
