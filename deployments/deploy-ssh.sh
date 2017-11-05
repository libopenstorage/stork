#!/bin/bash -x

EXTRA_ARGS=""

if [ -n "${VERBOSE}" ]; then
    VERBOSE="--v"
fi

if [ -z "${SCALE_FACTOR}" ]; then
    SCALE_FACTOR="10"
fi

if [ -n "${SKIP_TESTS}" ]; then
    EXTRA_ARGS="--skip=$SKIP_TESTS $EXTRA_ARGS"
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
apiVersion: rbac.authorization.k8s.io/v1alpha1
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
apiVersion: rbac.authorization.k8s.io/v1alpha1
metadata:
  name: torpedo-role-binding
subjects:
- apiVersion: v1
  kind: ServiceAccount
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
            "$EXTRA_ARGS",
             "--slowSpecThreshold", "600",
            "bin/basic.test",
            "bin/reboot.test",
            "bin/scale.test",
            "--",
            "--spec-dir", "../drivers/scheduler/k8s/specs",
            "--node-driver", "ssh",
            "--scale-factor", "$SCALE_FACTOR" ]
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
        exit 1
    fi
  fi

  sleep 1
done

echo "Failed to wait for torpedo to start running..."
kubectl describe pod torpedo
exit 1
