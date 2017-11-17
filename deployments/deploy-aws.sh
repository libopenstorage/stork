#!/bin/bash -x

EXTRA_ARGS=""

if [ -n "${VERBOSE}" ]; then
    VERBOSE="--v"
fi

if [ -n "${SKIP_TESTS}" ]; then
    EXTRA_ARGS="--skip=$SKIP_TESTS $EXTRA_ARGS"
fi

if [ -z "${SCALE_FACTOR}" ]; then
    SCALE_FACTOR="10"
fi

if [ -z "${TORPEDO_IMG}" ]; then
    TORPEDO_IMG="portworx/torpedo:latest"
    echo "Using default torpedo image: ${TORPEDO_IMG}"
fi

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "[ERROR] AWS_ACCESS_KEY_ID environment variable not set."
    exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "[ERROR] AWS_SECRET_ACCESS_KEY environment variable not set."
    exit 1
fi

if [ -z "$AWS_REGION" ]; then
    echo "[ERROR] AWS_REGION environment variable not set."
    exit 1
fi

kubectl delete secret aws-access || true
kubectl delete pod torpedo || true

echo "Creating AWS secret..."
kubectl create secret generic aws-access \
        --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
        --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
        --from-literal=AWS_REGION=$AWS_REGION


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
            "--node-driver", "aws",
            "--scale-factor", "$SCALE_FACTOR" ]
    tty: true
    env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: aws-access
            key: AWS_ACCESS_KEY_ID
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: aws-access
            key: AWS_SECRET_ACCESS_KEY
      - name: AWS_REGION
        valueFrom:
          secretKeyRef:
            name: aws-access
            key: AWS_REGION
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
