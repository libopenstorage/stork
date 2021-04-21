#!/bin/bash -x

if [ -n "${VERBOSE}" ]; then
    VERBOSE="--v"
fi

if [ -z "${SCALE_FACTOR}" ]; then
    SCALE_FACTOR="10"
fi

if [ -z "${VOLUME_PROVIDER}" ]; then
    VOLUME_PROVIDER="none"
fi

if [ -z "${OBJECT_STORE_PROVIDER}" ]; then
    OBJECT_STORE_PROVIDER="aws"
fi

if [ -z "${SPEC_DIR}" ]; then
    SPEC_DIR="../drivers/scheduler/k8s/specs"
fi

if [ -z "${SCHEDULER}" ]; then
    SCHEDULER="k8s"
fi

if [ -z "${LOGLEVEL}" ]; then
    LOGLEVEL="debug"
fi

if [ -z "${CHAOS_LEVEL}" ]; then
    CHAOS_LEVEL="5"
fi
if [ -z "${MIN_RUN_TIME}" ]; then
    MIN_RUN_TIME="0"
fi

if [[ -z "$FAIL_FAST" || "$FAIL_FAST" = true ]]; then
    FAIL_FAST="--failFast"
else
    FAIL_FAST="-keepGoing"
fi

SKIP_ARG=""
if [ -n "$SKIP_TESTS" ]; then
    skipRegex=$(echo $SKIP_TESTS | sed -e 's/,/}|{/g')
    SKIP_ARG="--skip={$skipRegex}"
fi

FOCUS_ARG=""
if [ -n "$FOCUS_TESTS" ]; then
    focusRegex=$(echo $FOCUS_TESTS | sed -e 's/,/}|{/g')
    FOCUS_ARG="--focus={$focusRegex}"
fi

if [ -z "${UPGRADE_ENDPOINT_URL}" ]; then
    UPGRADE_ENDPOINT_URL=""
fi

if [ -z "${UPGRADE_ENDPOINT_VERSION}" ]; then
    UPGRADE_ENDPOINT_VERSION=""
fi

if [ -z "${ENABLE_STORK_UPGRADE}" ]; then
    ENABLE_STORK_UPGRADE=false
fi

if [ -n "${PROVISIONER}" ]; then
    PROVISIONER="$PROVISIONER"
fi

if [ -z "${STORAGE_DRIVER}" ]; then
    STORAGE_DRIVER="pxd"
fi

if [ -z "${MAX_STORAGE_NODES_PER_AZ}" ]; then
    MAX_STORAGE_NODES_PER_AZ="2"
fi

if [ -z "${PROVISIONER}" ]; then
    PROVISIONER="portworx"
fi

CONFIGMAP=""
if [ -n "${CONFIG_MAP}" ]; then
    CONFIGMAP="${CONFIG_MAP}"
fi

if [ -z "${TORPEDO_IMG}" ]; then
    TORPEDO_IMG="portworx/torpedo:latest"
    echo "Using default torpedo image: ${TORPEDO_IMG}"
fi

if [ -z "${TIMEOUT}" ]; then
    TIMEOUT="720h0m0s"
    echo "Using default timeout of ${TIMEOUT}"
fi

if [ -z "$DRIVER_START_TIMEOUT" ]; then
    DRIVER_START_TIMEOUT="5m0s"
    echo "Using default timeout of ${DRIVER_START_TIMEOUT}"
fi

if [ -z "$SECRET_TYPE" ]; then
    SECRET_TYPE="k8s"
    echo "Using default secret type of ${SECRET_TYPE}"
fi

APP_DESTROY_TIMEOUT_ARG=""
if [ -n "${APP_DESTROY_TIMEOUT}" ]; then
    APP_DESTROY_TIMEOUT_ARG="--destroy-app-timeout=$APP_DESTROY_TIMEOUT"
fi

if [ -z "$STORAGENODE_RECOVERY_TIMEOUT" ]; then
    STORAGENODE_RECOVERY_TIMEOUT="35m0s"
    echo "Using default storage node recovery timeout of ${STORAGENODE_RECOVERY_TIMEOUT}"
fi

AZURE_TENANTID=""
if [ -n "$AZURE_TENANT_ID" ]; then
    AZURE_TENANTID="${AZURE_TENANT_ID}"
fi

AZURE_CLIENTID=""
if [ -n "$AZURE_CLIENT_ID" ]; then
    AZURE_CLIENTID="${AZURE_CLIENT_ID}"
fi

AZURE_CLIENTSECRET=""
if [ -n "$AZURE_CLIENT_SECRET" ]; then
    AZURE_CLIENTSECRET="${AZURE_CLIENT_SECRET}"
fi

SCHEDULER_UPGRADE_HOPS_ARG=""
if [ -n "${SCHEDULER_UPGRADE_HOPS}" ]; then
    SCHEDULER_UPGRADE_HOPS_ARG="--sched-upgrade-hops=$SCHEDULER_UPGRADE_HOPS"
fi

CSI_GENERIC_CONFIGMAP=""
if [ -n "${CSI_GENERIC_DRIVER_CONFIGMAP}" ]; then
    CSI_GENERIC_CONFIGMAP="${CSI_GENERIC_DRIVER_CONFIGMAP}"
fi

for i in $@
do
case $i in
	--backup-driver)
	BACKUP_DRIVER=$2
	shift
	shift
	;;
esac
done

if [[ -z "$TEST_SUITE" || "$TEST_SUITE" == "" ]]; then
    TEST_SUITE='"bin/asg.test",
            "bin/autopilot.test",
            "bin/basic.test",
            "bin/backup.test",
            "bin/reboot.test",
            "bin/upgrade.test",
            "bin/drive_failure.test",
            "bin/volume_ops.test",
            "bin/sched.test",
            "bin/scheduler_upgrade.test",
            "bin/node_decommission.test",'
else
  TEST_SUITE=$(echo \"$TEST_SUITE\" | sed "s/,/\",\n\"/g")","
fi
echo "Using list of test suite(s): ${TEST_SUITE}"

if [ -z "${AUTOPILOT_UPGRADE_VERSION}" ]; then
    AUTOPILOT_UPGRADE_VERSION=""
fi

kubectl delete secret torpedo
kubectl delete pod torpedo
state=`kubectl get pod torpedo | grep -v NAME | awk '{print $3}'`
timeout=0
while [ "$state" == "Terminating" -a $timeout -le 600 ]; do
  echo "Terminating torpedo..."
  sleep 1
  state=`kubectl get pod torpedo | grep -v NAME | awk '{print $3}'`
  timeout=$[$timeout+1]
done

if [ $timeout -gt 600 ]; then
  echo "Torpedo is taking too long to terminate. Operation timeout."
  describe_pod_then_exit
fi

TORPEDO_CUSTOM_PARAM_VOLUME=""
TORPEDO_CUSTOM_PARAM_MOUNT=""
CUSTOM_APP_CONFIG_PATH=""
if [ -n "${CUSTOM_APP_CONFIG}" ]; then
    kubectl create configmap custom-app-config --from-file=custom_app_config.yml=${CUSTOM_APP_CONFIG}
    CUSTOM_APP_CONFIG_PATH="/mnt/torpedo/custom_app_config.yml"
    TORPEDO_CUSTOM_PARAM_VOLUME="{ \"name\": \"custom-app-config-volume\", \"configMap\": { \"name\": \"custom-app-config\", \"items\": [{\"key\": \"custom_app_config.yml\", \"path\": \"custom_app_config.yml\"}] } }"
    TORPEDO_CUSTOM_PARAM_MOUNT="{ \"name\": \"custom-app-config-volume\", \"mountPath\": \"${CUSTOM_APP_CONFIG_PATH}\", \"subPath\": \"custom_app_config.yml\" }"
fi

TORPEDO_SSH_KEY_VOLUME=""
TORPEDO_SSH_KEY_MOUNT=""
if [ -n "${TORPEDO_SSH_KEY}" ]; then
    kubectl create secret generic key4torpedo --from-file=${TORPEDO_SSH_KEY}
    TORPEDO_SSH_KEY_VOLUME="{ \"name\": \"ssh-key-volume\", \"secret\": { \"secretName\": \"key4torpedo\", \"defaultMode\": 256 }}"
    TORPEDO_SSH_KEY_MOUNT="{ \"name\": \"ssh-key-volume\", \"mountPath\": \"/home/torpedo/\" }"
fi

TESTRESULTS_VOLUME="{ \"name\": \"testresults\", \"hostPath\": { \"path\": \"/mnt/testresults/\", \"type\": \"DirectoryOrCreate\" } }"
TESTRESULTS_MOUNT="{ \"name\": \"testresults\", \"mountPath\": \"/testresults/\" }"

AWS_VOLUME="{ \"name\": \"aws-volume\", \"configMap\": { \"name\": \"aws-cm\", \"items\": [{\"key\": \"credentials\", \"path\": \"credentials\"}, {\"key\": \"config\", \"path\": \"config\"}]} }"
AWS_VOLUME_MOUNT="{ \"name\": \"aws-volume\", \"mountPath\": \"/root/.aws/\" }"

VOLUMES="${TESTRESULTS_VOLUME}"

if [ "${STORAGE_DRIVER}" == "aws" ]; then
  VOLUMES="${VOLUMES},${AWS_VOLUME}"
  VOLUME_MOUNTS="${VOLUME_MOUNTS},${AWS_VOLUME_MOUNT}"
fi

if [ -n "${PROVIDERS}" ]; then
  echo "Create configs for providers",${PROVIDERS}
  for i in ${PROVIDERS//,/ };do
     if [ "${i}" == "aws" ]; then
      VOLUMES="${VOLUMES},${AWS_VOLUME}"
      VOLUME_MOUNTS="${VOLUME_MOUNTS},${AWS_VOLUME_MOUNT}"
     fi
  done
fi

if [ -n "${TORPEDO_SSH_KEY_VOLUME}" ]; then
    VOLUMES="${VOLUMES},${TORPEDO_SSH_KEY_VOLUME}"
fi

VOLUME_MOUNTS="${TESTRESULTS_MOUNT}"

if [ -n "${TORPEDO_SSH_KEY_MOUNT}" ]; then
    VOLUME_MOUNTS="${VOLUME_MOUNTS},${TORPEDO_SSH_KEY_MOUNT}"
fi

if [ -n "${TORPEDO_CUSTOM_PARAM_VOLUME}" ]; then
    VOLUMES="${VOLUMES},${TORPEDO_CUSTOM_PARAM_VOLUME}"
fi

if [ -n "${TORPEDO_CUSTOM_PARAM_MOUNT}" ]; then
    VOLUME_MOUNTS="${VOLUME_MOUNTS},${TORPEDO_CUSTOM_PARAM_MOUNT}"
fi

BUSYBOX_IMG="busybox"
if [ -n "${INTERNAL_DOCKER_REGISTRY}" ]; then
    BUSYBOX_IMG="${INTERNAL_DOCKER_REGISTRY}/busybox"
    TORPEDO_IMG="${INTERNAL_DOCKER_REGISTRY}/${TORPEDO_IMG}"
fi

# List of additional kubeconfigs of k8s clusters to register with px-backup, px-dr
FROM_FILE=""
CLUSTER_CONFIGS=""
echo "Create kubeconfig configmap",${KUBECONFIGS}
if [ -n "${KUBECONFIGS}" ]; then
  for i in ${KUBECONFIGS//,/ };do
     FROM_FILE="${FROM_FILE} --from-file=${i}"
     if [[ -z ${CLUSTER_CONFIGS} ]]; then
       CLUSTER_CONFIGS="`basename ${i}`"
     else
       CLUSTER_CONFIGS="${CLUSTER_CONFIGS},`basename ${i}`"
     fi
  done
  kubectl create configmap kubeconfigs ${FROM_FILE}
fi

K8S_VENDOR_KEY=""
K8S_VENDOR_VALUE=""
K8S_VENDOR_OPERATOR="Exists"
if [ -z "${NODE_DRIVER}" ]; then
    NODE_DRIVER="ssh"
fi
if [ -n "${K8S_VENDOR}" ]; then
    case "$K8S_VENDOR" in
        kubernetes)
            K8S_VENDOR_KEY=node-role.kubernetes.io/master
            ;;
        rancher)
            K8S_VENDOR_KEY=node-role.kubernetes.io/controlplane
            K8S_VENDOR_OPERATOR="In"
            K8S_VENDOR_VALUE='values: ["true"]'
            ;;
        gke)
            # Run torpedo on worker node, where px installation is disabled.
            K8S_VENDOR_KEY=px/enabled
            K8S_VENDOR_OPERATOR="In"
            K8S_VENDOR_VALUE='values: ["false"]'
            NODE_DRIVER="gke"
            ;;
        aks)
            # Run torpedo on worker node, where px installation is disabled.
            K8S_VENDOR_KEY=px/enabled
            K8S_VENDOR_OPERATOR="In"
            K8S_VENDOR_VALUE='values: ["false"]'
            NODE_DRIVER="aks"
            ;;
        eks)
            # Run torpedo on worker node, where px installation is disabled.
            K8S_VENDOR_KEY=px/enabled
            K8S_VENDOR_OPERATOR="In"
            K8S_VENDOR_VALUE='values: ["false"]'
            ;;
    esac
else
    K8S_VENDOR_KEY=node-role.kubernetes.io/master
fi

cat > torpedo.yaml <<EOF
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
  labels:
    app: torpedo
spec:
  tolerations:
  - key: node-role.kubernetes.io/master
    operator: Equal
    effect: NoSchedule
  - key: node-role.kubernetes.io/controlplane
    operator: Equal
    value: "true"
  - key: node-role.kubernetes.io/etcd
    operator: Equal
    value: "true"
  - key: apps
    operator: Equal
    value: "false"
    effect: "NoSchedule"
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: ${K8S_VENDOR_KEY}
            operator: ${K8S_VENDOR_OPERATOR}
            ${K8S_VENDOR_VALUE}
  initContainers:
  - name: init-sysctl
    image: ${BUSYBOX_IMG}
    imagePullPolicy: IfNotPresent
    securityContext:
      privileged: true
    command: ["sh", "-c", "mkdir -p /mnt/testresults && chmod 777 /mnt/testresults/"]
  containers:
  - name: torpedo
    image: ${TORPEDO_IMG}
    imagePullPolicy: Always
    command: [ "ginkgo" ]
    args: [ "--trace",
            "--timeout", "${TIMEOUT}",
            "$FAIL_FAST",
            "--slowSpecThreshold", "600",
            "$VERBOSE",
            "$FOCUS_ARG",
            "$SKIP_ARG",
            $TEST_SUITE
            "--",
            "--spec-dir", $SPEC_DIR,
            "--app-list", "$APP_LIST",
            "--scheduler", "$SCHEDULER",
            "--max-storage-nodes-per-az", "$MAX_STORAGE_NODES_PER_AZ",
            "--backup-driver", "$BACKUP_DRIVER",
            "--log-level", "$LOGLEVEL",
            "--node-driver", "$NODE_DRIVER",
            "--scale-factor", "$SCALE_FACTOR",
            "--minimun-runtime-mins", "$MIN_RUN_TIME",
            "--driver-start-timeout", "$DRIVER_START_TIMEOUT",
            "--chaos-level", "$CHAOS_LEVEL",
            "--storagenode-recovery-timeout", "$STORAGENODE_RECOVERY_TIMEOUT",
            "--provisioner", "$PROVISIONER",
            "--storage-driver", "$STORAGE_DRIVER",
            "--config-map", "$CONFIGMAP",
            "--custom-config", "$CUSTOM_APP_CONFIG_PATH",
            "--storage-upgrade-endpoint-url=$UPGRADE_ENDPOINT_URL",
            "--storage-upgrade-endpoint-version=$UPGRADE_ENDPOINT_VERSION",
            "--enable-stork-upgrade=$ENABLE_STORK_UPGRADE",
            "--secret-type=$SECRET_TYPE",
            "--vault-addr=$VAULT_ADDR",
            "--vault-token=$VAULT_TOKEN",
            "--autopilot-upgrade-version=$AUTOPILOT_UPGRADE_VERSION",
            "--csi-generic-driver-config-map=$CSI_GENERIC_CONFIGMAP",
            "$APP_DESTROY_TIMEOUT_ARG",
            "$SCHEDULER_UPGRADE_HOPS_ARG"
    ]
    tty: true
    volumeMounts: [${VOLUME_MOUNTS}]
    env:
    - name: K8S_VENDOR
      value: "${K8S_VENDOR}"
    - name: TORPEDO_SSH_USER
      value: "${TORPEDO_SSH_USER}"
    - name: TORPEDO_SSH_PASSWORD
      value: "${TORPEDO_SSH_PASSWORD}"
    - name: TORPEDO_SSH_KEY
      value: "${TORPEDO_SSH_KEY}"
    - name: AZURE_TENANT_ID
      value: "${AZURE_TENANTID}"
    - name: VOLUME_PROVIDER
      value: "${VOLUME_PROVIDER}"
    - name: OBJECT_STORE_PROVIDER
      value: "${OBJECT_STORE_PROVIDER}"
    - name: AZURE_CLIENT_ID
      value: "${AZURE_CLIENTID}"
    - name: AZURE_CLIENT_SECRET
      value: "${AZURE_CLIENTSECRET}"
    - name: AZURE_ACCOUNT_NAME
      value: "${AZURE_ACCOUNT_NAME}"
    - name: AZURE_ACCOUNT_KEY
      value: "${AZURE_ACCOUNT_KEY}"
    - name: AZURE_SUBSCRIPTION_ID
      value: "${AZURE_SUBSCRIPTION_ID}"
    - name: AWS_ACCESS_KEY_ID
      value: "${AWS_ACCESS_KEY_ID}"
    - name: AWS_SECRET_ACCESS_KEY
      value: "${AWS_SECRET_ACCESS_KEY}"
    - name: KUBECONFIGS
      value: "${CLUSTER_CONFIGS}"
    - name: S3_ENDPOINT
      value: "${S3_ENDPOINT}"
    - name: S3_REGION
      value: "${S3_REGION}"
    - name: S3_DISABLE_SSL
      value: "${S3_DISABLE_SSL}"
    - name: PROVIDERS
      value: "${PROVIDERS}"
    - name: INTERNAL_DOCKER_REGISTRY
      value: "$INTERNAL_DOCKER_REGISTRY"
    - name: IMAGE_PULL_SERVER
      value: "$IMAGE_PULL_SERVER"
    - name: IMAGE_PULL_USERNAME
      value: "$IMAGE_PULL_USERNAME"
    - name: IMAGE_PULL_PASSWORD
      value: "$IMAGE_PULL_PASSWORD"
    - name: VSPHERE_USER
      value: "${VSPHERE_USER}"
    - name: VSPHERE_PWD
      value: "${VSPHERE_PWD}"
    - name: VSPHERE_HOST_IP
      value: "${VSPHERE_HOST_IP}"
  volumes: [${VOLUMES}]
  restartPolicy: Never
  serviceAccountName: torpedo-account
EOF

if [ ! -z $IMAGE_PULL_SERVER ] && [ ! -z $IMAGE_PULL_USERNAME ] && [ ! -z $IMAGE_PULL_PASSWORD ]; then
  echo "Adding Docker registry secret ..."
  auth=$(echo "$IMAGE_PULL_USERNAME:$IMAGE_PULL_PASSWORD" | base64)
  secret=$(echo "{\"auths\":{\"$IMAGE_PULL_SERVER\":{\"username\":\"$IMAGE_PULL_USERNAME\",\"password\":\"$IMAGE_PULL_PASSWORD\",\"auth\":"$auth"}}}" | base64 -w 0)
  cat >> torpedo.yaml <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: torpedo
type: docker-registry
data:
  .dockerconfigjson: $secret

EOF
  sed -i '/spec:/a\  imagePullSecrets:\n    - name: torpedo' torpedo.yaml
fi

cat torpedo.yaml

echo "Deploying torpedo pod..."
kubectl apply -f torpedo.yaml

echo "Waiting for torpedo to start running"

function describe_pod_then_exit {
  echo "Pod description:"
  kubectl describe pod torpedo
  exit 1
}

for i in $(seq 1 900) ; do
  printf .
  state=`kubectl get pod torpedo | grep -v NAME | awk '{print $3}'`
  if [ "$state" == "Error" ]; then
    echo "Error: Torpedo finished with $state state"
    describe_pod_then_exit
  elif [ "$state" == "Running" ]; then
    echo ""
    kubectl logs -f torpedo
  elif [ "$state" == "Completed" ]; then
    echo "Success: Torpedo finished with $state state"
    exit 0
  fi

  sleep 1
done

echo "Error: Failed to wait for torpedo to start running..."
describe_pod_then_exit
