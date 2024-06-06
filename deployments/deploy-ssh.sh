#!/bin/bash -x

if [ -z "${ENABLE_DASH}" ]; then
    ENABLE_DASH=true
fi

if [ -z "${ENABLE_GRAFANA}" ]; then
    ENABLE_GRAFANA=false
fi

if [ -z "${DATA_INTEGRITY_VALIDATION_TESTS}" ]; then
    DATA_INTEGRITY_VALIDATION_TESTS=""
fi

if [ -z "${DASH_UID}" ]; then
    if [ -e /build.properties ]; then
      DASH_UID=`cat /build.properties | grep -i "DASH_UID=" | grep -Eo '[0-9]+'`
    else
      DASH_UID="0"
    fi
fi

SECURITY_CONTEXT=false

# System checks https://github.com/portworx/torpedo/blob/86232cb195400d05a9f83d57856f8f29bdc9789d/tests/common.go#L2173
# should be skipped from AfterSuite() if this flag is set to true. This is to avoid distracting test failures due to
# unstable testing environments.
TORPEDO_SKIP_SYSTEM_CHECKS=false
if [[ ! -z "${TORPEDO_SKIP_SYSTEM_CHECKS}" ]]; then
    TORPEDO_SKIP_SYSTEM_CHECKS=true
fi

if [ "${IS_OCP}" == true ]; then
    SECURITY_CONTEXT=true
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
    FAIL_FAST="--fail-fast"
else
    FAIL_FAST="-keep-going"
fi

SKIP_ARG=""
if [ -n "$SKIP_TESTS" ]; then
    skipRegex=$(echo $SKIP_TESTS | sed -e 's/,/}|{/g')
    SKIP_ARG="--skip={$skipRegex}"
fi

FOCUS_ARG=""
if [ -n "$FOCUS_TESTS" ]; then
    focusRegex="$(echo $FOCUS_TESTS | sed -e 's/,/}|{/g')"
    FOCUS_ARG="--focus={$focusRegex}"
fi

if [ -n "$LABEL_FILTER" ]; then
    FOCUS_ARG="--label-filter=$LABEL_FILTER"
fi

if [ -z "${UPGRADE_ENDPOINT_URL}" ]; then
    UPGRADE_ENDPOINT_URL=""
fi

if [ -z "${UPGRADE_ENDPOINT_VERSION}" ]; then
    UPGRADE_ENDPOINT_VERSION=""
fi

if [ -z "${UPGRADE_STORAGE_DRIVER_ENDPOINT_LIST}" ]; then
    UPGRADE_STORAGE_DRIVER_ENDPOINT_LIST=""
fi

if [ -z "${SKIP_PX_OPERATOR_UPGRADE}" ]; then
    SKIP_PX_OPERATOR_UPGRADE=false
fi

if [ -z "${ENABLE_STORK_UPGRADE}" ]; then
    ENABLE_STORK_UPGRADE=false
fi

if [ -z "${IS_PURE_VOLUMES}" ]; then
    IS_PURE_VOLUMES=false
fi

if [ -z "${DEPLOY_PDS_APPS}" ]; then
    DEPLOY_PDS_APPS=false
fi

if [ -z "${PURE_FA_CLONE_MANY_TEST}" ]; then
    PURE_FA_CLONE_MANY_TEST=false
fi

if [ -z "${PURE_SAN_TYPE}" ]; then
    PURE_SAN_TYPE=ISCSI
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

if [ -z "${IS_HYPER_CONVERGED}" ]; then
    IS_HYPER_CONVERGED=true
fi

if [ -z "${PX_POD_RESTART_CHECK}" ]; then
    PX_POD_RESTART_CHECK=false
fi

CONFIGMAP=""
if [ -n "${CONFIG_MAP}" ]; then
    CONFIGMAP="${CONFIG_MAP}"
fi

if [ -z "${TORPEDO_IMG}" ]; then
    TORPEDO_IMG="portworx/torpedo:master"
    echo "Using default torpedo image: ${TORPEDO_IMG}"
fi

if [ -z "${TIMEOUT}" ]; then
    TIMEOUT="720h0m0s"
    echo "Using default timeout of ${TIMEOUT}"
fi

if [ -z "$DRIVER_START_TIMEOUT" ]; then
    DRIVER_START_TIMEOUT="10m0s"
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

SCALE_APP_TIMEOUT_ARG=""
if [ -n "${SCALE_APP_TIMEOUT}" ]; then
    SCALE_APP_TIMEOUT_ARG="--scale-app-timeout=$SCALE_APP_TIMEOUT"
fi

if [ -z "$LICENSE_EXPIRY_TIMEOUT_HOURS" ]; then
    LICENSE_EXPIRY_TIMEOUT_HOURS="1h0m0s"
    echo "Using default license expiry timeout of ${LICENSE_EXPIRY_TIMEOUT_HOURS}"
fi

if [ -z "$METERING_INTERVAL_MINS" ]; then
    METERING_INTERVAL_MINS="10m0s"
    echo "Using default metering of ${METERING_INTERVAL_MINS}"
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

CSI_GENERIC_CONFIGMAP=""
if [ -n "${CSI_GENERIC_DRIVER_CONFIGMAP}" ]; then
    CSI_GENERIC_CONFIGMAP="${CSI_GENERIC_DRIVER_CONFIGMAP}"
fi

if [ -z "$AWS_REGION" ]; then
    AWS_REGION="us-east-1"
    echo "Using default AWS_REGION of ${AWS_REGION}"
fi

if [ -z "$TORPEDO_JOB_TYPE" ]; then
    TORPEDO_JOB_TYPE="functional"
fi

if [ -z "$TORPEDO_JOB_NAME" ]; then
    TORPEDO_JOB_NAME="torpedo-daily-job"
fi

if [ -n "$ANTHOS_ADMIN_WS_NODE" ]; then
    ANTHOS_ADMIN_WS_NODE="${ANTHOS_ADMIN_WS_NODE}"
fi
if [ -n "$ANTHOS_INST_PATH" ]; then
    ANTHOS_INST_PATH="${ANTHOS_INST_PATH}"
fi

if [ -n "$ANTHOS_HOST_PATH" ]; then
    ANTHOS_HOST_PATH="${ANTHOS_HOST_PATH}"
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

echo "Checking if we need to override test suite: ${TEST_SUITE}"

# TODO: Remove this after all longevity jobs switch to 'bin/longevity.test' for TEST_SUITE.
case $FOCUS_TESTS in
  Longevity|UpgradeLongevity|BackupLongevity)
    TEST_SUITE="bin/longevity.test"
    echo "Warning: Based on the FOCUS_TESTS ('$FOCUS_TESTS'), the TEST_SUITE ('$TEST_SUITE') is set to 'bin/longevity.test'"
    ;;
  *)
    ;;
esac

if [[ "$TEST_SUITE" != *"pds.test"* ]] && [[ "$TEST_SUITE" != *"backup.test"* ]] && [[ "$TEST_SUITE" != *"longevity.test"* ]]; then
    TEST_SUITE='"bin/basic.test"'
fi

echo "Using test suite: ${TEST_SUITE}"

if [ -z "${AUTOPILOT_UPGRADE_VERSION}" ]; then
    AUTOPILOT_UPGRADE_VERSION=""
fi

kubectl -n default delete secret torpedo
kubectl -n default delete pod torpedo
state=`kubectl -n default get pod torpedo | grep -v NAME | awk '{print $3}'`
timeout=0
while [ "$state" == "Terminating" -a $timeout -le 600 ]; do
  echo "Terminating torpedo..."
  sleep 1
  state=`kubectl -n default get pod torpedo | grep -v NAME | awk '{print $3}'`
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
    kubectl -n default create configmap custom-app-config --from-file=custom_app_config.yml=${CUSTOM_APP_CONFIG}
    CUSTOM_APP_CONFIG_PATH="/mnt/torpedo/custom_app_config.yml"
    TORPEDO_CUSTOM_PARAM_VOLUME="{ \"name\": \"custom-app-config-volume\", \"configMap\": { \"name\": \"custom-app-config\", \"items\": [{\"key\": \"custom_app_config.yml\", \"path\": \"custom_app_config.yml\"}] } }"
    TORPEDO_CUSTOM_PARAM_MOUNT="{ \"name\": \"custom-app-config-volume\", \"mountPath\": \"${CUSTOM_APP_CONFIG_PATH}\", \"subPath\": \"custom_app_config.yml\" }"
fi

TORPEDO_SSH_KEY_VOLUME=""
TORPEDO_SSH_KEY_MOUNT=""
if [ -n "${TORPEDO_SSH_KEY}" ]; then
    kubectl -n default create secret generic key4torpedo --from-file=${TORPEDO_SSH_KEY}
    TORPEDO_SSH_KEY_VOLUME="{ \"name\": \"ssh-key-volume\", \"secret\": { \"secretName\": \"key4torpedo\", \"defaultMode\": 256 }}"
    TORPEDO_SSH_KEY_MOUNT="{ \"name\": \"ssh-key-volume\", \"mountPath\": \"/home/torpedo/\" }"
fi

ORACLE_API_KEY_VOLUME=""
if [ -n "${ORACLE_API_KEY}" ]; then
    ORACLE_API_KEY_VOLUME="{ \"name\": \"oracle-api-key-volume\", \"secret\": { \"secretName\": \"key4oracle\", \"defaultMode\": 256 }}"
    ORACLE_API_KEY_MOUNT="{ \"name\": \"oracle-api-key-volume\", \"mountPath\": \"/home/oci/\" }"
fi

TESTRESULTS_VOLUME="{ \"name\": \"testresults\", \"hostPath\": { \"path\": \"/mnt/testresults/\", \"type\": \"DirectoryOrCreate\" } }"
TESTRESULTS_MOUNT="{ \"name\": \"testresults\", \"mountPath\": \"/testresults/\" }"

AWS_VOLUME="{ \"name\": \"aws-volume\", \"configMap\": { \"name\": \"aws-cm\", \"items\": [{\"key\": \"credentials\", \"path\": \"credentials\"}, {\"key\": \"config\", \"path\": \"config\"}]} }"
AWS_VOLUME_MOUNT="{ \"name\": \"aws-volume\", \"mountPath\": \"/root/.aws/\" }"

ANTHOS_VOLUME="{ \"name\": \"anthosdir\", \"hostPath\": { \"path\": \"${ANTHOS_HOST_PATH}\", \"type\": \"Directory\" } }"
ANTHOS_VOLUME_MOUNT="{ \"name\": \"anthosdir\", \"mountPath\": \"/anthosdir\" }"

VOLUMES="${TESTRESULTS_VOLUME}"

if [ "${STORAGE_DRIVER}" == "aws" ]; then
  VOLUMES="${VOLUMES},${AWS_VOLUME}"
  VOLUME_MOUNTS="${VOLUME_MOUNTS},${AWS_VOLUME_MOUNT}"
fi

JUNIT_REPORT_PATH="/testresults/junit_basic.xml"
if [ "${SCHEDULER}" == "openshift" ]; then
    SECURITY_CONTEXT=true
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

if [ -n "${ORACLE_API_KEY_MOUNT}" ]; then
    VOLUME_MOUNTS="${VOLUME_MOUNTS},${ORACLE_API_KEY_MOUNT}"
fi

if [ -n "${ORACLE_API_KEY_VOLUME}" ]; then
    VOLUMES="${VOLUMES},${ORACLE_API_KEY_VOLUME}"
fi

if [ -n "${TORPEDO_CUSTOM_PARAM_VOLUME}" ]; then
    VOLUMES="${VOLUMES},${TORPEDO_CUSTOM_PARAM_VOLUME}"
fi

if [ -n "${TORPEDO_CUSTOM_PARAM_MOUNT}" ]; then
    VOLUME_MOUNTS="${VOLUME_MOUNTS},${TORPEDO_CUSTOM_PARAM_MOUNT}"
fi

if [ -n "${ANTHOS_HOST_PATH}" ]; then
  VOLUMES="${VOLUMES},${ANTHOS_VOLUME}"
  VOLUME_MOUNTS="${VOLUME_MOUNTS},${ANTHOS_VOLUME_MOUNT}"
fi

BUSYBOX_IMG="busybox"
if [ -n "${INTERNAL_DOCKER_REGISTRY}" ]; then
    BUSYBOX_IMG="${INTERNAL_DOCKER_REGISTRY}/busybox"
    TORPEDO_IMG="${INTERNAL_DOCKER_REGISTRY}/${TORPEDO_IMG}"
fi

kubectl -n default create configmap cloud-config --from-file=/config/cloud-json

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
  kubectl -n default create configmap kubeconfigs ${FROM_FILE}
fi

K8S_VENDOR_KEY=""
if [ -z "${NODE_DRIVER}" ]; then
    NODE_DRIVER="ssh"
fi

if [ -n "${K8S_VENDOR}" ]; then
    case "$K8S_VENDOR" in
        oracle)
            NODE_DRIVER="oracle"
            ;;
    esac
fi

echo '' > torpedo.yaml


cat >> torpedo.yaml <<EOF
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
  - key: node-role.kubernetes.io/control-plane
    operator: Exists
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
          - key: node-role.kubernetes.io/master
            operator: "Exists"
        - matchExpressions:
          - key: node-role.kubernetes.io/control-plane
            operator: "Exists"
        - matchExpressions:
          - key: node-role.kubernetes.io/controlplane
            operator: "In"
            values: ["true"]
        - matchExpressions:
          - key: px/enabled
            operator: "In"
            values: ["false"]
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
    securityContext:
      privileged: ${SECURITY_CONTEXT}
    command: [ "ginkgo" ]
    args: [ "--trace",
            "--timeout", "${TIMEOUT}",
            "$FAIL_FAST",
            "--poll-progress-after", "20m",
            --junit-report=$JUNIT_REPORT_PATH,
            "$FOCUS_ARG",
            "$SKIP_ARG",
            $TEST_SUITE,
            "--",
            "--spec-dir", $SPEC_DIR,
            "--app-list", "$APP_LIST",
            "--deploy-pds-apps=$DEPLOY_PDS_APPS",
            "--pds-driver", "$PDS_DRIVER",
            "--secure-apps", "$SECURE_APP_LIST",
            "--repl1-apps", "$REPL1_APP_LIST",
            "--csi-app-list", "$CSI_APP_LIST",
            "--scheduler", "$SCHEDULER",
            "--max-storage-nodes-per-az", "$MAX_STORAGE_NODES_PER_AZ",
            "--backup-driver", "$BACKUP_DRIVER",
            "--log-level", "$LOGLEVEL",
            "--node-driver", "$NODE_DRIVER",
            "--scale-factor", "$SCALE_FACTOR",
            "--hyper-converged=$IS_HYPER_CONVERGED",
            "--fail-on-px-pod-restartcount=$PX_POD_RESTART_CHECK",
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
            "--upgrade-storage-driver-endpoint-list=$UPGRADE_STORAGE_DRIVER_ENDPOINT_LIST",
            "--enable-stork-upgrade=$ENABLE_STORK_UPGRADE",
            "--secret-type=$SECRET_TYPE",
            "--pure-volumes=$IS_PURE_VOLUMES",
            "--pure-fa-snapshot-restore-to-many-test=$PURE_FA_CLONE_MANY_TEST",
            "--pure-san-type=$PURE_SAN_TYPE",
            "--vault-addr=$VAULT_ADDR",
            "--vault-token=$VAULT_TOKEN",
            "--px-runtime-opts=$PX_RUNTIME_OPTS",
            "--px-cluster-opts=$PX_CLUSTER_OPTS",
            "--anthos-ws-node-ip=$ANTHOS_ADMIN_WS_NODE",
            "--anthos-inst-path=$ANTHOS_INST_PATH",
            "--autopilot-upgrade-version=$AUTOPILOT_UPGRADE_VERSION",
            "--csi-generic-driver-config-map=$CSI_GENERIC_CONFIGMAP",
            "--sched-upgrade-hops=$SCHEDULER_UPGRADE_HOPS",
            "--migration-hops=$MIGRATION_HOPS",
            "--license_expiry_timeout_hours=$LICENSE_EXPIRY_TIMEOUT_HOURS",
            "--metering_interval_mins=$METERING_INTERVAL_MINS",
            "--testrail-milestone=$TESTRAIL_MILESTONE",
            "--testrail-run-name=$TESTRAIL_RUN_NAME",
            "--testrail-run-id=$TESTRAIL_RUN_ID",
            "--testrail-jeknins-build-url=$TESTRAIL_JENKINS_BUILD_URL",
            "--testrail-host=$TESTRAIL_HOST",
            "--testrail-username=$TESTRAIL_USERNAME",
            "--testrail-password=$TESTRAIL_PASSWORD",
            "--jira-username=$JIRA_USERNAME",
            "--jira-token=$JIRA_TOKEN",
            "--jira-account-id=$JIRA_ACCOUNT_ID",
            "--user=$USER",
            "--enable-dash=$ENABLE_DASH",
            "--data-integrity-validation-tests=$DATA_INTEGRITY_VALIDATION_TESTS",
            "--test-desc=$TEST_DESCRIPTION",
            "--test-type=$TEST_TYPE",
            "--test-tags=$TEST_TAGS",
            "--testset-id=$DASH_UID",
            "--branch=$BRANCH",
            "--product=$PRODUCT",
            "--torpedo-job-name=$TORPEDO_JOB_NAME",
            "--torpedo-job-type=$TORPEDO_JOB_TYPE",
            "--torpedo-skip-system-checks=$TORPEDO_SKIP_SYSTEM_CHECKS",
            "--fa-secret=${FA_SECRET}",
            "$APP_DESTROY_TIMEOUT_ARG",
            "$SCALE_APP_TIMEOUT_ARG",
    ]
    tty: true
    volumeMounts: [${VOLUME_MOUNTS}]
    env:
    - name: NODE_NAME
      valueFrom:
        fieldRef:
          fieldPath: spec.nodeName
    - name: K8S_VENDOR
      value: "${K8S_VENDOR}"
    - name: TORPEDO_SSH_USER
      value: "${TORPEDO_SSH_USER}"
    - name: TORPEDO_SSH_PASSWORD
      value: "${TORPEDO_SSH_PASSWORD}"
    - name: TORPEDO_SSH_KEY
      value: "${TORPEDO_SSH_KEY}"
    - name: AZURE_ENDPOINT
      value: "${AZURE_ENDPOINT}"
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
    - name: SOURCE_RKE_TOKEN
      value: "${SOURCE_RKE_TOKEN}"
    - name: DESTINATION_RKE_TOKEN
      value: "${DESTINATION_RKE_TOKEN}"
    - name: AZURE_ACCOUNT_KEY
      value: "${AZURE_ACCOUNT_KEY}"
    - name: AZURE_SUBSCRIPTION_ID
      value: "${AZURE_SUBSCRIPTION_ID}"
    - name: AZURE_CLUSTER_NAME
      value: "${AZURE_CLUSTER_NAME}"
    - name: AWS_ACCESS_KEY_ID
      value: "${AWS_ACCESS_KEY_ID}"
    - name: AWS_SECRET_ACCESS_KEY
      value: "${AWS_SECRET_ACCESS_KEY}"
    - name: AWS_REGION
      value: "${AWS_REGION}"
    - name: AWS_MINIO_ACCESS_KEY_ID
      value: "${AWS_MINIO_ACCESS_KEY_ID}"
    - name: AWS_MINIO_SECRET_ACCESS_KEY
      value: "${AWS_MINIO_SECRET_ACCESS_KEY}"
    - name: AWS_MINIO_REGION
      value: "${AWS_MINIO_REGION}"
    - name: AWS_MINIO_ENDPOINT
      value: "${AWS_MINIO_ENDPOINT}"
    - name: KUBECONFIGS
      value: "${CLUSTER_CONFIGS}"
    - name: S3_ENDPOINT
      value: "${S3_ENDPOINT}"
    - name: S3_AWS_ACCESS_KEY_ID
      value: "${S3_AWS_ACCESS_KEY_ID}"
    - name: S3_AWS_SECRET_ACCESS_KEY
      value: "${S3_AWS_SECRET_ACCESS_KEY}"
    - name: S3_REGION
      value: "${S3_REGION}"
    - name: BUCKET_NAME
      value: "${BUCKET_NAME}"
    - name: LOCKED_BUCKET_NAME
      value: "${LOCKED_BUCKET_NAME}"
    - name: S3_DISABLE_SSL
      value: "${S3_DISABLE_SSL}"
    - name: DIAGS_BUCKET
      value: "${DIAGS_BUCKET}"
    - name: PROVIDERS
      value: "${PROVIDERS}"
    - name: CLUSTER_PROVIDER
      value: "${CLUSTER_PROVIDER}"
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
    - name: VSPHERE_DATACENTER
      value: "${VSPHERE_DATACENTER}"
    - name: IBMCLOUD_API_KEY
      value: "${IBMCLOUD_API_KEY}"
    - name: CONTROL_PLANE_URL
      value: "${CONTROL_PLANE_URL}"
    - name: DS_VERSION
      value: "${DS_VERSION}"
    - name: DS_BUILD
      value: "${DS_BUILD}"
    - name: NAMESPACE
      value: "${NAMESPACE}"
    - name: NO_OF_NODES
      value: "${NO_OF_NODES}"
    - name: DATA_SERVICE
      value: "${DATA_SERVICE}"
    - name: DEPLOY_ALL_VERSIONS
      value: "${DEPLOY_ALL_VERSIONS}"
    - name: DEPLOY_ALL_IMAGES
      value: "${DEPLOY_ALL_IMAGES}"
    - name: DEPLOY_ALL_DATASERVICE
      value: "${DEPLOY_ALL_DATASERVICE}"
    - name: GCP_PROJECT_ID
      value: "${GCP_PROJECT_ID}"
    - name: PDS_USERNAME
      value: "${PDS_USERNAME}"
    - name: PDS_PASSWORD
      value: "${PDS_PASSWORD}"
    - name: PDS_CLIENT_SECRET
      value: "${PDS_CLIENT_SECRET}"
    - name: PDS_CLIENT_ID
      value: "${PDS_CLIENT_ID}"
    - name: PDS_PARAM_CM
      value: "${PDS_PARAM_CM}"
    - name: PDS_ISSUER_URL
      value: "${PDS_ISSUER_URL}"
    - name: CLUSTER_TYPE
      value: "${CLUSTER_TYPE}"
    - name: TARGET_KUBECONFIG
      value: "${TARGET_KUBECONFIG}"
    - name: TARGET_CLUSTER_NAME
      value: "${TARGET_CLUSTER_NAME}"
    - name: PX_ORACLE_user_ocid
      value: "${PX_ORACLE_user_ocid}"
    - name: PX_ORACLE_compartment_id
      value: "${PX_ORACLE_compartment_id}"
    - name: PX_ORACLE_cluster_name
      value: "${PX_ORACLE_cluster_name}"
    - name: PX_ORACLE_fingerprint
      value: "${PX_ORACLE_fingerprint}"
    - name: PX_ORACLE_cluster_region
      value: "${PX_ORACLE_cluster_region}"
    - name: PX_ORACLE_tenancy
      value: "${PX_ORACLE_tenancy}"
    - name: PX_ORACLE_private_key_path
      value: "${ORACLE_API_KEY}"
    - name: INSTANCE_GROUP
      value: "${INSTANCE_GROUP}"
    - name: LOGGLY_API_TOKEN
      value: "${LOGGLY_API_TOKEN}"
    - name: PODMETRIC_METERING_INTERVAL_MINUTES
      value: "${PODMETRIC_METERING_INTERVAL_MINUTES}"
    - name: TARGET_PXBACKUP_VERSION
      value: "${TARGET_PXBACKUP_VERSION}"
    - name: TARGET_STORK_VERSION
      value: "${TARGET_STORK_VERSION}"
    - name: PX_BACKUP_HELM_REPO_BRANCH
      value: "${PX_BACKUP_HELM_REPO_BRANCH}"
    - name: BACKUP_TYPE
      value: "${BACKUP_TYPE}"
    - name: NFS_SERVER_ADDR
      value: "${NFS_SERVER_ADDR}"
    - name: NFS_SUB_PATH
      value: "${NFS_SUB_PATH}"
    - name: NFS_MOUNT_OPTION
      value: "${NFS_MOUNT_OPTION}"
    - name: NFS_PATH
      value: "${NFS_PATH}"
    - name: SKIP_PX_OPERATOR_UPGRADE
      value: "${SKIP_PX_OPERATOR_UPGRADE}"
    - name: VOLUME_SNAPSHOT_CLASS
      value: "${VOLUME_SNAPSHOT_CLASS}"
    - name: S3_SSE_TYPE
      value: "${S3_SSE_TYPE}"
    - name: S3_POLICY_SID
      value: "${S3_POLICY_SID}"
    - name: S3_ENCRYPTION_POLICY
      value: "${S3_ENCRYPTION_POLICY}"
    - name: NUM_VCLUSTERS
      value: "${NUM_VCLUSTERS}"
    - name: VCLUSTER_PARALLEL_APPS
      value: "${VCLUSTER_PARALLEL_APPS}"
    - name: VCLUSTER_TOTAL_ITERATIONS
      value: "${VCLUSTER_TOTAL_ITERATIONS}"
    - name: NUM_ML_WORKLOADS
      value: "${NUM_ML_WORKLOADS}"
    - name: ML_WORKLOAD_RUNTIME
      value: "${ML_WORKLOAD_RUNTIME}"
    - name: KUBEVIRT_UPGRADE_VERSION
      value: "${KUBEVIRT_UPGRADE_VERSION}"
    - name: PX_BACKUP_MONGODB_USERNAME
      value: "${PX_BACKUP_MONGODB_USERNAME}"
    - name: PX_BACKUP_MONGODB_PASSWORD
      value: "${PX_BACKUP_MONGODB_PASSWORD}"
    - name: ENABLE_GRAFANA
      value: "${ENABLE_GRAFANA}"
    - name: USE_GLOBAL_RULES
      value: "${USE_GLOBAL_RULES}"
    - name: EKS_CLUSTER_NAME
      value: "${EKS_CLUSTER_NAME}"
    - name: EKS_CLUSTER_REGION
      value: "${EKS_CLUSTER_REGION}"
    - name: EKS_PX_NODEGROUP_NAME
      value: "${EKS_PX_NODEGROUP_NAME}"
    - name: IKS_CLUSTER_NAME
      value: "${IKS_CLUSTER_NAME}"
    - name: IKS_PX_WORKERPOOL_NAME
      value: "${IKS_PX_WORKERPOOL_NAME}"
    - name: IKS_CLUSTER_REGION
      value: "${IKS_CLUSTER_REGION}"
    - name: LONGEVITY_UPGRADE_EXECUTION_THRESHOLD
      value: "${LONGEVITY_UPGRADE_EXECUTION_THRESHOLD}"
    - name: GOOGLE_APPLICATION_CREDENTIALS
      value: "${GOOGLE_APPLICATION_CREDENTIALS}"
  volumes: [${VOLUMES}]
  restartPolicy: Never
  serviceAccountName: torpedo-account


EOF

if [ "${RUN_GINKGO_COMMAND}" = "true" ]; then
    torpedo_pod_command="ginkgo"
    torpedo_pod_args=$(yq e '.spec.containers[] | select(.name == "torpedo") | .args[]' torpedo.yaml | sed 's/,$//')

	# This code removes the comma if the line ends with it.
	# If the line is an empty string, it is quoted.
	# Otherwise, the line is printed normally.
    formatted_torpedo_pod_args=$(echo "$torpedo_pod_args" | awk '{
        if ($0 ~ /,$/) {
            gsub(/,$/, "", $0);
            printf "%s ", $0;
        } else {
            if ($0 == "") {
                printf "\"\" ";
            } else {
                printf "%s ", $0;
            }
        }
    }')

    torpedo_pod_ginkgo_command="$torpedo_pod_command $formatted_torpedo_pod_args"
    echo "Formatted Ginkgo Command: $torpedo_pod_ginkgo_command"

    # This code skips a flag followed by an empty string ("") if the next token is another flag or if it is the end of the command.
    # This is necessary because Torpedo does not handle empty strings as expected.
    # Example: In the input ginkgo --trace --timeout "" --fail-fast ... --fa-secret ""
    # --timeout "" is skipped because it is immediately followed by another flag --fail-fast
    # --fa-secret "" is also skipped because it is the last token and followed by no other arguments.
    cleaned_torpedo_pod_ginkgo_command=$(echo "$torpedo_pod_ginkgo_command" | awk '
	{
	   output = "";
	   i = 1;
	   while (i <= NF) {
		   if ($(i) ~ /^--/ && $(i+1) == "\"\"") {
			   if (i+2 <= NF && $(i+2) ~ /^--/) {
				   i += 2;
				   continue;
			   } else if (i+2 > NF) {
				   i += 2;
				   continue;
			   }
		   }
		   output = output (output ? " " : "") $(i);
		   i++;
	   }
	   print output;
	}')
    echo "Cleaned Ginkgo Command: $cleaned_torpedo_pod_ginkgo_command"

    $cleaned_torpedo_pod_ginkgo_command
    exit $?
fi

# If these are passed, we will create a docker config secret to use to pull images
if [ ! -z $IMAGE_PULL_SERVER ] && [ ! -z $IMAGE_PULL_USERNAME ] && [ ! -z $IMAGE_PULL_PASSWORD ]; then
  echo "Adding Docker registry secret ..."
  auth=$(echo -n "$IMAGE_PULL_USERNAME:$IMAGE_PULL_PASSWORD" | base64)
  secret=$(echo -n "{\"auths\":{\"$IMAGE_PULL_SERVER\":{\"auth\":\"$auth\"}}}" | base64 -w 0)
  cat >> torpedo.yaml <<EOF
---
apiVersion: v1
kind: Secret
metadata:
  name: torpedo
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: $secret

EOF
  sed -i '/spec:/a\  imagePullSecrets:\n    - name: torpedo' torpedo.yaml
fi

cat torpedo.yaml

echo "Deploying torpedo pod..."
kubectl -n default apply -f torpedo.yaml

echo "Waiting for torpedo to start running"

function describe_pod_then_exit {
  echo "Pod description:"
  kubectl -n default describe pod torpedo
  exit 1
}

function terminate_pod_then_exit {
    echo "Terminating Ginkgo test in Torpedo pod..."
    # Fetch the PID of the Ginkgo test process
    local test_pid
    test_pid=$(kubectl -n default exec torpedo -- pgrep -f 'torpedo/bin')
    if [ "$test_pid" ]; then
        # Using SIGKILL instead of SIGTERM to immediately stop the process.
        # SIGTERM would allow Ginkgo to run AfterSuite and generate reports,
        # but the intention here is to stop the process immediately.
        echo "Sending SIGKILL to terminate Ginkgo test process with PID: $test_pid"
        kubectl -n default exec torpedo -- kill -SIGKILL "$test_pid"
    fi
    exit 1
}

trap terminate_pod_then_exit SIGTERM

# The for loop is run in a background process (subshell) to allow the main script
# to remain responsive to signals (SIGTERM, SIGINT) while the loop is executing.
(
    first_iteration=true
    for i in $(seq 1 900); do
        echo "Iteration: $i"
        state=$(kubectl -n default get pod torpedo | grep -v NAME | awk '{print $3}')

        if [ "$state" == "Error" ]; then
            echo "Error: Torpedo finished with $state state"
            describe_pod_then_exit
        elif [ "$state" == "Running" ]; then
            # For the first iteration, display all logs. Later, only from 1 minute ago
            if [ "$first_iteration" = true ]; then
                echo "Logs from first iteration"
                kubectl -n default logs -f torpedo
                first_iteration=false
            else
                echo "Logs from iteration: $i"
                kubectl -n default logs -f --since=1m torpedo
            fi
        elif [ "$state" == "Completed" ]; then
            echo "Success: Torpedo finished with $state state"
            exit 0
        fi

        sleep 1
    done

    echo "Error: Failed to wait for torpedo to start running..."
    describe_pod_then_exit
) &

wait $!
