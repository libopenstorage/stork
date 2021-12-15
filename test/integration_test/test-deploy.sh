#!/bin/bash -x

snapshot_scale=10
migration_scale=10
backup_scale=10
failover_failback_scale=1
image_name="openstorage/stork:master"
test_image_name="openstorage/stork_test:latest"
src_config_path=""
dest_config_path=""
run_cluster_domain_test=false
environment_variables=""
storage_provisioner="portworx"
focus_tests=""
auth_secret_configmap=""
volume_driver="pxd"
short_test=false
backup_location_path="test-restore-path"
cloud_secret=""
aws_id=""
aws_key=""
generic_csi_configmap_name=""
external_test_pod=false
stork_test_version_check=false
kube_scheduler_version="v1.21.0"
for i in "$@"
do
case $i in
    --snapshot-scale-count)
        echo "Scale for snapshot test (default 10): $2"
        snapshot_scale=$2
        shift
        shift
        ;;
    --migration-scale-count)
        echo "Scale for migration scale test (default 10): $2"
        migration_scale=$2
        shift
        shift
        ;;
    --backup-scale-count)
        echo "Scale for migration scale test (default 10): $2"
        backup_scale=$2
        shift
        shift
        ;;
    --failover-failback-scale-count)
        echo "Scale for failover failback scale test (default 1): $2"
        failover_failback_scale=$2
        shift
        shift
        ;;
    --stork-image)
        echo "Stork Docker image to use for test: $2"
        image_name=$2
        shift
        shift
        ;;
    --stork-test-image)
        echo "Stork Test Docker image to use for test: $2"
        test_image_name=$2
        shift
        shift
        ;;
    --src-config-path)
        echo "Source kubeconfig path to use for test: $2"
        src_config_path=$2
        shift
        shift
        ;;
    --dest-config-path)
        echo "Destination kubeconfig path to use for test: $2"
        dest_config_path=$2
        shift
        shift
        ;;
    --cluster_domain_tests)
        echo "Flag for clusterdomain test: $2"
        run_cluster_domain_test=true
        shift
        shift
        ;;
    --storage_provisioner)
        echo "Flag for clusterdomain test: $2"
        storage_provisioner=$2
        shift
        shift
        ;;
    --env_vars)
        echo "Flag for environment variables: $2"
        environment_variables=$2
        shift
        shift
        ;;
    --focus_tests)
        echo "Flag for focus tests: $2"
        focus_tests=$2
        shift
        shift
        ;;
    --auth_secret_configmap)
        echo "K8s secret name to use for test: $2"
        auth_secret_configmap=$2
        shift
        shift
        ;;
    --volume_driver)
        echo "K8s secret name to use for test: $2"
        volume_driver=$2
        shift
        shift
        ;;
    --short_test)
        echo "Skip tests that are long/not supported: $2"
        short_test=$2
        shift
        shift
        ;;
    --backup_location_path)
        echo "Path used for backups in application backup tests: $2"
        backup_location_path=$2
        shift
        shift
        ;;
    --cloud_secret)
        echo "Secret name for cloud provider API access: $2"
        cloud_secret=$2
        shift
        shift
        ;;
    --aws_id)
        echo "AWS user id for API access: $2"
        aws_id=$2
        shift
        shift
        ;;
    --aws_key)
        echo "AWS key for API access: $2"
        aws_key=$2
        shift
        shift
        ;;
    --generic_csi_configmap)
        echo "Config map that contains csi generic driver storage class: $2"
        generic_csi_configmap_name=$2
        shift
        shift
        ;;
    --external_test_pod)
        echo "Flag for three cluster test config: $2"
        external_test_pod=true
        shift
        shift
        ;;
    --stork_test_version_check)
        echo "Flag for comparing stork version with stork test pod version: $2"
        stork_test_version_check=false
        shift
        shift
        ;;
    --kube_scheduler_version)
        echo "Kube scheduler version for stork scheduler: $2"
        kube_scheduler_version=$2
        shift
        shift
        ;;
esac
done

apk update
apk add jq
apt-get -y update 
apt-get -y install jq

sed -i 's|'openstorage/stork:.*'|'"$image_name"'|g'  /specs/stork-deployment.yaml

# Replace volume driver in stork
if [ "$volume_driver" != "pxd" ] ; then
	sed -i 's/- --driver=pxd//g' /specs/stork-deployment.yaml
fi

# For integration test mock times
kubectl delete cm stork-mock-time  -n kube-system || true
kubectl create cm stork-mock-time  -n kube-system --from-literal=time=""

echo "Creating stork deployment"
kubectl apply -f /specs/stork-deployment.yaml

# Turn on test mode
kubectl set env deploy/stork -n kube-system TEST_MODE="true"

echo "Add environment variables to the stork deployment"
if [ ! -z  $environment_variables ] ; then
        for j in $(echo $environment_variables | awk -F, '{for(i=1;i<=NF;i++){print $i}}');
          do
                kubectl set env deploy/stork -n kube-system $j;
          done
else
  echo "No environment variables passed."
fi

if [ "$volume_driver" == "azure" ] ; then
    echo "For azure backups add environment variables to stork from existing k8s secret px-azure"
    kubectl -n  kube-system set env --from=secret/$cloud_secret deploy/stork
fi

# Delete the pods to make sure we are waiting for the status from the
# new pods
kubectl delete pods -n kube-system -l name=stork

echo "Waiting for stork to be in running state"
for i in $(seq 1 100) ; do
    replicas=$(kubectl get deployment -n kube-system stork -o json | jq ".status.readyReplicas")
    if [ "$replicas" == "3" ]; then
        break
    else
        echo "Stork is not ready yet"
        sleep 10
    fi
done

if [ "$run_cluster_domain_test" == "true" ] ; then
	sed -i 's/'enable_cluster_domain'/'\""true"\"'/g' /testspecs/stork-test-pod.yaml
else 
	sed -i 's/'enable_cluster_domain'/'\"\"'/g' /testspecs/stork-test-pod.yaml
fi

if [ "$focus_tests" != "" ] ; then
     echo "Running focussed test: ${focus_tests}"
	sed -i 's|'FOCUS_TESTS'|- -test.run='"$focus_tests"'|g' /testspecs/stork-test-pod.yaml
else 
	sed -i 's|'FOCUS_TESTS'|''|g' /testspecs/stork-test-pod.yaml
fi

sed -i 's/'SHORT_FLAG'/'"$short_test"'/g' /testspecs/stork-test-pod.yaml

# Configmap with secrets indicates auth-enabled runs are required
#  * Adding the shared secret to stork, stork-test pod as env variable
#  * Pass config map to containing px secret stork-test pod so tests can pick up auth-token
if [ "$auth_secret_configmap" != "" ] ; then
    sed -i 's/'auth_secret_configmap'/'"$auth_secret_configmap"'/g' /testspecs/stork-test-pod.yaml
    sed -i 's/'px_shared_secret_key'/'"$AUTH_SHARED_KEY"'/g' /testspecs/stork-test-pod.yaml
    kubectl set env deploy/stork -n kube-system PX_SHARED_SECRET="${AUTH_SHARED_KEY}"
else
	sed -i 's/'auth_secret_configmap'/'\"\"'/g' /testspecs/stork-test-pod.yaml
fi

sed -i 's/'storage_provisioner'/'"$storage_provisioner"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/- -snapshot-scale-count=10/- -snapshot-scale-count='"$snapshot_scale"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/- -migration-scale-count=10/- -migration-scale-count='"$migration_scale"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/- -backup-scale-count=10/- -backup-scale-count='"$backup_scale"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/- -failover-failback-scale-count=1/- -failover-failback-scale-count='"$failover_failback_scale"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/'username'/'"$SSH_USERNAME"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/'password'/'"$SSH_PASSWORD"'/g' /testspecs/stork-test-pod.yaml
sed  -i 's|'openstorage/stork_test:.*'|'"$test_image_name"'|g'  /testspecs/stork-test-pod.yaml
sed -i 's/'backup_location_path'/'"$backup_location_path"'/g' /testspecs/stork-test-pod.yaml

# Add AWS creds to stork-test pod
sed -i 's/'aws_access_key_id'/'"$aws_id"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/'aws_secret_access_key'/'"$aws_key"'/g' /testspecs/stork-test-pod.yaml

if [ "$volume_driver" != "" ] ; then
	sed -i 's/- -volume-driver=pxd/- -volume-driver='"$volume_driver"'/g' /testspecs/stork-test-pod.yaml
fi

if [ "$src_config_path" != "" ]; then
    kubectl create configmap sourceconfigmap --from-file=$src_config_path -n kube-system
fi
if [ "$dest_config_path" != "" ]; then
    kubectl create configmap destinationconfigmap --from-file=$dest_config_path -n kube-system
fi

if [ "$generic_csi_configmap_name" != "" ] ; then
	sed -i 's/- -generic-csi-config=csi_config_map_name/- -generic-csi-config='"$generic_csi_configmap_name"'/g' /testspecs/stork-test-pod.yaml
fi

if [ "$external_test_pod" == "true" ] ; then
	sed -i 's/'external_test_cluster'/'\""true"\"'/g' /testspecs/stork-test-pod.yaml
else 
	sed -i 's/'external_test_cluster'/'\"\"'/g' /testspecs/stork-test-pod.yaml
fi

if [ "$stork_test_version_check" == "true" ] ; then
	sed -i 's/'stork_test_version_check'/'\""true"\"'/g' /testspecs/stork-test-pod.yaml
	sed -i 's/- -stork-version-check=false/- -stork-version-check='"$stork_test_version_check"'/g' /testspecs/stork-test-pod.yaml
else 
	sed -i 's/'stork_test_version_check'/'\"\"'/g' /testspecs/stork-test-pod.yaml
fi

kubectl delete -f /testspecs/stork-test-pod.yaml
kubectl create -f /testspecs/stork-test-pod.yaml

for i in $(seq 1 100) ; do
    test_status=$(kubectl get pod stork-test -n kube-system -o json | jq ".status.phase" -r)
    if [ "$test_status" == "Running" ] || [ "$test_status" == "Completed" ]; then
        break
    else
        echo "Test hasn't started yet, status: $test_status"
        sleep 5
    fi
done

kubectl logs stork-test  -n kube-system -f
for i in $(seq 1 100) ; do
    test_status=$(kubectl get pod stork-test -n kube-system -o json | jq ".status.phase" -r)
    if [ "$test_status" == "Running" ]; then
        echo "Test is still running, status: $test_status"
        kubectl logs stork-test  -n kube-system -f
    else
        sleep 5
        break
    fi
done

test_status=$(kubectl get pod stork-test -n kube-system -o json | jq ".status.phase" -r)
if [ "$test_status" == "Succeeded" ]; then
    echo "Tests passed"
    exit 0
elif [ "$test_status" == "Failed" ]; then
    echo "Tests failed"
    exit 1
else
    echo "Unknown test status $test_status"
    exit 1
fi
