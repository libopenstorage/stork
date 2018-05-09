#!/bin/bash -x

initializer="false"
for i in "$@"
do
case $i in
    --with-initializer)
        echo "Starting test with initializer"
        initializer="true"
        shift
        ;;
esac
done

apk update
apk add jq

if [ "$initializer" == "true" ] ; then
    # Remove schedule name from all specs
    find /testspecs/specs -name '*.yaml' | xargs sed -i '/schedulerName: stork/d'
    # Create the initializer
    kubectl delete -f /specs/stork-initializer.yaml
    kubectl create -f /specs/stork-initializer.yaml
    # Enable it in the stork spec
    sed -i s/'#- --app-initializer=true'/'- --app-initializer=true'/g /specs/stork-deployment.yaml
fi

KUBEVERSION=$(kubectl version -o json | jq ".serverVersion.gitVersion" -r)
sed -i 's/<kube_version>/'"$KUBEVERSION"'/g' /specs/stork-scheduler.yaml
sed -i 's/'stork:.*'/'stork:master'/g' /specs/stork-deployment.yaml

echo "Creating stork deployment"
kubectl delete -f /specs/stork-deployment.yaml
kubectl create -f /specs/stork-deployment.yaml

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

echo "Creating stork scheduler"
kubectl delete -f /specs/stork-scheduler.yaml
kubectl create -f /specs/stork-scheduler.yaml

echo "Waiting for stork-scheduler to be in running state"
for i in $(seq 1 100) ; do
    replicas=$(kubectl get deployment -n kube-system stork-scheduler -o json | jq ".status.readyReplicas")
    if [ "$replicas" == "3" ]; then
        break
    else
        echo "Stork scheduler is not ready yet"
        sleep 10
    fi
done

sed -i 's/'username'/'"$SSH_USERNAME"'/g' /testspecs/stork-test-pod.yaml
sed -i 's/'password'/'"$SSH_PASSWORD"'/g' /testspecs/stork-test-pod.yaml

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
        sleep 5
    else
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
