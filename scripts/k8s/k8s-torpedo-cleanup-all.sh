#!/bin/bash

for ns in `kubectl get ns -l creater=torpedo | grep Active | awk '{print $1}'`; do
    echo "Cleaning up torpedo from namespace: ${ns}"
    kubectl delete statefulset -n "${ns}" --all
    kubectl delete deployment -n "${ns}" --all
    kubectl delete svc -n "${ns}" --all
    kubectl delete pvc -n "${ns}" --all
    kubectl delete namespace $ns
done

kubectl delete sc --all

