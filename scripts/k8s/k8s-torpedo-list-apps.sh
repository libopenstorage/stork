#!/bin/bash

for ns in `kubectl get ns -l creator=torpedo | grep Active | awk '{print $1}'`; do
    echo "Listing torpedo app components from namespace: ${ns}"

    echo "***** Pods ***** "
    kubectl get pods -n "${ns}"

    echo "***** Deployments ***** "
    kubectl get deployment -n "${ns}"

    echo "***** Statefulsets ***** "
    kubectl get statefulset -n "${ns}"

    echo "***** Services ***** "
    kubectl get service -n "${ns}"
done
