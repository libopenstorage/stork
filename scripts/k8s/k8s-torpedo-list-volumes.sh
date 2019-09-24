#!/bin/bash 

for ns in `kubectl get ns -l creator=torpedo | grep Active | awk '{print $1}'`; do
    echo "Listing torpedo storage components from namespace: ${ns}"
    kubectl get pv -n ${ns} 
    kubectl get pvc -n ${ns} 
done

kubectl get sc 

