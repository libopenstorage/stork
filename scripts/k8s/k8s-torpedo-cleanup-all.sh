#!/bin/bash

for ns in `kubectl get ns -l creator=torpedo | grep Active | awk '{print $1}'`; do
    echo "Cleaning up torpedo from namespace: ${ns}"
    kubectl delete namespace "${ns}"
done

for ap in `kubectl get autopilotrules -l creator=torpedo | awk 'NR > 1 {print $1}'`; do
    echo "Cleaning up autopilot rule: ${ap}"
    kubectl delete autopilotrules "${ap}"
done