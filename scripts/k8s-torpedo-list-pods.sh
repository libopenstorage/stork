#!/bin/bash

for ns in `kubectl get ns -l creater=torpedo | grep Active | awk '{print $1}'`; do
    echo "Listing torpedo pods components from namespace: ${ns}"
    kubectl get pods -n "${ns}"
done
