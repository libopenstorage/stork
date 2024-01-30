#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <namespace>"
    exit 1
fi

namespace=$1

# Download Grafana dashboard configuration files
curl -O https://docs.portworx.com/samples/k8s/pxc/grafana-dashboard-config.yaml
curl -O https://docs.portworx.com/samples/k8s/pxc/grafana-datasource.yaml

# Create ConfigMaps for Grafana dashboards
kubectl -n $namespace create configmap grafana-dashboard-config --from-file=grafana-dashboard-config.yaml
kubectl -n $namespace create configmap grafana-source-config --from-file=grafana-datasource.yaml

# Download Grafana dashboard JSON files
curl "https://docs.portworx.com/samples/k8s/pxc/portworx-cluster-dashboard.json" -o portworx-cluster-dashboard.json && \
curl "https://docs.portworx.com/samples/k8s/pxc/portworx-node-dashboard.json" -o portworx-node-dashboard.json && \
curl "https://docs.portworx.com/samples/k8s/pxc/portworx-volume-dashboard.json" -o portworx-volume-dashboard.json && \
curl "https://docs.portworx.com/samples/k8s/pxc/portworx-performance-dashboard.json" -o portworx-performance-dashboard.json && \
curl "https://docs.portworx.com/samples/k8s/pxc/portworx-etcd-dashboard.json" -o portworx-etcd-dashboard.json

# Create ConfigMap for Grafana dashboards
kubectl -n $namespace create configmap grafana-dashboards \
--from-file=portworx-cluster-dashboard.json \
--from-file=portworx-performance-dashboard.json \
--from-file=portworx-node-dashboard.json \
--from-file=portworx-volume-dashboard.json \
--from-file=portworx-etcd-dashboard.json

# Download Grafana YAML file and replace namespace
curl "https://docs.portworx.com/samples/k8s/pxc/grafana.yaml" -o grafana.yaml
sed -i "s/namespace: kube-system/namespace: $namespace/" grafana.yaml

# Apply Grafana YAML
kubectl apply -f grafana.yaml

# Create node-exporter DaemonSet
cat >> node-exporter.yaml <<EOF
apiVersion: apps/v1
kind: DaemonSet
metadata:
  labels:
    app.kubernetes.io/component: exporter
    app.kubernetes.io/name: node-exporter
  name: node-exporter
  namespace: $namespace
spec:
  selector:
    matchLabels:
      app.kubernetes.io/component: exporter
      app.kubernetes.io/name: node-exporter
  template:
    metadata:
      labels:
        app.kubernetes.io/component: exporter
        app.kubernetes.io/name: node-exporter
    spec:
      containers:
      - args:
        - --path.sysfs=/host/sys
        - --path.rootfs=/host/root
        - --no-collector.wifi
        - --no-collector.hwmon
        - --collector.filesystem.ignored-mount-points=^/(dev|proc|sys|var/lib/docker/.+|var/lib/kubelet/pods/.+)($|/)
        - --collector.netclass.ignored-devices=^(veth.*)$
        name: node-exporter
        image: prom/node-exporter
        ports:
          - containerPort: 9100
            protocol: TCP
        resources:
          limits:
            cpu: 250m
            memory: 180Mi
          requests:
            cpu: 102m
            memory: 180Mi
        volumeMounts:
        - mountPath: /host/sys
          mountPropagation: HostToContainer
          name: sys
          readOnly: true
        - mountPath: /host/root
          mountPropagation: HostToContainer
          name: root
          readOnly: true
      volumes:
      - hostPath:
          path: /sys
        name: sys
      - hostPath:
          path: /
        name: root
EOF

# Apply node-exporter DaemonSet
kubectl apply -f node-exporter.yaml -n $namespace


# Create node-exporter Service
cat >> node-exportersvc.yaml <<EOF
---
kind: Service
apiVersion: v1
metadata:
  name: node-exporter
  namespace: $namespace
  labels:
    name: node-exporter
spec:
  selector:
      app.kubernetes.io/component: exporter
      app.kubernetes.io/name: node-exporter
  ports:
  - name: node-exporter
    protocol: TCP
    port: 9100
    targetPort: 9100
EOF

# Apply node-exporter service
kubectl apply -f node-exportersvc.yaml -n $namespace


# Create node-exporter service monitor
cat >> node-exporter-svcmonitor.yaml <<EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: node-exporter
  labels:
    prometheus: portworx
spec:
  selector:
    matchLabels:
      name: node-exporter
  endpoints:
  - port: node-exporter
EOF

# Apply node-exporter service monitor
kubectl apply -f node-exporter-svcmonitor.yaml -n $namespace


echo "Grafana and Node-Exporter setup completed for namespace: $namespace"
