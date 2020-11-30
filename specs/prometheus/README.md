# Setting up Prometheus metrics for Stork
All of prometheus install specs are under specs/prometheus directory
* If prometheus operator is not installed already install via applying spec
```
kubectl apply -f specs/prometheus/00-promethues-oper.yaml
```
* Apply clusterrole, serviceAccount settings to ensure prometheus pod has access to metric api
```
kubectl apply -f specs/role.yaml
kubectl apply -f specs/service.yaml
kubectl apply -f specs/sa.yaml
```
* Apply Service Monitor specs
```
kubectl apply -f specs/service_monitor.yaml
```
* Apply prometheus specs for stork metrics
```
kubectl apply -f specs/prometheus.yaml
```

* Apply promethues rules to get pod hyper/non-hyper converged alerts
```
kubectl apply -f specs/rule.yaml
```