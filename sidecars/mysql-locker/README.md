Mysql locker is a binary that flushes the database and acquires a read lock on the given mysql hosts.

# How to run

## To run as binary
```
bin/mysql-locker -h
```

## To run as a docker container
```
docker run -e MYSQL_PASSWORD=<mysql-password> openstore/mysql-locker:latest
```

## To run as a Kubernetes pod

Update the mysql password, host and port in the `mysql-locker-pod.yaml` spec and apply the spec.
```
kubectl apply -f mysql-locker-pod.yaml
kubectl logs -f mysql-locker
```
