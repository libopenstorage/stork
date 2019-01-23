# Builing and Running Portworx Operator

## Vendoring

For vendoring, we use [dep](https://golang.github.io/dep/).
-  `dep ensure`: install the project's dependencies
-  `dep ensure -update`: update the locked versions of all dependencies
-  `dep ensure -add github.com/pkg/errors`: add a dependency to the project

## Build

#### Compile
```bash
make docker-build
```
#### Build and push docker image
```bash
export DOCKER_HUB_REPO=<docker-repo>
export DOCKER_HUB_OPERATOR_IMAGE=talisman
export DOCKER_HUB_TAG=latest

make deploy
```

## Test

#### Deploy the Portworx CRD
```bash
kubectl create -f examples/px-crd.yaml
```

#### Deploy Portworx cluster
```bash
kubectl create -f examples/px-cluster.yaml
```
