# Running the tests

The unit tests use a provided KUBECONFIG to deploy pods in the Kubernetes cluster and runs through positive and negative test cases for running async commands on pods.

```
export KUBECONFIG=<absoulute location of kubeconfig pointing to your cluster>
go test -v
```
