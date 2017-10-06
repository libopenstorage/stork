Torpedo is written in Golang.
Torpedo uses [Ginkgo](https://github.com/onsi/ginkgo) as the testing framework.

#### To build Torpedo
```
# git clone git@github.com:portworx/torpedo.git
# make
```

#### To deploy Torpedo image in your docker repository
```
# export DOCKER_HUB_REPO=harshpx
# export DOCKER_HUB_TORPEDO_IMAGE=torpedo
# export DOCKER_HUB_TAG=latest
# make deploy
```

Make sure you change the environment variables above to match your docker repository.

To build Torpedo using a docker build container: `make docker-build`


#### To build a specific ginkgo test

```
ginkgo build  <path_to_test_pkg>
```

For e.g:
```
ginkgo build  tests/basic
```
