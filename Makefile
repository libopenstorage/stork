DOCKER_HUB_REPO ?= openstorage

DOCKER_HUB_STORK_IMAGE ?= stork
DOCKER_HUB_STORK_TAG ?= dev

DOCKER_HUB_CMD_EXECUTOR_IMAGE ?= cmdexecutor
DOCKER_HUB_CMD_EXECUTOR_TAG ?= dev

DOCKER_HUB_STORK_TEST_IMAGE ?= stork_test
DOCKER_HUB_STORK_TEST_TAG ?= dev

STORK_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_STORK_TAG)
CMD_EXECUTOR_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_CMD_EXECUTOR_IMAGE):$(DOCKER_HUB_CMD_EXECUTOR_TAG)
STORK_TEST_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_TEST_IMAGE):$(DOCKER_HUB_STORK_TEST_TAG)

DOCK_BUILD_CNT  := golang:1.19.10

ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v 'github.com/libopenstorage/stork/vendor' | grep -v 'pkg/client/informers/externalversions' | grep -v versioned | grep -v 'pkg/apis/stork' | grep -v 'hack')
endif

GO_FILES := $(shell find . -name '*.go' | grep -v vendor | \
                                   grep -v '\.pb\.go' | \
                                   grep -v '\.pb\.gw\.go' | \
                                   grep -v 'externalversions' | \
                                   grep -v 'versioned' | \
                                   grep -v 'generated' | \
								   grep -v 'hack')

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS += -gcflags "-N -l"
endif

RELEASE_VER := 2.7.0
BASE_DIR    := $(shell git rev-parse --show-toplevel)
GIT_SHA     := $(shell git rev-parse --short HEAD)
BIN         :=$(BASE_DIR)/bin

VERSION = $(RELEASE_VER)-$(GIT_SHA)

LDFLAGS += "-s -w -X github.com/libopenstorage/stork/pkg/version.Version=$(VERSION)"
BUILD_OPTIONS := -ldflags=$(LDFLAGS) -buildvcs=false

SECCOMP_OPTIONS := --security-opt seccomp=unconfined

.DEFAULT_GOAL=all
.PHONY: test clean vendor vendor-update px-statfs

all: stork storkctl cmdexecutor pretest px-statfs

vendor-tidy:
	go mod tidy

vendor-update:
	go mod download

vendor:
	go mod vendor

lint:
	GO111MODULE=off go get -u golang.org/x/lint/golint
	for file in $(GO_FILES); do \
		echo "running lint on ${file}" \
		golint $${file}; \
		if [ -n "$$(golint $${file})" ]; then \
			exit 1; \
		fi; \
	done

vet:
	  docker run --rm $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		      /bin/bash -c "cd /go/src/github.com/libopenstorage/stork; \
	          go vet $(PKGS); \
	          go vet -tags unittest $(PKGS); \
	          go vet -tags integrationtest github.com/libopenstorage/stork/test/integration_test"

staticcheck:
	  docker run --rm  $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork $(DOCK_BUILD_CNT) \
		      /bin/bash -c "cd /go/src/github.com/libopenstorage/stork; \
			  go install honnef.co/go/tools/cmd/staticcheck@v0.3.3; \
			  staticcheck $(PKGS); \
			  staticcheck -tags integrationtest test/integration_test/*.go;staticcheck -tags unittest $(PKGS)"

errcheck:
	  docker run --rm  $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		      /bin/bash -c "cd /go/src/github.com/libopenstorage/stork; \
	          GO111MODULE=off go get -u github.com/kisielk/errcheck; \
	          errcheck -verbose -blank $(PKGS); \
	          errcheck -verbose -blank -tags unittest $(PKGS); \
	          errcheck -verbose -blank -tags integrationtest /go/src/github.com/libopenstorage/stork/test/integration_test"

check-fmt:
	  docker run --rm  $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork $(DOCK_BUILD_CNT) \
		      /bin/bash -c "cd /go/src/github.com/libopenstorage/stork; \
			  diff -u <(echo -n) <(gofmt -l -d -s -e $(GO_FILES));"

do-fmt:
	 gofmt -s -w $(GO_FILES)

gocyclo:
	GO111MODULE=off go get -u github.com/fzipp/gocyclo
	gocyclo -over 15 $(GO_FILES)

pretest: check-fmt vet errcheck staticcheck

test:
	echo "" > coverage.txt
	for pkg in $(PKGS);	do \
		go test -v -tags unittest -coverprofile=profile.out -covermode=atomic $(BUILD_OPTIONS) $${pkg} || exit 1; \
		if [ -f profile.out ]; then \
			cat profile.out >> coverage.txt; \
			rm profile.out; \
		fi; \
	done

integration-test:
	@echo "Building stork integration tests"
	docker run --rm $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		   /bin/bash -c 'cd /go/src/github.com/libopenstorage/stork/test/integration_test && \
		   GOOS=linux go test -tags integrationtest $(BUILD_OPTIONS) -v -c -o stork.test;'

integration-test-container:
	@echo "Building container: docker build --tag $(STORK_TEST_IMG) -f Dockerfile ."
	@rm -rf test/integration_test/stork-specs && mkdir -p test/integration_test/stork-specs
	@cp -r specs/* test/integration_test/stork-specs
	@cd test/integration_test && sudo docker build --tag $(STORK_TEST_IMG) -f Dockerfile .

integration-test-deploy:
	sudo docker push $(STORK_TEST_IMG)

codegen:
	@echo "Generating CRD"
	(GOFLAGS="" hack/update-codegen.sh)

stork:
	@echo "Building the stork binary"
	docker run --rm $(SECCOMP_OPTIONS)  -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
           /bin/bash -c 'cd /go/src/github.com/libopenstorage/stork/cmd/stork && \
		   CGO_ENABLED=0 GOOS=linux go build $(BUILD_OPTIONS) -o /go/src/github.com/libopenstorage/stork/bin/stork;'

cmdexecutor:
	@echo "Building command executor binary"
	docker run --rm $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		/bin/bash -c 'cd /go/src/github.com/libopenstorage/stork/cmd/cmdexecutor && \
		GOOS=linux go build $(BUILD_OPTIONS) -o /go/src/github.com/libopenstorage/stork/bin/cmdexecutor;'

storkctl:
	@echo "Building storkctl"
	docker run --rm $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		/bin/bash -c 'cd /go/src/github.com/libopenstorage/stork/cmd/storkctl; \
		CGO_ENABLED=0 GOOS=linux go build $(BUILD_OPTIONS) -o /go/src/github.com/libopenstorage/stork/bin/linux/storkctl; \
		CGO_ENABLED=0 GOOS=darwin go build $(BUILD_OPTIONS) -o /go/src/github.com/libopenstorage/stork/bin/darwin/storkctl; \
		CGO_ENABLED=0 GOOS=windows go build $(BUILD_OPTIONS) -o /go/src/github.com/libopenstorage/stork/bin/windows/storkctl.exe;'

px-statfs:
	@echo "Building px_statfs.so"
	docker run --rm  $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork  $(DOCK_BUILD_CNT) \
		   /bin/bash -c 'cd /go/src/github.com/libopenstorage/stork/drivers/volume/portworx/px-statfs &&  \
           gcc -g -shared -fPIC -o /go/src/github.com/libopenstorage/stork/bin/px_statfs.so px_statfs.c -ldl -D__USE_LARGEFILE64;'


container: help
	@echo "Building container: docker build --build-arg VERSION=$(DOCKER_HUB_STORK_TAG) --build-arg RELEASE=$(DOCKER_HUB_STORK_TAG) --tag $(STORK_IMG) -f Dockerfile . "
	sudo docker build --build-arg VERSION=$(DOCKER_HUB_STORK_TAG) --build-arg RELEASE=$(DOCKER_HUB_STORK_TAG) --tag $(STORK_IMG) -f Dockerfile .

	@echo "Building container: docker build --tag $(CMD_EXECUTOR_IMG) -f Dockerfile.cmdexecutor ."
	sudo docker build --tag $(CMD_EXECUTOR_IMG) -f Dockerfile.cmdexecutor .

help:
	@echo "Updating help file"
	docker run --rm $(SECCOMP_OPTIONS) -v $(shell pwd):/go/src/github.com/libopenstorage/stork $(DOCK_BUILD_CNT) \
           /bin/bash -c "cd /go/src/github.com/libopenstorage/stork; \
                apt-get update -y && apt-get install -y go-md2man; \
                go-md2man -in help.md -out help.1; \
                go-md2man -in help-cmdexecutor.md -out help-cmdexecutor.1;"

deploy:
	sudo docker push $(STORK_IMG)
	sudo docker push $(CMD_EXECUTOR_IMG)

clean:
	-rm -rf $(BIN)
	@echo "Deleting image "$(STORK_IMG)
	-sudo docker rmi -f $(STORK_IMG)
	@echo "Deleting image "$(CMD_EXECUTOR_IMG)
	-sudo docker rmi -f $(CMD_EXECUTOR_IMG)
	go clean -i $(PKGS)
