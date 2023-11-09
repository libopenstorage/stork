# Rule of thumb (to prevent huge containers):
#	A single torpedo container should contain only one test binary.
#
# make {build|container}:
#	create basic.test binary/container (<repo>/torpedo:<tag>)
#
# make {build-pds|container-pds}:
#	 create pds.test binary/container (<repo>/torpedo-pds:<tag>)
#
# make {build-backup|container-backup}:
#	 create backup.test binary/container (<repo>/torpedo-backup:<tag>)
#
# make all:
#	 verify that all test binaries build successfully
#
# Note that DOCKER_HUB_TORPEDO_IMAGE environment variable is not used since
# it is set automatically depending on which binary is being built.
#

.PHONY: vendor

export GO111MODULE=on
export GOFLAGS = -mod=vendor
HAS_GOMODULES := $(shell go help mod why 2> /dev/null)

ifndef HAS_GOMODULES
$(error torpedo can only be built with go 1.11+ which supports go modules)
endif

ifndef TAGS
TAGS := daemon
endif

ifndef PKGS
# shell does not honor export command above, so we need to explicitly pass GOFLAGS here
PKGS := $(shell GOFLAGS=-mod=vendor go list ./... 2>&1 | grep -v 'github.com/portworx/torpedo/tests')
PKGIN := $(shell GOFLAGS=-mod=vendor go list ./... 2>&1 | grep -v 'github.com/portworx/torpedo/apiServer/taas')
endif

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS := -gcflags "-N -l"
endif

ifndef DOCKER_HUB_REPO
    DOCKER_HUB_REPO := $(shell id -un)px
    $(warning DOCKER_HUB_REPO not defined, using '$(DOCKER_HUB_REPO)' instead)
endif
ifndef DOCKER_HUB_TAG
    DOCKER_HUB_TAG := latest
    $(warning DOCKER_HUB_TAG not defined, using '$(DOCKER_HUB_TAG)' instead)
endif

BASE_DIR := $(shell git rev-parse --show-toplevel)

BIN :=$(BASE_DIR)/bin
GOFMT := gofmt
.DEFAULT_GOAL=all

SIDECAR_WP_CLI_DOCKER_TAG=1.2.28
SIDECAR_WP_CLI_IMG=$(DOCKER_HUB_REPO)/wp-cli:$(SIDECAR_WP_CLI_DOCKER_TAG)
SIDECAR_DIR=drivers/scheduler/sidecars
SYSBENCH_IMG=$(DOCKER_HUB_REPO)/torpedo-sysbench:latest
PGBENCH_IMG=$(DOCKER_HUB_REPO)/torpedo-pgbench:latest
ESLOAD_IMG=$(DOCKER_HUB_REPO)/torpedo-esload:latest


all: vet build build-pds build-backup build-taas fmt

deps:
	go get -d -v $(PKGS)

update-deps:
	go get -d -v -u -f $(PKGS)

test-deps:
	go get -d -v -t $(PKGS)

update-test-deps:
	go get -tags "$(TAGS)" -d -v -t -u -f $(PKGS)

fmt:
	@echo -e "Performing gofmt on following: $(PKGS)"
	@./scripts/do-gofmt.sh $(PKGS)

# Tools
$(GOPATH)/bin/ginkgo:
	GOFLAGS= GO111MODULE=on go install github.com/onsi/ginkgo/ginkgo@v1.16.5

# this target builds the basic.test binary only. Use build-pds for pds.test binary.
build: GINKGO_BUILD_DIR=./tests/basic
build: $(GOPATH)/bin/ginkgo
	mkdir -p $(BIN)
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

	ginkgo build -r $(GINKGO_BUILD_DIR)
	find $(GINKGO_BUILD_DIR) -name '*.test' | awk '{cmd="cp  "$$1"  $(BIN)"; system(cmd)}'
	chmod -R 755 bin/*

# this target builds the pds.test binary only.
build-pds: GINKGO_BUILD_DIR=./tests/pds
build-pds: $(GOPATH)/bin/ginkgo
	mkdir -p $(BIN)
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

	ginkgo build -r $(GINKGO_BUILD_DIR)
	find $(GINKGO_BUILD_DIR) -name '*.test' | awk '{cmd="cp  "$$1"  $(BIN)"; system(cmd)}'
	chmod -R 755 bin/*

# this target builds the backup.test binary only.
build-backup: GINKGO_BUILD_DIR=./tests/backup
build-backup: $(GOPATH)/bin/ginkgo
	mkdir -p $(BIN)
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

	ginkgo build -r $(GINKGO_BUILD_DIR)
	find $(GINKGO_BUILD_DIR) -name '*.test' | awk '{cmd="cp  "$$1"  $(BIN)"; system(cmd)}'
	chmod -R 755 bin/*

# this target builds the taas binary only.
build-taas: TAAS_BUILD_DIR=./apiServer/taas
build-taas:
	mkdir -p $(BIN)
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGIN)
	go build $(TAAS_BUILD_DIR)

	find . -name 'taas' | awk '{cmd="cp  "$$1"  $(BIN)"; system(cmd)}'
	chmod -R 755 bin/*

vendor-update:
	go mod download

vendor-tidy:
	go mod tidy

vendor:
	go mod vendor
	sed -i '1 i\// +build skipcompile\n' vendor/kubevirt.io/client-go/kubecli/kubevirt_test_utils.go

install:
	go install -tags "$(TAGS)" $(PKGS)

lint:
	(mkdir -p tools && GO111MODULE=off go get -v golang.org/x/lint/golint)
	for file in $$(find . -name '*.go' | grep -v vendor | grep -v '\.pb\.go' | grep -v '\.pb\.gw\.go'); do \
		golint $${file}; \
		if [ -n "$$(golint $${file})" ]; then \
			exit 1; \
		fi; \
	done

vet:
	go vet $(PKGS)

errcheck:
	(mkdir -p tools && GO111MODULE=off && go get -v github.com/kisielk/errcheck)
	errcheck -tags "$(TAGS)" $(PKGS)

pretest: vet errcheck

test:
	go test -tags "$(TAGS)" $(TESTFLAGS) $(PKGS)

# this target builds a container with test.basic binary only. Repo is hardcoded to ".../torpedo".
container: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo:$(DOCKER_HUB_TAG)
container:
	@echo "Building basic.test container "$(TORPEDO_IMG)
	sudo DOCKER_BUILDKIT=1 docker build --tag $(TORPEDO_IMG) --build-arg MAKE_TARGET=build -f Dockerfile .

# this target builds a container with pds.test binary only. Repo is hardcoded to ".../torpedo-pds".
container-pds: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo-pds:$(DOCKER_HUB_TAG)
container-pds:
	@echo "Building pds.test container "$(TORPEDO_IMG)
	sudo DOCKER_BUILDKIT=1 docker build --tag $(TORPEDO_IMG) --build-arg MAKE_TARGET=build-pds -f Dockerfile .

# this target builds a container with backup.test binary only. Repo is hardcoded to ".../torpedo-backup".
container-backup: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo-backup:$(DOCKER_HUB_TAG)
container-backup:
	@echo "Building backup.test container "$(TORPEDO_IMG)
	sudo DOCKER_BUILDKIT=1 docker build --tag $(TORPEDO_IMG) --build-arg MAKE_TARGET=build-backup -f Dockerfile .

# this target builds a container with torpedo apiserver binary only. Repo is hardcoded to ".../taas".
container-taas: TORPEDO_IMG=$(DOCKER_HUB_REPO)/taas:$(DOCKER_HUB_TAG)
container-taas:
	@echo "Building taas container "$(TORPEDO_IMG)
	sudo DOCKER_BUILDKIT=1 docker build --tag $(TORPEDO_IMG) --build-arg MAKE_TARGET=build-taas -f Dockerfile-taas .

deploy: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo:$(DOCKER_HUB_TAG)
deploy: container
	sudo docker push $(TORPEDO_IMG)

deploy-pds: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo-pds:$(DOCKER_HUB_TAG)
deploy-pds: container-pds
	sudo docker push $(TORPEDO_IMG)

deploy-backup: TORPEDO_IMG=$(DOCKER_HUB_REPO)/torpedo-backup:$(DOCKER_HUB_TAG)
deploy-backup: container-backup
	sudo docker push $(TORPEDO_IMG)

deploy-taas: TORPEDO_IMG=$(DOCKER_HUB_REPO)/taas:$(DOCKER_HUB_TAG)
deploy-taas: container-taas
	sudo docker push $(TORPEDO_IMG)

clean:
	-sudo rm -rf bin
	-sudo find . -type f -name "*.test" -delete
	@echo "Deleting basic image"
	-docker rmi -f $(DOCKER_HUB_REPO)/torpedo:$(DOCKER_HUB_TAG)
	@echo "Deleting pds image"
	-docker rmi -f $(DOCKER_HUB_REPO)/torpedo-pds:$(DOCKER_HUB_TAG)
	@echo "Deleting backup image"
	-docker rmi -f $(DOCKER_HUB_REPO)/torpedo-backup:$(DOCKER_HUB_TAG)
	@echo "Deleting taas image"
	-docker rmi -f $(DOCKER_HUB_REPO)/torpedo-taas:$(DOCKER_HUB_TAG)
	go clean -i $(PKGS)

sidecar: sidecar-wp-cli

sidecar-wp-cli:
	docker build -t $(SIDECAR_WP_CLI_IMG) -f $(SIDECAR_DIR)/wp-cli.dockerfile $(SIDECAR_DIR)
	docker push $(SIDECAR_WP_CLI_IMG)

sysbench:
	docker build -t $(SYSBENCH_IMG) -f $(SIDECAR_DIR)/sysbench.dockerfile $(SIDECAR_DIR)
	docker push $(SYSBENCH_IMG)

pgbench:
	docker build -t $(PGBENCH_IMG) -f $(SIDECAR_DIR)/pgbench.dockerfile $(SIDECAR_DIR)
	docker push $(PGBENCH_IMG)

esload:
	docker build -t $(ESLOAD_IMG) $(SIDECAR_DIR)/es-stress-test
	docker push $(ESLOAD_IMG)

