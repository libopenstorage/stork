TORPEDO_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_TORPEDO_IMAGE):$(DOCKER_HUB_TAG)
.PHONY: vendor

ifndef TAGS
TAGS := daemon
endif

ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v 'github.com/portworx/torpedo/vendor' | grep -v 'github.com/portworx/torpedo/tests')
endif

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS := -gcflags "-N -l"
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

all: vet lint build fmt

deps:
	GO15VENDOREXPERIMENT=0 go get -d -v $(PKGS)

update-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -u -f $(PKGS)

test-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -t $(PKGS)

update-test-deps:
	GO15VENDOREXPERIMENT=0 go get -tags "$(TAGS)" -d -v -t -u -f $(PKGS)

fmt:
	@echo -e "Performing gofmt on following: $(PKGS)"
	@./scripts/do-gofmt.sh $(PKGS)

build:
	mkdir -p $(BIN)
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

	go get github.com/onsi/ginkgo/ginkgo
	go get github.com/onsi/gomega
	ginkgo build -r

	find . -name '*.test' | awk '{cmd="cp  "$$1"  $(BIN)"; system(cmd)}'
	chmod -R 755 bin/*

vendor-update:
	dep ensure -update

vendor:
	dep ensure

install:
	go install -tags "$(TAGS)" $(PKGS)

lint:
	go get -v golang.org/x/lint/golint
	for file in $$(find . -name '*.go' | grep -v vendor | grep -v '\.pb\.go' | grep -v '\.pb\.gw\.go'); do \
		golint $${file}; \
		if [ -n "$$(golint $${file})" ]; then \
			exit 1; \
		fi; \
	done

vet:
	go vet $(PKGS)

errcheck:
	go get -v github.com/kisielk/errcheck
	errcheck -tags "$(TAGS)" $(PKGS)

pretest: lint vet errcheck

test:
	go test -tags "$(TAGS)" $(TESTFLAGS) $(PKGS)

container:
	@echo "Building container: docker build --tag $(TORPEDO_IMG) -f Dockerfile ."
	sudo docker build --tag $(TORPEDO_IMG) -f Dockerfile .

deploy: container
	sudo docker push $(TORPEDO_IMG)

clean:
	-sudo rm -rf bin
	@echo "Deleting image "$(TORPEDO_IMG)
	-docker rmi -f $(TORPEDO_IMG)
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

