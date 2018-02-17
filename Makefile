STORK_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_IMAGE):$(DOCKER_HUB_STORK_TAG)
STORK_TEST_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_STORK_TEST_IMAGE):$(DOCKER_HUB_STORK_TEST_TAG)

ifndef TAGS
TAGS := daemon
endif

ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v 'github.com/libopenstorage/stork/vendor')
endif

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS := -gcflags "-N -l"
endif

BASE_DIR := $(shell git rev-parse --show-toplevel)

BIN :=$(BASE_DIR)/bin

.DEFAULT_GOAL=all
.PHONY: test clean

all: stork vet lint integration-test

deps:
	GO15VENDOREXPERIMENT=0 go get -d -v $(PKGS)

update-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -u -f $(PKGS)

test-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -t $(PKGS)

update-test-deps:
	GO15VENDOREXPERIMENT=0 go get -tags "$(TAGS)" -d -v -t -u -f $(PKGS)

vendor-update:
	GO15VENDOREXPERIMENT=0 GOOS=linux GOARCH=amd64 go get -tags "daemon" -d -v -t -u -f $(shell go list ./... 2>&1 | grep -v 'github.com/libopenstorage/stork/vendor')

vendor-without-update:
	go get -v github.com/kardianos/govendor
	rm -rf vendor
	govendor init
	GOOS=linux GOARCH=amd64 govendor add +external
	GOOS=linux GOARCH=amd64 govendor update +vendor
	GOOS=linux GOARCH=amd64 govendor add +external
	GOOS=linux GOARCH=amd64 govendor update +vendor

vendor: vendor-update vendor-without-update

build:
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

install:
	go install -tags "$(TAGS)" $(PKGS)

lint:
	go get -v github.com/golang/lint/golint
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
	errcheck -verbose -blank -tags "$(TAGS) unittest integrationtest" $(PKGS)

pretest: lint vet errcheck

test:
	set -e
	echo "" > coverage.txt
	for pkg in $(PKGS);	do \
		go test -tags unittest -coverprofile=profile.out -covermode=atomic $(TESTFLAGS) $${pkg}; \
		if [ -f profile.out ]; then \
			cat profile.out >> coverage.txt; \
			rm profile.out; \
		fi; \
	done

integration-test:
	@echo "Building stork integration tests"
	@cd test/integration_test && go test -tags integrationtest -v -c -o stork.test

integration-test-container:
	@echo "Building container: docker build --tag $(STORK_TEST_IMG) -f Dockerfile ."
	@cd test/integration_test && sudo docker build --tag $(STORK_TEST_IMG) -f Dockerfile .

integration-test-deploy:
	sudo docker push $(STORK_TEST_IMG)

stork:
	@echo "Building the stork binary"
	@cd cmd/stork && go build $(BUILD_OPTIONS) -o $(BIN)/stork

container: help
	@echo "Building container: docker build --tag $(STORK_IMG) -f Dockerfile ."
	sudo docker build --tag $(STORK_IMG) -f Dockerfile .

help:
	@echo "Updating help file"
	go-md2man -in help.md -out help.1

deploy:
	sudo docker push $(STORK_IMG)

clean:
	-rm -rf stork
	@echo "Deleting image "$(STORK_IMG)
	-sudo docker rmi -f $(STORK_IMG)
	go clean -i $(PKGS)`

