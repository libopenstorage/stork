#
# Based on http://chrismckenzie.io/post/deploying-with-golang/
#          https://raw.githubusercontent.com/lpabon/quartermaster/dev/Makefile
#

.PHONY: version all run clean container deploy talisman

DOCKER_HUB_TAG ?= latest
PX_NODE_WIPER_TAG ?= latest
DOCKER_PULLER_IMG=$(DOCKER_HUB_REPO)/docker-puller:$(DOCKER_HUB_TAG)
PX_NODE_WIPER_IMG=$(DOCKER_HUB_REPO)/px-node-wiper:$(PX_NODE_WIPER_TAG)
TALISMAN_IMG=$(DOCKER_HUB_REPO)/talisman:$(DOCKER_HUB_TAG)

SHA    := $(shell git rev-parse --short HEAD)
BRANCH := $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))
VER    := $(shell git rev-parse --short HEAD)
GO     := go
GOENV  := GOOS=linux GOARCH=amd64
DIR=.

ifndef TAGS
TAGS := daemon
endif

ifdef APP_SUFFIX
  VERSION = $(VER)-$(subst /,-,$(APP_SUFFIX))
else
ifeq (master,$(BRANCH))
  VERSION = $(VER)
else
  VERSION = $(VER)-$(BRANCH)
endif
endif

LDFLAGS += -X github.com/portworx/talisman/pkg/version.Version=$(VERSION)

BUILD_TYPE=static
ifeq ($(BUILD_TYPE),static)
    LDFLAGS += -extldflags -static
    BUILD_OPTIONS += -v -a -ldflags "$(LDFLAGS)"
    GOENV += CGO_ENABLED=0
else ifeq ($(BUILD_TYPE),debug)
    BUILD_OPTIONS += -i -v -gcflags "-N -l" -ldflags "$(LDFLAGS)"
else
    BUILD_OPTIONS += -i -v -ldflags "$(LDFLAGS)"
endif


PKGS=$(shell go list ./... | grep -v vendor)
GOVET_PKGS=$(shell  go list ./... | grep -v vendor | grep -v pkg/client/informers/externalversions | grep -v versioned)

BASE_DIR := $(shell git rev-parse --show-toplevel)

BIN :=$(BASE_DIR)/bin
GOFMT := gofmt

.DEFAULT: all

all: clean pretest talisman

# print the version
version:
	@echo $(VERSION)

talisman:
	mkdir -p $(BIN)
	env $(GOENV) go build $(BUILD_OPTIONS) -o $(BIN)/talisman cmd/talisman/talisman.go

test:
	go test -tags "$(TAGS)" $(TESTFLAGS) $(PKGS)

fmt:
	@echo "Performing gofmt on following: $(PKGS)"
	@./hack/do-gofmt.sh $(PKGS)

checkfmt:
	@echo "Checking gofmt on following: $(PKGS)"
	@./hack/check-gofmt.sh $(PKGS)

lint:
	@echo "golint"
	go get -u golang.org/x/lint/golint
	for file in $$(find . -name '*.go' | grep -v vendor | \
																			grep -v '\.pb\.go' | \
																			grep -v '\.pb\.gw\.go' | \
																			grep -v 'externalversions' | \
																			grep -v 'versioned' | \
																			grep -v 'generated'); do \
		golint $${file}; \
		if [ -n "$$(golint $${file})" ]; then \
			exit 1; \
		fi; \
	done

vet:
	@echo "govet"
	go vet $(GOVET_PKGS)

errcheck:
	@echo "errcheck"
	go get -v github.com/kisielk/errcheck
	errcheck -tags "$(TAGS)" $(GOVET_PKGS)

codegen:
	@echo "Generating files"
	@./hack/update-codegen.sh

verifycodegen:
	@echo "Verifying generated files"
	@./hack/verify-codegen.sh

pretest: checkfmt vet lint errcheck

container:
	sudo docker build --tag $(TALISMAN_IMG) -f Dockerfile.talisman .
	sudo docker build -t $(PX_NODE_WIPER_IMG) cmd/px-node-wiper/

deploy: container
	sudo docker push $(TALISMAN_IMG)
	sudo docker push $(PX_NODE_WIPER_IMG)

docker-build:
	docker build -t px/docker-build -f Dockerfile.build .
	@echo "Building using docker"
	docker run \
		--privileged \
		-v $(shell pwd):/go/src/github.com/portworx/talisman \
		px/docker-build make all

.PHONY: test clean name run version

clean:
	@echo Cleaning Workspace...
	-sudo rm -rf $(BIN)
	-docker rmi -f $(OPERATOR_IMG)
	-docker rmi -f $(PX_NODE_WIPER_IMG)
	-docker rmi -f $(TALISMAN_IMG)
	go clean -i $(PKGS)
