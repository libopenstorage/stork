DOCKER_HUB_CLOUDOPS_TAG ?= latest
CLOUDOPS_IMG=$(DOCKER_HUB_REPO)/cloudops:$(DOCKER_HUB_CLOUDOPS_TAG)

ifndef PKGS
PKGS := ./...
endif

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS += -gcflags "-N -l"
endif

RELEASE_VER := 0.0.1
BASE_DIR    := $(shell git rev-parse --show-toplevel)
GIT_SHA     := $(shell git rev-parse --short HEAD)
BIN         :=$(BASE_DIR)/bin

LDFLAGS += "-s -w -X github.com/libopenstorage/cloudops/pkg/version.Version=$(VERSION)"
BUILD_OPTIONS := -ldflags=$(LDFLAGS)

HAS_GOMODULES := $(shell go help mod why 2> /dev/null)

ifdef HAS_GOMODULES
export GO111MODULE=on
export GOFLAGS = -mod=vendor
else
$(error cloudops can only be built with go 1.11+ which supports go modules)
endif

.DEFAULT_GOAL=all
.PHONY: test clean vendor vendor-update container deploy

all: build vet lint

vendor-update:
	go mod download

vendor-tidy:
	go mod tidy

vendor:
	go mod vendor

lint:
	(mkdir -p tools && cd tools && GO111MODULE=off && go get -v golang.org/x/lint/golint)
	for file in $$(find . -name '*.go' | grep -v vendor | \
                                       grep -v '\.pb\.go' | \
                                       grep -v '\.pb\.gw\.go' | \
                                       grep -v 'vsphere' | \
                                       grep -v 'externalversions' | \
                                       grep -v 'versioned' | \
                                       grep -v 'generated'); do \
		golint $${file}; \
		if [ -n "$$(golint $${file})" ]; then \
			exit 1; \
		fi; \
	done

vet:
	go vet $(PKGS)

$(GOPATH)/bin/staticcheck:
	(mkdir -p tools && cd tools && GO111MODULE=off && go get -u honnef.co/go/tools/cmd/staticcheck)

staticcheck: $(GOPATH)/bin/staticcheck
	$(GOPATH)/bin/staticcheck $(PKGS)

errcheck:
	(mkdir -p tools && cd tools && GO111MODULE=off && go get -v github.com/kisielk/errcheck)
	errcheck -verbose -blank $(PKGS)
	errcheck -verbose -blank -tags unittest $(PKGS)

pretest: lint vet errcheck staticcheck

build:
	go build -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

install:
	go install -tags "$(TAGS)" $(BUILDFLAGS) $(PKGS)

cloudops:
	@echo "Building the cloudops binary"
	@cd cmd/cloudops && CGO_ENABLED=0 go build $(BUILD_OPTIONS) -o $(BIN)/cloudops

generate-mockfiles:
	go generate $(PKGS)

container:
	@echo "Building container: docker build --tag $(CLOUDOPS_IMG) -f Dockerfile ."
	sudo docker build --tag $(CLOUDOPS_IMG) -f Dockerfile .

deploy: container
	sudo docker push $(CLOUDOPS_IMG)

docker-build-osd-dev:
	# This image is local only and will not be pushed
	docker build -t openstorage/osd-dev -f Dockerfile.osd-dev .

docker-build: docker-build-osd-dev
	docker run \
		--privileged \
		-e AWS_ACCESS_KEY_ID \
		-e AWS_SECRET_ACCESS_KEY \
		-e "TAGS=$(TAGS)" \
		-e "PKGS=$(PKGS)" \
		-e "BUILDFLAGS=$(BUILDFLAGS)" \
		openstorage/osd-dev \
			make

test:
	echo "" > coverage.txt
	for pkg in $(PKGS);	do \
		go test -v -tags unittest -coverprofile=profile.out -covermode=atomic $(BUILD_OPTIONS) $${pkg} || exit 1; \
		if [ -f profile.out ]; then \
			cat profile.out >> coverage.txt; \
			rm profile.out; \
		fi; \
	done

clean:
	go clean -i $(PKGS)
