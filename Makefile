TORPEDO_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_TORPEDO_IMAGE):$(DOCKER_HUB_TAG)

ifndef TAGS
TAGS := daemon
endif

ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v 'github.com/portworx/torpedo/vendor')
endif

ifeq ($(BUILD_TYPE),debug)
BUILDFLAGS := -gcflags "-N -l"
endif

BASE_DIR := $(shell git rev-parse --show-toplevel)

BIN :=$(BASE_DIR)/bin

.DEFAULT_GOAL=all

all: vet lint build torpedo

deps:
	GO15VENDOREXPERIMENT=0 go get -d -v $(PKGS)

update-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -u -f $(PKGS)

test-deps:
	GO15VENDOREXPERIMENT=0 go get -d -v -t $(PKGS)

update-test-deps:
	GO15VENDOREXPERIMENT=0 go get -tags "$(TAGS)" -d -v -t -u -f $(PKGS)

vendor-update:
	GO15VENDOREXPERIMENT=0 GOOS=linux GOARCH=amd64 go get -tags "daemon" -d -v -t -u -f $(shell go list ./... 2>&1 | grep -v 'github.com/portworx/torpedo/vendor')

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
	errcheck -tags "$(TAGS)" $(PKGS)

pretest: lint vet errcheck

test:
	go test -tags "$(TAGS)" $(TESTFLAGS) $(PKGS)

docker-build-osd-dev:
	docker build -t openstorage/osd-dev -f Dockerfile.osd-dev .

torpedo:
	@echo "Building the torpedo binary"
	@cd cmd/torpedo && go build $(BUILD_OPTIONS) -o $(BIN)/torpedo

container:
	@echo "Building container: docker build --tag $(TORPEDO_IMG) -f Dockerfile ."
	sudo docker build --tag $(TORPEDO_IMG) -f Dockerfile .

deploy: all container
	docker push $(TORPEDO_IMG)

clean:
	-rm -rf bin
	@echo "Deleting image "$(TORPEDO_IMG)
	-docker rmi -f $(TORPEDO_IMG)
	go clean -i $(PKGS)
