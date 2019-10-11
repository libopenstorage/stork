DOCKER_HUB_CLOUDOPS_TAG ?= latest
CLOUDOPS_IMG=$(DOCKER_HUB_REPO)/cloudops:$(DOCKER_HUB_CLOUDOPS_TAG)

ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v 'github.com/libopenstorage/cloudops/vendor' | grep -v versioned | grep -v 'pkg/apis/cloudops')
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

.DEFAULT_GOAL=all
.PHONY: clean vendor vendor-update container deploy

all: build vet lint

vendor-update:
	dep ensure -update

vendor:
	dep ensure

lint:
	go get -v golang.org/x/lint/golint
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
	go get -u honnef.co/go/tools/cmd/staticcheck

staticcheck: $(GOPATH)/bin/staticcheck
	$(GOPATH)/bin/staticcheck $(PKGS)

errcheck:
	go get -v github.com/kisielk/errcheck
	errcheck -verbose -blank $(PKGS)

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

clean:
	go clean -i $(PKGS)
