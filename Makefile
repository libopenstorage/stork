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
GLIDEPATH := $(shell command -v glide 2> /dev/null)

.DEFAULT_GOAL=all

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

vendor:
ifndef GLIDEPATH
	echo "Installing Glide"
	curl https://glide.sh/get | sh
endif

	glide update -v
	# Workaround for https://github.com/sirupsen/logrus/issues/451, https://github.com/sirupsen/logrus/issues/543
	find . -name '*.go' | grep -e vendor | xargs sed -i 's/Sirupsen/sirupsen/g'

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

DOCKER_HUB_SIDECAR_TAG=1.0
SIDECAR_WORDPRESS_IMG=$(DOCKER_HUB_REPO)/wordpress-extract:$(DOCKER_HUB_SIDECAR_TAG)
SIDECAR_MYSQL_IMG=$(DOCKER_HUB_REPO)/mysql-dump:$(DOCKER_HUB_SIDECAR_TAG)
SIDECAR_PING_IMG=$(DOCKER_HUB_REPO)/ping-test:$(DOCKER_HUB_SIDECAR_TAG)
SIDECAR_DIR=drivers/scheduler/sidecars

sidecar: sidecar-wordpress-extract sidecar-wordpress sidecar-mysql sidecar-ping

sidecar-wordpress-extract:
	wget http://objects.liquidweb.services/triv/mwch-base.tar.gz
	tar -xvzf mwch-base.tar.gz -C $(SIDECAR_DIR)

sidecar-wordpress:
	docker build -t $(SIDECAR_WORDPRESS_IMG) -f $(SIDECAR_DIR)/mysql-dump.dockerfile $(SIDECAR_DIR)
	docker push $(SIDECAR_WORDPRESS_IMG)

sidecar-mysql:
	docker build -t $(SIDECAR_MYSQL_IMG) -f $(SIDECAR_DIR)/wordpress-extract.dockerfile $(SIDECAR_DIR)
	docker push $(SIDECAR_MYSQL_IMG)

sidecar-ping:
	docker build -t $(SIDECAR_PING_IMG) -f $(SIDECAR_DIR)/ping-test.dockerfile $(SIDECAR_DIR)
	docker push $(SIDECAR_PING_IMG)

