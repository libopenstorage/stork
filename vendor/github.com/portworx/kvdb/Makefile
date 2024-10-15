ifndef PKGS
PKGS := $(shell go list ./... 2>&1 | grep -v test)
endif
$(info PKGS=$(PKGS))

HAS_ERRCHECK := $(shell command -v errcheck 2> /dev/null)

all: test

vendor:
	go mod download
	go mod tidy -compat=1.17
	go mod vendor

build:
	@echo ">>> go build"
	go build $(PKGS)

install:
	@echo ">>> go install"
	go install $(PKGS)

vet:
	@echo ">>> go vet"
	go vet $(PKGS)

errcheck:
ifndef HAS_ERRCHECK
	go install github.com/kisielk/errcheck@latest
endif
	@echo ">>> errcheck"
	errcheck \
		github.com/portworx/kvdb \
		github.com/portworx/kvdb/common \
		github.com/portworx/kvdb/consul \
		github.com/portworx/kvdb/mem \
		github.com/portworx/kvdb/wrappers \
		github.com/portworx/kvdb/zookeeper

pretest: errcheck vet

gotest:
	@echo ">>> gotest"
ifeq ($(PKGS),"")
	$(error Error: No packages found to test)
endif
	for pkg in $(PKGS); \
		do \
			echo ">>> Testing $${pkg}"; \
			go test --timeout 1h -v -tags unittest -coverprofile=profile.out -covermode=atomic $(BUILD_OPTIONS) $${pkg} || exit 1; \
			if [ -f profile.out ]; then \
				cat profile.out >> coverage.txt; \
				rm profile.out; \
			fi; \
		done

test: pretest gotest

docker-build-kvdb-dev:
	docker build -t portworx/kvdb:test_container -f $(GOPATH)/src/github.com/portworx/kvdb/Dockerfile.kvdb .

docker-test:
	docker run --rm \
		-v $(GOPATH)/src/github.com/portworx/kvdb:/go/src/github.com/portworx/kvdb \
		portworx/kvdb:test_container \
		make gotest

clean:
	go clean -i ./...

.PHONY: \
	all \
	vendor \
	build \
	install \
	vet \
	errcheck \
	pretest \
	gotest \
	test \
	clean
