FROM golang:1.13.7-alpine AS build
LABEL maintainer="harsh@portworx.com"

WORKDIR /go/src/github.com/portworx/torpedo

# Install setup dependencies
RUN apk update && \
    apk add git gcc  musl-dev && \
    apk add make && \
    apk add openssh-client && \
    go get github.com/onsi/ginkgo/ginkgo && \
    go get github.com/onsi/gomega && \
    go get github.com/sirupsen/logrus

# No need to copy *everything*. This keeps the cache useful
COPY deployments deployments
COPY drivers drivers
COPY pkg pkg
COPY scripts scripts
COPY tests tests
COPY vendor vendor
COPY Makefile Makefile
COPY go.mod go.mod
COPY go.sum go.sum

# Why? Errors if this is removed
COPY .git .git

# Compile
RUN mkdir bin && \
    make build

# Build a fresh container with just the binaries
FROM alpine

RUN apk add ca-certificates 

# Install kubectl from Docker Hub.
COPY --from=lachlanevenson/k8s-kubectl:latest /usr/local/bin/kubectl /usr/local/bin/kubectl

# Copy scripts into container
WORKDIR /torpedo
COPY deployments deployments
COPY scripts scripts

WORKDIR /go/src/github.com/portworx/torpedo

# Copy ginkgo & binaries over from previous container
COPY --from=build /go/bin/ginkgo /bin/ginkgo
COPY --from=build /go/src/github.com/portworx/torpedo/bin bin
COPY drivers drivers

ENTRYPOINT ["ginkgo", "--failFast", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
