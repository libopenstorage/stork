FROM golang:1.8.3-alpine AS build
LABEL maintainer="harsh@portworx.com"

WORKDIR /go/src/github.com/portworx/torpedo

# Install setup dependencies
RUN apk update && \
    apk add git && \
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

# Why? Errors if this is removed
COPY .git .git

# Compile
RUN mkdir bin && \
    make build

# Build a fresh container with just the binaries
FROM alpine

WORKDIR /go/src/github.com/portworx/torpedo

# Copy just ginkgo & binaries over from previous container
COPY --from=build /go/bin/ginkgo /bin/ginkgo
COPY --from=build /go/src/github.com/portworx/torpedo/bin bin
COPY drivers drivers

ENTRYPOINT ["ginkgo", "--failFast", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
