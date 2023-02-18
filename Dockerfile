FROM golang:1.19.5-alpine AS build
LABEL maintainer="harsh@portworx.com"
ARG MAKE_TARGET

WORKDIR /go/src/github.com/portworx/torpedo

# Install setup dependencies
RUN apk update && apk add --no-cache bash git gcc musl-dev make curl openssh-client

RUN GOFLAGS= GO111MODULE=on go install github.com/onsi/ginkgo/ginkgo@v1.16.5

# Install aws-iam-authenticator
# This is needed by test running inside EKS cluster and creating aws entities like bucket etc.
RUN mkdir bin && \
    curl -o aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.16.8/2020-04-16/bin/linux/amd64/aws-iam-authenticator && \
    chmod a+x aws-iam-authenticator && \
    mv aws-iam-authenticator bin

# No need to copy *everything*. This keeps the cache useful
COPY vendor vendor
COPY Makefile Makefile
COPY go.mod go.mod
COPY go.sum go.sum
COPY pkg pkg
COPY porx porx
COPY scripts scripts
COPY drivers drivers
COPY deployments deployments

# Why? Errors if this is removed
COPY .git .git

# copy tests last to allow caching of the previous docker image layers
COPY tests tests

# Compile
RUN --mount=type=cache,target=/root/.cache/go-build make $MAKE_TARGET

# Build a fresh container with just the binaries
FROM alpine

RUN apk add --no-cache ca-certificates bash curl jq libc6-compat

# Install kubectl from Docker Hub.
COPY --from=lachlanevenson/k8s-kubectl:latest /usr/local/bin/kubectl /usr/local/bin/kubectl

# Install helm from Docker Hub
COPY --from=alpine/helm:latest /usr/bin/helm /usr/local/bin/helm

# Copy scripts into container
WORKDIR /torpedo
COPY deployments deployments
COPY scripts scripts

WORKDIR /go/src/github.com/portworx/torpedo

# Copy ginkgo & binaries over from previous container
COPY --from=build /go/bin/ginkgo /bin/ginkgo
COPY --from=build /go/src/github.com/portworx/torpedo/bin bin
COPY --from=build /go/src/github.com/portworx/torpedo/bin/aws-iam-authenticator /bin/aws-iam-authenticator
COPY drivers drivers

ENTRYPOINT ["ginkgo", "--failFast", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
