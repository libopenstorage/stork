FROM golang:1.17.0-alpine AS build
LABEL maintainer="harsh@portworx.com"

WORKDIR /go/src/github.com/portworx/torpedo

# Install setup dependencies
RUN apk update && apk add --no-cache git gcc  musl-dev make curl openssh-client

# No need to copy *everything*. This keeps the cache useful
COPY deployments deployments
COPY drivers drivers
COPY pkg pkg
COPY porx porx
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

# Install aws-iam-authenticator
# This is needed by test running inside EKS cluster and creating aws entities like bucket etc.
RUN curl -o aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.16.8/2020-04-16/bin/linux/amd64/aws-iam-authenticator && \
    chmod a+x aws-iam-authenticator && \
    mv aws-iam-authenticator bin

# Build a fresh container with just the binaries
FROM alpine

RUN apk add --no-cache ca-certificates curl jq libc6-compat

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
COPY --from=build /go/src/github.com/portworx/torpedo/bin/aws-iam-authenticator /bin/aws-iam-authenticator
COPY drivers drivers

ENTRYPOINT ["ginkgo", "--failFast", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
