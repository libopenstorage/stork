FROM golang:1.21.6-alpine AS build
LABEL maintainer="harsh@portworx.com"
ARG MAKE_TARGET

WORKDIR /go/src/github.com/portworx/torpedo
ENV NEXUS_URL="https://nexus.pwx.dev.purestorage.com"

# Install setup dependencies
RUN apk update && apk add --no-cache bash git gcc musl-dev make curl openssh-client coreutils python3

RUN GOFLAGS= GO111MODULE=on go install -mod=mod github.com/onsi/ginkgo/v2/ginkgo@v2.15.0

# Install aws-iam-authenticator
# This is needed by test running inside EKS cluster and creating aws entities like bucket etc.
RUN mkdir bin && \
    curl -o aws-iam-authenticator https://amazon-eks.s3.us-west-2.amazonaws.com/1.16.8/2020-04-16/bin/linux/amd64/aws-iam-authenticator && \
    chmod a+x aws-iam-authenticator && \
    mv aws-iam-authenticator bin

# Install IBM Cloud SDK
RUN curl -fskSL $NEXUS_URL/repository/store/ibmcloud/linux | bash && \
    ibmcloud plugin install -v 11.3.0 -f vpc-infrastructure && \
    ibmcloud plugin install -f container-service

# Install vCluster binary
RUN curl -L -o vcluster "https://github.com/loft-sh/vcluster/releases/latest/download/vcluster-linux-amd64"  \
    && install -c -m 0755 vcluster /usr/local/bin  \
    && rm -f vcluster



# No need to copy *everything*. This keeps the cache useful
COPY vendor vendor
COPY Makefile Makefile
COPY go.mod go.mod
COPY go.sum go.sum
COPY pkg pkg
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
FROM alpine:3.18.5

RUN apk add --no-cache ca-certificates bash curl jq libc6-compat coreutils

 # Install Azure Cli
RUN apk add --no-cache --update python3 py3-pip
RUN apk add --no-cache --update --virtual=build gcc musl-dev python3-dev libffi-dev openssl-dev cargo make && pip3 install "pyyaml<=5.3.1" && pip3 install --no-cache-dir --prefer-binary azure-cli && apk del build


# Install Oracle CLI
RUN bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)" -- --python-install-location /usr/bin/python3 --accept-all-defaults && \
    chmod a+x /root/bin/oci && \
    mv -f /root/bin/oci /usr/bin/oci


# Install kubectl from Docker Hub.
COPY --from=lachlanevenson/k8s-kubectl:latest /usr/local/bin/kubectl /usr/local/bin/kubectl

# Install helm from Docker Hub
COPY --from=alpine/helm:latest /usr/bin/helm /usr/local/bin/helm

# Copy scripts into container
WORKDIR /torpedo
COPY deployments deployments
COPY scripts scripts

# Install Postman-Newman Dependencies
RUN apk update && apk upgrade \
    && apk add --no-cache \
        nodejs \
        npm \
    && rm -rf /var/cache/apk/*

# Install Newman globally using npm
RUN npm install -g newman

WORKDIR /go/src/github.com/portworx/torpedo

# Install docker
RUN apk add --update --no-cache docker

# Install openssh and sshpass
RUN apk add --no-cache openssh sshpass

# Install dependancy for OCP 4.14 CLI
RUN apk --update add gcompat

# Install yq
RUN wget https://github.com/mikefarah/yq/releases/download/v4.25.1/yq_linux_amd64 -O /usr/bin/yq && \
    chmod +x /usr/bin/yq

# Copy ginkgo & binaries over from previous container
COPY --from=build /go/bin/ginkgo /bin/ginkgo
COPY --from=build /go/src/github.com/portworx/torpedo/bin bin
COPY --from=build /go/src/github.com/portworx/torpedo/bin/aws-iam-authenticator /bin/aws-iam-authenticator
COPY --from=build /usr/local/bin/ibmcloud /bin/ibmcloud
COPY --from=build /usr/local/bin/vcluster /bin/vcluster
COPY --from=build /root/.bluemix/plugins /root/.bluemix/plugins
COPY drivers drivers

ENTRYPOINT ["ginkgo", "--fail-fast", "--poll-progress-after", "3m", "-v", "-trace", "--junit-report=/testresults/junit_basic.xml"]
CMD []
