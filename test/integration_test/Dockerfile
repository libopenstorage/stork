FROM golang:1.16.1

# Install dependancies
RUN apt-get update && \ 
    /usr/local/go/bin/go install gotest.tools/gotestsum@latest

RUN apt-get update && apt-get install -y python3-pip && apt-get install -y jq

RUN pip3 install --upgrade pip

#Install Google Cloud SDK
ARG GCLOUD_SDK=google-cloud-sdk-418.0.0-linux-x86_64.tar.gz
ARG GCLOUD_INSTALL_DIR="/usr/lib"
RUN curl -q -o $GCLOUD_SDK https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK -C $GCLOUD_INSTALL_DIR && rm -rf $GCLOUD_SDK && \
    rm -rf $GCLOUD_INSTALL_DIR/google-cloud-sdk/platform/gsutil \
           $GCLOUD_INSTALL_DIR/google-cloud-sdk/RELEASE_NOTES
ENV PATH "${PATH}:$GCLOUD_INSTALL_DIR/google-cloud-sdk/bin"
#Install gke-gcloud-auth-plugin
RUN gcloud components install gke-gcloud-auth-plugin

RUN wget -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator && \
    chmod +x /usr/local/bin/aws-iam-authenticator

RUN pip3 install awscli

# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin

WORKDIR /

COPY stork.test /
COPY specs /specs/
COPY stork-test-pod.yaml test-deploy.sh /testspecs/
COPY stork-specs /stork-specs/
COPY --from=alpine/helm:latest /usr/bin/helm /usr/local/bin/helm