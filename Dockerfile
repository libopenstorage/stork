FROM registry.access.redhat.com/ubi8-minimal:latest
MAINTAINER Portworx Inc. <support@portworx.com>

ARG VERSION=master
ARG RELEASE=latest
LABEL name="Stork" \
    vendor="Openstorage.org" \
    version=${VERSION} \
    release=${RELEASE} \
    summary="Storage Operator Runtime for Kubernetes" \
    description="Stork is a Cloud Native storage operator runtime scheduler plugin"

RUN microdnf clean all && microdnf install -y python3.9 ca-certificates tar gzip openssl curl git findutils unzip

RUN python3 -m pip install awscli && python3 -m pip install oci-cli && python3 -m pip install rsa --upgrade


RUN curl -Lo /usr/local/bin/aws-iam-authenticator https://github.com/kubernetes-sigs/aws-iam-authenticator/releases/download/v0.6.11/aws-iam-authenticator_0.6.11_linux_amd64 && \
    chmod +x /usr/local/bin/aws-iam-authenticator

#Install asdf
RUN git clone https://github.com/asdf-vm/asdf.git $HOME/.asdf --branch v0.12.0
RUN echo -e '\n. "$HOME/.asdf/asdf.sh"' >> ~/.bashrc
RUN echo -e '\n. "$HOME/.asdf/completions/asdf.bash"' >> ~/.bashrc
RUN . $HOME/.asdf/asdf.sh
ENV PATH "${PATH}:$HOME/root/.asdf/bin:$HOME/root/.asdf/shims"
#Install kubelogin plugin
RUN asdf update
RUN asdf plugin add kubelogin
RUN asdf install kubelogin latest
RUN asdf global kubelogin latest

#Install Google Cloud SDK
ARG GCLOUD_SDK=google-cloud-cli-455.0.0-linux-x86_64.tar.gz
ARG GCLOUD_INSTALL_DIR="/usr/lib"
RUN curl -q -o $GCLOUD_SDK https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK -C $GCLOUD_INSTALL_DIR && rm -rf $GCLOUD_SDK && \
    rm -rf $GCLOUD_INSTALL_DIR/google-cloud-sdk/platform/gsutil \
    $GCLOUD_INSTALL_DIR/google-cloud-sdk/RELEASE_NOTES
ENV PATH "${PATH}:$GCLOUD_INSTALL_DIR/google-cloud-sdk/bin"
#Install gke-gcloud-auth-plugin
RUN gcloud components install gke-gcloud-auth-plugin
#Create symlink /google-cloud-sdk/bin -> /usr/lib/google-cloud-sdk/bin for legacy cluster pair with gcp auth plugin
RUN mkdir google-cloud-sdk
RUN ln -s /usr/lib/google-cloud-sdk/bin /google-cloud-sdk/bin

WORKDIR /

COPY ./bin/linux/storkctl /storkctl/linux/
COPY ./bin/darwin/storkctl /storkctl/darwin/
COPY ./bin/windows/storkctl.exe /storkctl/windows/
COPY ./LICENSE /licenses
COPY ./bin/stork /
COPY ./bin/px_statfs.so /
COPY ./bin/px_statfs.so.sha256 /
