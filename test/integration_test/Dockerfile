FROM golang:1.16.1

# Install dependancies
RUN apt-get update && \ 
    /usr/local/go/bin/go install gotest.tools/gotestsum@latest

RUN apt-get update && apt-get install -y python3-pip && apt-get install -y jq

RUN pip3 install --upgrade pip

ARG GCLOUD_SDK=google-cloud-sdk-269.0.0-linux-x86_64.tar.gz
# Remove the test directories
# Also don't need gsutil
RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/$GCLOUD_SDK && \
    tar xf $GCLOUD_SDK && rm -rf $GCLOUD_SDK && \
    cp -r google-cloud-sdk/bin/* /usr/local/bin/  && \
    cp -r google-cloud-sdk/lib/* /usr/local/lib/  && \
    rm -rf /google-cloud-sdk/platform/gsutil/third_party/oauth2client/tests \
        /google-cloud-sdk/platform/gsutil/third_party/rsa/tests \
        /google-cloud-sdk/platform/gsutil/third_party/httplib2/python2/httplib2/test \
        /google-cloud-sdk/platform/gsutil && \
    python3 -m pip install pyyaml>=5.1 rsa>=4.0 urllib3>=1.24.2 --upgrade -t /google-cloud-sdk/lib/third_party

RUN wget -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator && \
    chmod +x /usr/local/bin/aws-iam-authenticator


# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin

WORKDIR /

COPY stork.test /
COPY specs /specs/
COPY stork-test-pod.yaml test-deploy.sh /testspecs/
COPY stork-specs /stork-specs/

