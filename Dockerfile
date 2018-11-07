FROM alpine:latest
MAINTAINER Portworx Inc. <support@portworx.com>

RUN apk add tar python 

RUN wget -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator
RUN chmod +x /usr/local/bin/aws-iam-authenticator

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-224.0.0-linux-x86_64.tar.gz
RUN tar xvf google-cloud-sdk-224.0.0-linux-x86_64.tar.gz
RUN rm -rf google-cloud-sdk-224.0.0-linux-x86_64.tar.gz

WORKDIR /

COPY ./bin/stork /
COPY ./bin/linux/storkctl /
