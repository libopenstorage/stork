FROM alpine:latest
MAINTAINER Portworx Inc. <support@portworx.com>

RUN wget -O /usr/local/bin/aws-iam-authenticator https://amazon-eks.s3-us-west-2.amazonaws.com/1.10.3/2018-07-26/bin/linux/amd64/aws-iam-authenticator
RUN chmod +x /usr/local/bin/aws-iam-authenticator

WORKDIR /

COPY ./bin/stork /
COPY ./bin/linux/storkctl /
