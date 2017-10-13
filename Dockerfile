FROM golang:1.8.3
MAINTAINER harsh@portworx.com

ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN go get github.com/onsi/ginkgo/ginkgo
RUN go install github.com/onsi/ginkgo/ginkgo
RUN go get github.com/onsi/gomega

ADD . /go/src/github.com/portworx/torpedo
WORKDIR /go/src/github.com/portworx/torpedo

ENTRYPOINT ["ginkgo", "--failFast", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
