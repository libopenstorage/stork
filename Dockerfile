FROM golang:1.8.3
MAINTAINER harsh@portworx.com

ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN go get github.com/onsi/ginkgo/ginkgo
RUN go install github.com/onsi/ginkgo/ginkgo
RUN go get github.com/onsi/gomega

ADD . /go/src/github.com/portworx/torpedo
WORKDIR /go/src/github.com/portworx/torpedo

ENTRYPOINT ["ginkgo", "--slowSpecThreshold", "180", "-v", "-trace"]
CMD []
#CMD ["ginkgo", "-dryRun", "--slowSpecThreshold", "180", "bin/*.test", "--", "--spec-dir","pxd"]
