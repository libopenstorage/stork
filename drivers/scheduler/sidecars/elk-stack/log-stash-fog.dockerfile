FROM golang:1.17.13
ENV CGO_ENABLED=0
ENV GO111MODULE=on
USER root
WORKDIR /go/src
RUN cd /go/src && git clone https://github.com/mingrammer/flog.git
RUN cd /go/src/flog && go build -o /bin/flog
COPY scripts/elk-stack/flog.sh /go/src/entry-point.sh
RUN chmod 777 /go/src/entry-point.sh
ENTRYPOINT ["/go/src/entry-point.sh"]
