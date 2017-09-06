FROM ubuntu

WORKDIR /

COPY ./bin/stork /
ENTRYPOINT ["/stork"]
CMD []
