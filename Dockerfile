FROM fedora:25

WORKDIR /

COPY ./bin/torpedo /
ENTRYPOINT ["/torpedo"]
CMD []
