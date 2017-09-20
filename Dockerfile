FROM fedora:25

WORKDIR /

COPY ./bin/torpedo /
COPY ./drivers/scheduler/k8s/specs /specs/k8s

ENTRYPOINT ["/torpedo"]
CMD []
