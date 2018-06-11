FROM registry.access.redhat.com/rhel7-atomic
MAINTAINER Portworx Inc. <support@portworx.com>

WORKDIR /

COPY ./bin/mysql-locker /
ENTRYPOINT ["./mysql-locker"]
CMD ["-h"]
