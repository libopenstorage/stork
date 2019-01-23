FROM bash:4.4
LABEL maintainer="harsh@portworx.com"

WORKDIR /

ADD pull_docker.sh /pull_docker.sh

RUN apk --no-cache add openssl wget
RUN \
  wget --no-check-certificate -q https://download.docker.com/linux/static/stable/x86_64/docker-17.03.0-ce.tgz && \
  tar -xvzf docker-17.03.0-ce.tgz && \
  cp docker/docker /bin/ && \
  rm -rf docker* && \
  chmod +x /bin/docker

ENTRYPOINT ["bash", "/pull_docker.sh"]
CMD []
