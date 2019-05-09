FROM golang:1.11.2 

# Install dependancies
RUN apt-get update && \ 
    /usr/local/go/bin/go get -u gotest.tools/gotestsum

WORKDIR /

COPY stork.test /
COPY specs /specs/
