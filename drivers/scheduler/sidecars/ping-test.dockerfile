FROM alpine:3.7

RUN apk add --update curl && rm -rf /var/cache/apk/*
COPY scripts/ping-test.sh .

ENV HOST="localhost:80"

CMD ./ping-test.sh
