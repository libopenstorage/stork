FROM alpine:3.7

WORKDIR /src
COPY www .

CMD cp -r html/* /wordpress/
