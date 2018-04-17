FROM wordpress:cli

WORKDIR /home/www-data

USER root
RUN apk add --update bash less curl
USER www-data

COPY scripts/wp-install.sh .

CMD ./wp-install.sh
