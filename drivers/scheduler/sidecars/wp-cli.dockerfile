FROM wordpress:cli

WORKDIR /home/www-data

USER root
RUN apk add --update bash less curl

ARG wppluginurl=https://downloads.wordpress.org/plugin/

ADD $wppluginurl/bj-lazy-load.zip /home/www-data/	
ADD $wppluginurl/contact-form-7.5.0.4.zip /home/www-data/
ADD $wppluginurl/woocommerce.3.4.5.zip  /home/www-data/
ADD $wppluginurl/wordpress-seo.8.3.zip /home/www-data/	

RUN chmod -R 777 /home/www-data/
COPY scripts/wp-install.sh .

USER www-data
CMD ./wp-install.sh
