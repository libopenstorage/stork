FROM postgres:latest

RUN apt-get update 

COPY scripts/pgbench.sh /entry-point.sh

RUN chmod u+x /entry-point.sh

ENTRYPOINT ["/entry-point.sh"]
