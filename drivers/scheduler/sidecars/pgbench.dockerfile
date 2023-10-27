FROM postgres:latest

RUN apt-get update 

COPY --chmod=777 scripts/pgbench.sh /entry-point.sh

RUN chmod u+x /entry-point.sh

ENTRYPOINT ["/entry-point.sh"]
