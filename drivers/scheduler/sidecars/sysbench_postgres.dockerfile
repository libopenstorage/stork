FROM severalnines/sysbench
RUN apt-get update && apt-get install -y postgresql-client
COPY scripts/sysbench_postgres.sh /entry-point.sh
RUN chmod u+x /entry-point.sh
ENTRYPOINT ["/entry-point.sh"]
