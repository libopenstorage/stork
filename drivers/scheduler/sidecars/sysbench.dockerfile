FROM severalnines/sysbench
RUN apt-get update && apt-get install -y mysql-client
COPY --chmod=777 scripts/sysbench.sh /entry-point.sh
ENTRYPOINT ["/entry-point.sh"]