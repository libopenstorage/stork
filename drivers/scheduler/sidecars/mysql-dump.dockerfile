FROM mysql:5.7.21

COPY www/WordPress.sql .
COPY scripts/mysql-dump.sh .

CMD ./mysql-dump.sh
