#!/bin/sh
trap '' INT
trap '' QUIT
trap '' STOP
until mysql -h ${MYSQL_HOST} -u root -p${MYSQL_ROOT_PASSWORD} -e "CREATE SCHEMA IF NOT EXISTS sbtest ; CREATE USER IF NOT EXISTS sysbench@'%' IDENTIFIED BY 'password'; GRANT ALL PRIVILEGES ON sbtest.* to sysbench@'%';"
do
  echo "failed to create schema and user. retrying..."
  sleep 2
done
while :
do
    sysbench --db-driver=mysql --oltp-table-size=1000000 --oltp-tables-count=1000 --threads=5 --mysql-host=127.0.0.1 --mysql-port=3306 --mysql-user=sysbench --mysql-password=password /usr/share/sysbench/tests/include/oltp_legacy/parallel_prepare.lua prepare
    sysbench --db-driver=mysql --report-interval=1 --mysql-table-engine=innodb --oltp-table-size=1000000 --threads=5 --time=5400 --mysql-host=${MYSQL_HOST} --mysql-port=3306 --mysql-user=sysbench --mysql-password=password --olpt-skip-trx /usr/share/sysbench/tests/include/oltp_legacy/oltp.lua run
    sysbench --db-driver=mysql --oltp-table-size=1000000 --oltp-tables-count=1000 --threads=5 --mysql-host=${MYSQL_HOST} --mysql-port=3306 --mysql-user=sysbench --mysql-password=password /usr/share/sysbench/tests/include/oltp_legacy/oltp.lua cleanup
    sleep 2
done