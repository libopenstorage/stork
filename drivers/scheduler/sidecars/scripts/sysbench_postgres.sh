#!/bin/sh
trap '' INT
trap '' QUIT
trap '' STOP
until psql -h ${PG_HOST} -p ${PGPORT} -U postgres -v ON_ERROR_STOP=1 <<-EOSQL
     CREATE USER "sbtest" WITH PASSWORD 'Password1';
     CREATE DATABASE "sbtest";
     GRANT ALL PRIVILEGES ON DATABASE "sbtest" TO "sbtest";
EOSQL
do
  echo "failed to create schema and user. retrying..."
  sleep 2
done
while :
do
  sysbench --db-driver=pgsql --oltp-table-size=${SYSTABSIZE} --oltp-tables-count=64 --threads=32 --time=${SYSBENCHTIM} --pgsql-host=${PG_HOST} --pgsql-port=${PGPORT} --pgsql-user=sbtest --pgsql-password=Password1 --pgsql-db=sbtest /usr/share/sysbench/tests/include/oltp_legacy/parallel_prepare.lua prepare
  sysbench --db-driver=pgsql --oltp-table-size=${SYSTABSIZE} --oltp-tables-count=64 --threads=32 --time=${SYSBENCHTIM} --pgsql-host=${PG_HOST} --pgsql-port=${PGPORT} --pgsql-user=sbtest --pgsql-password=Password1 --pgsql-db=sbtest /usr/share/sysbench/tests/include/oltp_legacy/parallel_prepare.lua run
  sysbench --db-driver=pgsql --oltp-table-size=${SYSTABSIZE} --oltp-tables-count=64 --threads=32 --time=${SYSBENCHTIM} --pgsql-host=${PG_HOST} --pgsql-port=${PGPORT} --pgsql-user=sbtest --pgsql-password=Password1 --pgsql-db=sbtest /usr/share/sysbench/tests/include/oltp_legacy/parallel_prepare.lua cleanup
  sleep 2
done
