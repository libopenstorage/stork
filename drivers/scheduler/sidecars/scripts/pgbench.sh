#!/bin/sh

if [ -z "${SIZE}" ]; then
    echo "SIZE is not defined, exiting..."
    exit 1
fi

pgbench_folder=/pgbench
pgbench_state_file=${pgbench_folder}/pgbench_state.file

while :
do
    if test -f "$pgbench_state_file"; then
        echo "all done"
        while :
        do
            sleep 600
        done
    else
        expected_data_size=$(($SIZE * 1024 * 1024))
        actual_data_size=$(du -s /var/lib/postgresql/* | cut -f1)
        if [ $actual_data_size -le $expected_data_size ]; then
            echo "data size of ${actual_data_size} is less than ${expected_data_size} expected data size"
            timestamp=$(date +%s)
            database="pxdemo_${timestamp}"
            echo "Using database ${database}"
            psql -h ${PG_HOST} -U ${PG_USER} -c "create database ${database}"
            if [ $? -ne 0 ]; then exit 1; fi
            pgbench -h ${PG_HOST} -U ${PG_USER} -i -s 10 ${database}
            if [ $? -ne 0 ]; then exit 1; fi
        else
            touch $pgbench_state_file
        fi
    fi
done
