#!/bin/sh

if [ -z "${SIZE}" ]; then
    echo "SIZE is not defined, exiting..."
    exit 1
fi


if [ -z "${IO_PORTION}" ]; then
    echo "IO_PORTION is not defined, defaulting to 1, meaning pod will wait till ${SIZE} IO completes."
    IO_PORTION=1
fi

if [ -z "${DELAY}" ]; then
    DELAY=0
fi

pgbench_folder=/pgbench
pgbench_state_file=${pgbench_folder}/pgbench_state.file

readinessprobe_data_size=`echo - | awk "{print $SIZE * $IO_PORTION * 1024 * 1024}"`
expected_data_size=$(($SIZE * 1024 * 1024))

while :
do
    if [ $actual_data_size -ge $expected_data_size ]; then
        echo "all done"
        while :
        do
            sleep 600
        done
    else
        actual_data_size=$(du -s /var/lib/postgresql/* | cut -f1)

        echo "Expected size: $expected_data_size"
        echo "Actual size: $actual_data_size"

        if [ $actual_data_size -le $expected_data_size ]; then
            echo "data size of ${actual_data_size} is less than ${expected_data_size} expected data size"
            timestamp=$(date +%s)
            database="pxdemo_${timestamp}"
            echo "Using database ${database}"
            psql -h ${PG_HOST} -U ${PG_USER} -c "create database ${database}"
            if [ $? -ne 0 ]; then exit 1; fi
            pgbench -h ${PG_HOST} -U ${PG_USER} -i -s 10 ${database}
            if [ $? -ne 0 ]; then exit 1; fi
            echo "Sleeping for ${DELAY} seconds"
            sleep $DELAY
        fi

        if [ $actual_data_size -ge $readinessprobe_data_size ]; then
            echo "Expected size: $expected_data_size"
            echo "Actual size: $actual_data_size"
            echo "actual data size is now greater than readinessprobe data size. Going to create a state file now."
            mkdir -p $pgbench_folder
            touch $pgbench_state_file
        fi
    fi
done
