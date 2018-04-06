#!/usr/bin/env bash

function mysql_command {
    mysql \
        -hwordpress-mysql \
        -uroot \
        -p${MYSQL_ROOT_PASSWORD} \
        "$@"
}

mysql_command -e "CREATE database wordpress" || true
mysql_command wordpress < WordPress.sql || true
