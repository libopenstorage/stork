#!/usr/bin/env bash
set -x

pid=0

esload() {
	while true; do
		python elasticsearch-stress-test.py "$@"
		sleep 5
	done
}

# SIGTERM-handler
term_handler() {
	if [ $pid -ne 0 ]; then
		kill -SIGTERM "$pid"
		wait "$pid"
	fi
	exit 143; # 128 + 15 -- SIGTERM
}

# setup handlers
# on callback, kill the last background process, which is "tail -f"
trap 'kill ${!}; term_handler' SIGTERM

# run app
esload "$@" &
pid="$!"

# wait forever
while true
do
	tail -f /dev/null & wait "${!}"
done

