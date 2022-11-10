#!/bin/sh
while :
do
 flog -t log -f apache_combined -o /tmp/combinedappache.log -b 10485760 -p 1048576 -w
 flog -t log  -f rfc3164  -o /tmp/sys.log -b 10485760 -p 1048576 -w
 flog -t log  -f apache_common  -o /tmp/apachecommon.log -b 10485760 -p 1048576 -w
 flog -t log  -f apache_error  -o /tmp/apacheerror.log -b 10485760 -p 1048576 -w
 echo "Waiting 10 seconds to transfer logs"
 sleep 10
done
