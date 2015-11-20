#!/bin/bash

pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

while [ "$pid" != "" ];
do
        echo $pid
        kill -9 $pid
        pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

done
nohup ../bin/rest-server.sh > out 2>&1&
PID=$!

echo "PID: " $PID
max=10
count=0

while [ $count -lt $max ];
do
    for pid in `pstree -pn $PID | grep -o "[[:digit:]]*"`
    do
        bound=$(netstat -tulnp | grep $pid)
        if [ "$bound" != "" ]; then
            echo server is up
            exit 0
        fi
    done
    sleep 2
    count=$((count+1))
done