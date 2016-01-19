#!/bin/bash

pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

while [ "$pid" != "" ];
do
        echo $pid
        kill -9 $pid
        pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

done
pushd ..
  nohup bin/rest-server.sh > out 2>&1&
  PID=$!
popd

echo "PID: " $PID
max=100
count=0

while [ $count -lt $max ];
do
    for pid in `pstree -pn $PID | grep -o "[[:digit:]]*"`
    do
        bound=$(netstat -tulnp  2>/dev/null | grep $pid)
        if [ "$bound" != "" ]; then
            echo server is up
            exit 0
        fi
    done
    sleep 2
    count=$((count+1))
done

tail ../out

netstat -tulnp