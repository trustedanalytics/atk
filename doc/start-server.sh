#!/bin/bash
#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#


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
max=10
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