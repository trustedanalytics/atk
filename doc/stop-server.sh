#!/bin/bash
# rest servers started with module loader
pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

while [ "$pid" != "" ];
do
        echo $pid
        kill -9 $pid
        pid=$(ps -aux  | grep -v grep | grep "java.*org.trustedanalytics.atk.moduleloader.Module" | head -n 1 |  awk '{print $2}')

done