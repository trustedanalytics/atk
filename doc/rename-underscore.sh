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

PREFIX=R
pushd build/html

for directory in `ls -d */ | grep '^_' `
do
  echo $directory
  rename=$PREFIX$directory
  mv $directory $PREFIX$directory
  IFS=$'\n'
  for file in `grep -raIn "$directory"`
  do
    f=$(echo $file | tr ":" " " | awk '{print $1}')
    grep -raIn "$rename" $f > /dev/null
    if [ $? -eq 1 ]; then
      sed -i "s|$directory|$rename|g" $f
    fi
  done
done

for init in `find . -name "__init__.html"`
do
    name=$(basename $init)
    fullDir=$(dirname $init)
    echo rename file: $init to $fullDir/$PREFIX$name

    mv $init $fullDir/$PREFIX$name
done

IFS=$'\n'
for file in `grep -raIn "__init__.html"`
do
   #echo $file
   f=$(echo $file | tr ":" " " | awk '{print $1}')
   echo $f
   grep -raIn "R__init__.html" $f > /dev/null
   if [ $? -eq 1 ]; then
      sed -i "s|__init__.html|R__init__.html|g" $f
   fi
done

popd
