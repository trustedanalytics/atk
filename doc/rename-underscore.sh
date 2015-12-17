#!/bin/bash
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

popd