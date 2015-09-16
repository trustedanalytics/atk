#!/bin/bash
#download and start zinc server to enable incremental builds.
#./zinc.sh start to start the server
#./zinc.sh shutdown to stop it
ZINC_VERSION=0.3.7
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

echo $DIR

pushd $DIR

pwd

if [ ! -d zinc-$ZINC_VERSION ]; then
	wget http://downloads.typesafe.com/zinc/$ZINC_VERSION/zinc-$ZINC_VERSION.tgz -O `pwd`/zinc.tgz
  tar -xvf zinc.tgz
  rm zinc.tgz
fi

pushd zinc-$ZINC_VERSION

bin/zinc -$1

popd
popd
