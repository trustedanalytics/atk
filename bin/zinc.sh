#!/bin/bash
#/*
#// Copyright (c) ${currentYear} Intel Corporation 
#//
#// Licensed under the Apache License, Version 2.0 (the "License");
#// you may not use this file except in compliance with the License.
#// You may obtain a copy of the License at
#//
#//      http://www.apache.org/licenses/LICENSE-2.0
#//
#// Unless required by applicable law or agreed to in writing, software
#// distributed under the License is distributed on an "AS IS" BASIS,
#// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#// See the License for the specific language governing permissions and
#// limitations under the License.
#*/

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
