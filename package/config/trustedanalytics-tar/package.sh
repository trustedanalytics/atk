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

package=$1
BUILD_NUMBER=$2
VERSION=$3
workDir=$(pwd)
baseDir=${workDir##*/}
gitRoot="."
if [ "$baseDir" == "package" ]; then
	source common.sh
	  gitRoot=".."
	else
	source package/common.sh
	gitRoot="."
fi

echo "$SCRIPTPATH"

#pushd $SCRIPTPATH

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz

mkdir -p  tarballs/$package/bin
mkdir -p  tarballs/$package/conf
mkdir -p  tarballs/$package/lib
mkdir -p  tarballs/$package/daal


cp -v  config/$package/application.conf tarballs/$package/conf
cp -v  config/$package/logback.xml tarballs/$package/conf
cp -Rv config/trustedanalytics/assets/etc/trustedanalytics/rest-server/* tarballs/$package/conf
cp -v  config/$package/rest-server.sh tarballs/$package/bin/
cp -v  config/$package/jq tarballs/$package/
cp -v  config/$package/daal.tar.gz tarballs/$package/

jars="rest-server.jar  engine-core.jar  interfaces.jar  deploy.jar scoring-models.jar"

#popd

pushd $gitRoot
for jar in $jars
do
	jarPath=$(find .  -path ./package -prune -o -name $jar -print)
	echo $jarPath
	cp -v $jarPath package/tarballs/$package/lib/

done

jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)

echo $jarPath
#enable this to copy the regular launcher.jar to the correct place
cp -v $jarPath package/tarballs/$package/launcher.jar


mkdir -p package/tarballs/$package/client
ls -l package/config/trustedanalytics-python-client/trustedanalytics/dist
cp -v package/config/trustedanalytics-python-client/trustedanalytics/dist/trustedanalytics*.tar.gz package/tarballs/$package/client
ls -l python-client/target
cp -v python-client/target/trustedanalytics.zip package/tarballs/$package/lib/

popd


pushd tarballs/$package
    tar -pczf ../../trustedanalytics.tar.gz .
popd



rm -rf tarballs
