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
pwd
package="trustedanalytics-scoring-tar"
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

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz


echo "create package directories"
mkdir -pv  tarballs/$package/bin
mkdir -pv  tarballs/$package/conf
mkdir -pv  tarballs/$package/lib
mkdir -pv  tarballs/$package/data

pwd
ls -la
if [ -f licenses-scoring-engine.xml ]; then
        mv licenses-scoring-engine.xml tarballs/$package/licenses.xml
else
        echo no license file
fi


echo "copy config into package"
cp -v  config/$package/logback.xml tarballs/$package/conf
cp -v config/$package/scoring-server.sh tarballs/$package/bin/
cp -v config/$package/application.conf tarballs/$package/conf
cp -v config/$package/jq tarballs/$package/
cp -v config/$package/manifest.yml.tpl tarballs/$package/

echo "copy jar dependencies"
cp -v scoring-engine-lib/target/dependency/*.jar tarballs/$package/lib/

pushd tarballs/$package
    tar -pczf ../../trustedanalytics-scoring.tar.gz .
popd


rm -rf tarballs
