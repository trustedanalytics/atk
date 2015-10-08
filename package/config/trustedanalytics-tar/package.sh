#!/bin/bash
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

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz

echo "create package directories"
mkdir -pv  tarballs/$package/bin
mkdir -pv  tarballs/$package/conf
mkdir -pv  tarballs/$package/lib

echo "copy config into package"
cp -v  config/$package/application.conf tarballs/$package/conf
cp -v  config/$package/logback.xml tarballs/$package/conf
cp -Rv config/trustedanalytics/assets/etc/trustedanalytics/rest-server/* tarballs/$package/conf
cp -v  config/$package/rest-server.sh tarballs/$package/bin/
cp -v  config/$package/jq tarballs/$package/

echo "copy jar dependencies"
cp -v rest-server-lib/target/dependency/*.jar tarballs/$package/lib/

mkdir -pv tarballs/$package/client
ls -l config/trustedanalytics-python-client/trustedanalytics/dist
cp -v config/trustedanalytics-python-client/trustedanalytics/dist/trustedanalytics*.tar.gz tarballs/$package/client
ls -l ../python-client/target
cp -v ../python-client/target/trustedanalytics.zip tarballs/$package/lib/

pushd tarballs/$package
    tar -pczf ../../trustedanalytics.tar.gz .
popd



rm -rf tarballs
