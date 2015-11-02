#!/bin/bash
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

echo "copy config into package"
cp -v  config/$package/logback.xml tarballs/$package/conf
cp -v config/$package/scoring-server.sh tarballs/$package/bin/
cp -v config/$package/application.conf tarballs/$package/conf
cp -v config/$package/jq tarballs/$package/

echo "copy jar dependencies"
cp -v scoring-engine-lib/target/dependency/*.jar tarballs/$package/lib/

pushd tarballs/$package
    tar -pczf ../../trustedanalytics-scoring.tar.gz .
popd


rm -rf tarballs
