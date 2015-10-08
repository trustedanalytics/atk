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

mkdir -p  tarballs/$package/bin
mkdir -p  tarballs/$package/conf
mkdir -p  tarballs/$package/lib
mkdir -p  tarballs/$package/data

cp -v  config/$package/logback.xml tarballs/$package/conf

cp -v config/$package/scoring-server.sh tarballs/$package/bin/
cp -v config/$package/application.conf tarballs/$package/conf

cp -v scoring-engine-lib/target/dependency/*.jar package/tarballs/$package/lib/

pushd tarballs/$package
    tar -pczf ../../trustedanalytics-scoring.tar.gz .
pop


rm -rf tarballs
