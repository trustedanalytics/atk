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

#pushd $SCRIPTPATH

rm -rf ../bin/stage
rm -rf tarballs/$package
rm $package-source.tar.gz

mkdir -p  tarballs/$package/bin
mkdir -p  tarballs/$package/conf
mkdir -p  tarballs/$package/lib

cp -v  config/$package/logback.xml tarballs/$package/conf

cp -v config/$package/scoring-server.sh tarballs/$package/bin/
cp -v config/$package/application.conf tarballs/$package/conf
cp -v config/$package/jq tarballs/$package/

jars="scoring-engine.jar scoring-interfaces.jar"

#popd

pushd $gitRoot
for jar in $jars
do
	jarPath=$(find .  -path ./package -prune -o -name $jar -print )
	echo $jarPath
	cp -v $jarPath package/tarballs/$package/lib/

done

jarPath=$(find .  -path ./package -prune -o -name launcher.jar -print)

echo $jarPath
#enable this to copy the regular launcher.jar to the correct place
cp -v $jarPath package/tarballs/$package/launcher.jar


popd


pushd tarballs/$package
    tar -pczf ../../trustedanalytics-scoring.tar.gz .
popd


rm -rf tarballs
