#!/bin/bash
source common.sh

log "Build rpm package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

#deleteOldBuildDirs

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy and rename: $tarFile"
log "mkdir -p $SCRIPTPATH/rpm/SOURCES"
mkdir -p $SCRIPTPATH/rpm/SOURCES
log "cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz"
cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz


LICENSE="Apache"
SUMMARY="Trusted analytics atk rest server"
DESCRIPTION="$SUMMARY "
day=$(date +"%Y%m%d")
REQUIRES=" java-1.7.0-openjdk, trustedanalytics-python-client >= ${version}-${day}${BUILD_NUMBER}, python-argparse, python-cm-api, postgresql-server"

REQUIRES=" java-1.7.0-openjdk, trustedanalytics-python-client >= ${version}-${BUILD_NUMBER}, python-argparse, python-cm-api, postgresql-server"

PRE="
restUser=atkuser
if [ \"\`cat /etc/passwd | grep \$restUser\`\" == \"\" ]; then
	echo create \$restUser
	useradd -G hadoop \$restUser
fi

hadoop fs -ls /user/\$restUser 2>/dev/null
if [ \$? -eq 1 ]; then
	echo create \$restUser hdfs home directory
	su -c \"hadoop fs -mkdir /user/\$restUser\" hdfs
	su -c \"hadoop fs -chown \$restUser:\$restUser /user/\$restUser\" hdfs
	su -c \"hadoop fs -chmod 755 /user/\$restUser\" hdfs
fi
"

POST="
restUser=atkuser

if [ \$1 -eq 2 ]; then
  echo start trustedanalytics
  service trustedanalytics restart
fi

hadoop fs -ls /user/\$restUser/datasets 2>/dev/null
if [ \$? -eq 1 ]; then
	echo move sample data scripts and data sets
	cp -R /usr/lib/trustedanalytics/rest-server/examples /home/\$restUser
	chown -R \$restUser:\$restUser /home/\$restUser/examples
	su -c \"hadoop fs -put ~/examples/datasets \" \$restUser
fi

"

PREUN="
 checkStatus=\$(service trustedanalytics status | grep start/running)
 if  [ \$1 -eq 0 ] && [ \"\$checkStatus\" != \"\" ]; then
    echo stopping trustedanalytics
    service trustedanalytics stop
 fi
"

FILES="
/etc/trustedanalytics/rest-server
/usr/lib/trustedanalytics/rest-server
"


log "mkdir -p $SCRIPTPATH/rpm/SPECS"
mkdir -p $SCRIPTPATH/rpm/SPECS
log "rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec"
env
rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec

log "topdir "
topDir="$SCRIPTPATH/rpm"
#exit 1
pushd $SCRIPTPATH/rpm

log "clean up build dirs"
rm -rf BUILD/*
rm -rf BUILDROOT/*


log $BUILD_NUMBER
pwd
rpmbuild --define "_topdir $topDir"  --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -bb SPECS/$packageName.spec

cleanRpm

popd

