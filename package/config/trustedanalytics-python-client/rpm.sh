#!/bin/bash
#
#  Copyright (c) 2016 Intel Corporation 
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

source common.sh

log "Build rpm package"

packageName=$1
tarFile=$2
TAR_FILE=$tarFile
version=$3

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy and rename: $tarFile"
cp $tarFile $SCRIPTPATH/rpm/SOURCES/${packageName}-${version}.tar.gz

LICENSE="Apache"
#SUMMARY="$packageName-$version Build number: $BUILD_NUMBER. TimeStamp $TIMESTAMP"
DESCRIPTION=$SUMMARY
REQUIRES="python27, python27-ordereddict, python27-numpy >= 1.8.1, python27-bottle >= 0.12, python27-requests >= 2.2.1, python27-decorator >= 3.4.0, python27-pandas >= 0.15.0, python27-pymongo"

POST="
 #sim link to python sites packages
 if [ -d /usr/lib/python2.7/site-packages/trustedanalytics ]; then
   rm /usr/lib/python2.7/site-packages/trustedanalytics
 fi

 ln -s /usr/lib/trustedanalytics/python-client  /usr/lib/python2.7/site-packages/trustedanalytics
"

#delete the sym link only if we are uninstalling not updating
POSTUN="
 if  [ \$1 -eq 0 ]; then
    rm /usr/lib/python2.7/site-packages/trustedanalytics
 fi
"

FILES="
/usr/lib/trustedanalytics/python-client
%config /usr/lib/trustedanalytics/python-client/rest/config.py
"

mkdir -p $SCRIPTPATH/rpm/SPECS
rpmSpec > $SCRIPTPATH/rpm/SPECS/$packageName.spec

topDir="$SCRIPTPATH/rpm"
#exit 1
pushd $SCRIPTPATH/rpm

log "clean up build dirs"
rm -rf BUILD/*
rm -rf BUILDROOT/*


log $BUILD_NUMBER
rpmbuild --define "_topdir $topDir"  --define "BUILD_NUMBER $BUILD_NUMBER" --define "VERSION $VERSION" -bb SPECS/$packageName.spec

cleanRpm
popd

