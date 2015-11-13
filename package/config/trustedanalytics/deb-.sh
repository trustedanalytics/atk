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

source common.sh
log "Build deb package"

packageName=$1
tarFile=$2
version=$3

deleteOldBuildDirs

log "package name: $packageName, tar file: $tarFile, version: $version, script path: $SCRIPTPATH"

log "copy tar.gz and rename for packaing"
expandTarDeb

BUILD_DEPENDS="$BUILD_DEPENS"
#DEPENDS="\${java:Depends}"
DEPENDS="openjdk-7-jdk"
#RECOMMENDS="\${java:Recommends}"
SOURCE=$packageName
SUMMARY=" "
DESCRIPTION=$SUMMARY
SUBJECT="$DESCRIPTION
start the server with 'service trustedanalytics-rest-server status'
config files are in /etc/trustedanalytics/rest-server
log files live in /var/log/trustedanalytics/rest-server
"

debDir=${packageName}-${version}

mkdir -p $SCRIPTPATH/$debDir/debian

log "create control file"
debControl > $SCRIPTPATH/$debDir/debian/control

log "create compat file"
debCompat >  $SCRIPTPATH/$debDir/debian/compat

pwd
log "create install file"
debInstall > $SCRIPTPATH/$debDir/debian/$packageName.install

log "create copyright file"
debCopyright >  $SCRIPTPATH/$debDir/debian/copyright

RULESSETUP="JAVA_HOME=/usr/lib/jvm/default-java
JH_DEPENDS_ARGS=-vn"
#RULEOPT="--with javahelper  "
log "create rules file"
debRules > $SCRIPTPATH/$debDir/debian/rules
chmod +x $SCRIPTPATH/$debDir/debian/rules

pushd $SCRIPTPATH/$debDir

rm -rf debian/trustedanalytics

log "clean debian/source dir"
rm -rf debian/source
mkdir debian/source ; echo '3.0 (quilt)' > debian/source/format ; dch 'Switch to dpkg-source 3.0 (quilt) format'

log "create change log "
rm debian/changelog
debChangeLog

log "build deb package"
debuild -us -uc --source-option=--include-binaries --source-option=-isession

popd

cleanDeb
