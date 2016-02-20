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
log create parcel

PACKAGE_NAME=TRUSTEDANALYTICS
parcelDir=$PACKAGE_NAME-$VERSION-$BUILD_NUMBER.cdh5.3.0
tarFile=$2

rm -rf $SCRIPTPATH/$parcelDir


pushd $SCRIPTPATH

if [ -d /root/python ]; then
    cp -Rv /root/python/* python/
fi

mkdir -p $parcelDir/tmp
mkdir -p $parcelDir/log

cp -Rv parcel/* $parcelDir

sed -i "s/VERSION/${VERSION}/g" $parcelDir/meta/parcel.json
sed -i "s/BUILD/${BUILD_NUMBER}/g" $parcelDir/meta/parcel.json

tar -xvf $2 -C $parcelDir/

for pythonPackage in `ls python/`
do
    echo $pythonPackage
    tar -xvf python/${pythonPackage} -C $parcelDir/
done

mkdir -p $parcelDir/usr/lib/python2.7/site-packages/trustedanalytics
cp -Rv $parcelDir/usr/lib/trustedanalytics/rest-client/python/* $parcelDir/usr/lib/python2.7/site-packages/trustedanalytics
rm -rf $parcelDir/usr/lib/trustedanalytics/rest-client/python

tar zcvf $parcelDir-el6.parcel $parcelDir/ --owner=root --group=root

popd

rm -rf $parcelDir
#[name]-[version]-[distro suffix].parcel


#csd naming
#<name>-<csd-version>-<extra>.jar

