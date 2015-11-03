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
log "Create csd"

PACKAGE_NAME=TRUSTEDANALYTICS

pkgFolder=$PACKAGE_NAME-$VERSION.$BUILD_NUMBER
pkgPath=$SCRIPTPATH/$PACKAGE_NAME.$VERSION-$BUILD_NUMBER

rm -rf $pkgPath

pushd $SCRIPTPATH
env

mkdir -p $pkgFolder
cp -Rv csd/* $pkgFolder/
sed -i "s/VERSION/${VERSION}/g" $pkgFolder/descriptor/service.sdl
sed -i "s/BUILD/${BUILD_NUMBER}/g" $pkgFolder/descriptor/service.sdl

if [ ! -z  "$INTERNAL_REPO_SERVER" ]; then
    sed -i "s/PARCELURL/http:\/\/${INTERNAL_REPO_SERVER}\/packages\/cloudera\/${BRANCH}/g" $pkgFolder/descriptor/service.sdl
fi


pushd $pkgFolder
    jar -cvf $pkgFolder.jar *
popd


popd

rm -rf $pkgFolder