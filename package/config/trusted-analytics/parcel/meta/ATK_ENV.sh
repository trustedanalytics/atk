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


echo "ATK env sh"



export ATK_PARCEL_HOME="$PARCELS_ROOT/TRUSTEDANALYTICS"

export ATK_LOG="${ATK_PARCEL_HOME}/log"

export ATK_LAUNCHER_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics//rest-server"

export ATK_LAUNCHER_LIB_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics/rest-server/lib"

export ATK_SPARK_DEPS_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics/graphbuilder/lib"

export ATK_CONFIG_DIR="$ATK_PARCEL_HOME/etc/trustedanalytics/rest-server"

export ATK_LOGBACK_JARS="$ATK_LAUNCHER_LIB_DIR/logback-classic-1.1.1.jar:$ATK_LAUNCHER_LIB_DIR/logback-core-1.1.1.jar"

export ATK_PYTHON="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"

alias python="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"
alias python2.7="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"

export ATK_CLASSPATH="$CONF_DIR:$ATK_CONFIG_DIR:$ATK_LOGBACK_JARS:$ATK_LAUNCHER_DIR/launcher.jar:/etc/hbase/conf:/etc/hadoop/conf"

export ATK_TEMP="$PARCELS_ROOT/TRUSTEDANALYTICS/tmp"

export ATK_DOC_PATH="${ATK_PARCEL_HOME}/usr/lib/python2.7/site-packages/trustedanalytics/doc/html"