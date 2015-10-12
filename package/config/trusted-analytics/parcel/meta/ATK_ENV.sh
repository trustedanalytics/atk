#!/bin/bash

echo "ATK env sh"



export ATK_PARCEL_HOME="$PARCELS_ROOT/TRUSTEDANALYTICS"

export ATK_LOG="${ATK_PARCEL_HOME}/log"

export ATK_LAUNCHER_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics//rest-server"

export ATK_LIB_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics/rest-server/lib"

export ATK_SPARK_DEPS_DIR="$ATK_PARCEL_HOME/usr/lib/trustedanalytics/graphbuilder/lib"

export ATK_CONFIG_DIR="$ATK_PARCEL_HOME/etc/trustedanalytics/rest-server"

export ATK_JARS="$ATK_LIB_DIR/module-loader-master-SNAPSHOT.jar:$ATK_LIB_DIR/scala-library-2.10.4.jar:$ATK_LIB_DIR/config-1.2.1.jar:$ATK_LIB_DIR/scala-reflect-2.10.4.jar:$ATK_LIB_DIR/logback-classic-1.1.1.jar:$ATK_LIB_DIR/logback-core-1.1.1.jar"

export ATK_PYTHON="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"

alias python="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"
alias python2.7="$PARCELS_ROOT/TRUSTEDANALYTICS/usr/bin/python2.7"

export ATK_CLASSPATH="$CONF_DIR:$ATK_CONFIG_DIR:$ATK_JARS:/etc/hbase/conf:/etc/hadoop/conf"

export ATK_TEMP="$PARCELS_ROOT/TRUSTEDANALYTICS/tmp"

export ATK_DOC_PATH="${ATK_PARCEL_HOME}/usr/lib/python2.7/site-packages/trustedanalytics/doc/html"