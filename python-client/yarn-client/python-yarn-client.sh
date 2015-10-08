#!/bin/sh

workDir=$(pwd)
TARGET_DIR=${workDir}/target
mkdir -p ${TARGET_DIR}

log "regular package"

PYTHON_CLIENT_ROOT=${workDir}
TRUSTEDANALYTICS_ROOT=${TARGET_DIR}/trustedanalytics
CORE=${TRUSTEDANALYTICS_ROOT}/core
REST=${TRUSTEDANALYTICS_ROOT}/rest
mkdir -p ${TRUSTEDANALYTICS_ROOT}
mkdir -p ${CORE}
mkdir -p ${REST}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/core/__init__.py ${CORE}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/core/row.py ${CORE}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/core/atktypes.py ${CORE}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/rest/__init__.py ${REST}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/rest/spark.py ${REST}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/rest/serializers.py ${REST}
cp -v ${PYTHON_CLIENT_ROOT}/trustedanalytics/rest/cloudpickle.py ${REST}
cp -v ${workDir}/yarn-client/assets/__init__.py ${TRUSTEDANALYTICS_ROOT}

pushd ${TARGET_DIR}
zip -r trustedanalytics.zip trustedanalytics
rm -rf trustedanalytics
popd
