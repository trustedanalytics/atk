/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <jni.h>
#include <stdio.h>
#include "algorithms/algorithm.h"
#include "data_management/data/numeric_table.h"
#include "data_management/data/data_serialize.h"
#include "services/daal_defines.h"
#include "algorithms/linear_regression/linear_regression_model.h"
#include "algorithms/linear_regression/linear_regression_qr_model.h"
#include "com_intel_daal_algorithms_ModelSerializer.h"

using namespace daal;
using namespace daal::services;

/** JNI wrapper for serializing DAAL QR models to byte arrays */
JNIEXPORT jobject JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cSerializeQrModel
  (JNIEnv *env, jclass thisClass, jlong cModel) {

   //get linear regression QR model
   services::SharedPtr<algorithms::linear_regression::ModelQR> *sharedPtr =
    (services::SharedPtr<algorithms::linear_regression::ModelQR>*)cModel;
   algorithms::linear_regression::ModelQR* qrModel = sharedPtr->get();

   //serialize model
   data_management::InputDataArchive *dataArchive = new data_management::InputDataArchive();
   qrModel->serialize(*dataArchive);

   size_t length = dataArchive->getSizeOfArchive();

   byte* buffer = (byte*)daal_malloc(length);
   dataArchive->copyArchiveToArray(buffer, length);

   return env->NewDirectByteBuffer(buffer, length);
}

/** JNI wrapper for deserializing byte arrays to DAAL QR models */
JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeQrModel
  (JNIEnv *env, jclass thisClass, jobject byteBuffer, jlong bufferSize) {
    jbyte* buffer = (jbyte*)env->GetDirectBufferAddress(byteBuffer);

    //Get linear regression QR model
    algorithms::linear_regression::ModelQR *qrModel = new algorithms::linear_regression::ModelQR();

    //deserialize model
    data_management::OutputDataArchive *dataArchive =
        new data_management::OutputDataArchive((daal::byte*)buffer, bufferSize);
    qrModel->deserialize(*dataArchive);

    services::SharedPtr<algorithms::linear_regression::ModelQR> *sharedPtr =
        new services::SharedPtr<algorithms::linear_regression::ModelQR>(qrModel);
    return (jlong)sharedPtr;
}

/** JNI wrapper for serializing DAAL QR models to byte arrays */
JNIEXPORT void JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cFreeByteBuffer
  (JNIEnv *env, jclass thisClass, jobject byteBuffer) {
    daal::byte* buffer = (daal::byte*)env->GetDirectBufferAddress(byteBuffer);
    daal_free(buffer);
}
