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
#include "algorithms/naive_bayes/multinomial_naive_bayes_model.h"
#include "com_intel_daal_algorithms_ModelSerializer.h"

using namespace daal;

/** JNI wrapper for serializing DAAL QR models to byte arrays */
JNIEXPORT jobject JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cSerializeQrModel
  (JNIEnv *env, jclass thisClass, jlong cModel) {

   services::SharedPtr<data_management::NumericTable> dataTable;
   services::SharedPtr<algorithms::linear_regression::ModelQR> *sharedPtr = (services::SharedPtr<algorithms::linear_regression::ModelQR>*)cModel;

   algorithms::linear_regression::ModelQR* qrModel = sharedPtr->get();
   data_management::InputDataArchive *dataArchive = new data_management::InputDataArchive();
   qrModel->serialize(*dataArchive);
   size_t size = dataArchive->getSizeOfArchive();
   byte* byteArray = new daal::byte[size];
   dataArchive->copyArchiveToArray(byteArray, size);

   jobject directBuffer = env->NewDirectByteBuffer(byteArray, size);
   jobject globalRef = env->NewGlobalRef(directBuffer);
   delete dataArchive;
   return globalRef;
}

/** JNI wrapper for deserializing byte arrays to DAAL QR models */
JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeQrModel
  (JNIEnv *env, jclass thisClass, jobject buffer, jlong bufferSize) {
    jbyte* bufferPtr = (jbyte*)env->GetDirectBufferAddress(buffer);

    algorithms::linear_regression::ModelQR *qrModel = new algorithms::linear_regression::ModelQR();
    data_management::OutputDataArchive *dataArchive = new data_management::OutputDataArchive((daal::byte*)bufferPtr, bufferSize);
    qrModel->deserialize(*dataArchive);

    services::SharedPtr<algorithms::linear_regression::ModelQR> *sharedPtr = new services::SharedPtr<algorithms::linear_regression::ModelQR>(qrModel);
    return (jlong)sharedPtr;
}

/** JNI wrapper for serializing DAAL Naive Bayes models to byte arrays */
JNIEXPORT jobject JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cSerializeNaiveBayesModel
  (JNIEnv *env, jclass thisClass, jlong cModel) {

   services::SharedPtr<data_management::NumericTable> dataTable;
   services::SharedPtr<algorithms::multinomial_naive_bayes::Model> *sharedPtr = (services::SharedPtr<algorithms::multinomial_naive_bayes::Model>*)cModel;

   algorithms::multinomial_naive_bayes::Model* nbModel = sharedPtr->get();
   data_management::InputDataArchive *dataArchive = new data_management::InputDataArchive();
   nbModel->serialize(*dataArchive);
   size_t size = dataArchive->getSizeOfArchive();
   byte* byteArray = new daal::byte[size];
   dataArchive->copyArchiveToArray(byteArray, size);

   jobject directBuffer = env->NewDirectByteBuffer(byteArray, size);
   jobject globalRef = env->NewGlobalRef(directBuffer);
   delete dataArchive;
   return globalRef;
}

/** JNI wrapper for deserializing byte arrays to DAAL Naive Bayes models */
JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeNaiveBayesModel
  (JNIEnv *env, jclass thisClass, jobject buffer, jlong bufferSize) {
    jbyte* bufferPtr = (jbyte*)env->GetDirectBufferAddress(buffer);

    algorithms::multinomial_naive_bayes::Model *nbModel = new algorithms::multinomial_naive_bayes::Model();
    data_management::OutputDataArchive *dataArchive = new data_management::OutputDataArchive((daal::byte*)bufferPtr, bufferSize);
    nbModel->deserialize(*dataArchive);

    services::SharedPtr<algorithms::multinomial_naive_bayes::Model> *sharedPtr = new services::SharedPtr<algorithms::multinomial_naive_bayes::Model>(nbModel);
    return (jlong)sharedPtr;
}

/** JNI wrapper for freeing byte buffer */
JNIEXPORT void JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cFreeByteBuffer
  (JNIEnv *env, jclass thisClass, jobject buffer) {
    daal::byte* byteArray = (daal::byte*)env->GetDirectBufferAddress(buffer);
    env->DeleteGlobalRef(buffer);
    delete byteArray;
}
