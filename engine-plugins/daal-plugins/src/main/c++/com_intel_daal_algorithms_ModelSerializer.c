/*
//  Copyright(C) 2014-2015 Intel Corporation. All Rights Reserved.
//
//  The source code, information  and  material ("Material") contained herein is
//  owned  by Intel Corporation or its suppliers or licensors, and title to such
//  Material remains  with Intel Corporation  or its suppliers or licensors. The
//  Material  contains proprietary information  of  Intel or  its  suppliers and
//  licensors. The  Material is protected by worldwide copyright laws and treaty
//  provisions. No  part  of  the  Material  may  be  used,  copied, reproduced,
//  modified, published, uploaded, posted, transmitted, distributed or disclosed
//  in any way  without Intel's  prior  express written  permission. No  license
//  under  any patent, copyright  or  other intellectual property rights  in the
//  Material  is  granted  to  or  conferred  upon  you,  either  expressly,  by
//  implication, inducement,  estoppel or  otherwise.  Any  license  under  such
//  intellectual  property  rights must  be express  and  approved  by  Intel in
//  writing.
//
//  *Third Party trademarks are the property of their respective owners.
//
//  Unless otherwise  agreed  by Intel  in writing, you may not remove  or alter
//  this  notice or  any other notice embedded  in Materials by Intel or Intel's
//  suppliers or licensors in any way.
*/

#include <jni.h>
#include "algorithms/algorithm.h"
#include "data_management/data/numeric_table.h"
#include "data_management/data/data_serialize.h"
#include "services/daal_defines.h"
#include "algorithms/linear_regression/linear_regression_model.h"
#include "algorithms/linear_regression/linear_regression_qr_model.h"

using namespace daal;

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

JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeQrModel
  (JNIEnv *env, jclass thisClass, jobject buffer, jlong bufferSize) {
    jbyte* bufferPtr = (jbyte*)env->GetDirectBufferAddress(buffer);

    algorithms::linear_regression::ModelQR *qrModel = new algorithms::linear_regression::ModelQR();//numFeatures, numResponses, parameter);
    data_management::OutputDataArchive *dataArchive = new data_management::OutputDataArchive((daal::byte*)bufferPtr, bufferSize);
    qrModel->deserialize(*dataArchive);

    services::SharedPtr<data_management::NumericTable> betasTest2 =  qrModel->getBeta();
    //qrModel->printNumericTable(betasTest2.get(), "serialized betas");
    //qrModel->printNumericTable(qrModel->getRTable(), "serialized rtable");
    services::SharedPtr<algorithms::linear_regression::ModelQR> *sharedPtr = new services::SharedPtr<algorithms::linear_regression::ModelQR>(qrModel);

    services::SharedPtr<data_management::NumericTable> betasTest =  sharedPtr->get()->getBeta();
    return (jlong)sharedPtr;
}

JNIEXPORT void JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cFreeByteBuffer
  (JNIEnv *env, jclass thisClass, jobject buffer) {
    daal::byte* byteArray = (daal::byte*)env->GetDirectBufferAddress(buffer);
    env->DeleteGlobalRef(buffer);
    delete byteArray;
}
