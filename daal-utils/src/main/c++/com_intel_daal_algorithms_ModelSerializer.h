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

#ifndef _Included_com_intel_daal_algorithms_ModelSerializer
#define _Included_com_intel_daal_algorithms_ModelSerializer
#ifdef __cplusplus
extern "C" {
#endif

/** JNI wrapper for serializing DAAL QR models to byte arrays */
JNIEXPORT jobject JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cSerializeQrModel
  (JNIEnv *, jclass, jlong);

/** JNI wrapper for deserializing byte arrays to DAAL QR models */
JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeQrModel
  (JNIEnv *, jclass, jobject, jlong);

/** JNI wrapper for serializing DAAL Naive Bayes models to byte arrays */
JNIEXPORT jobject JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cSerializeNaiveBayesModel
  (JNIEnv *, jclass, jlong);

/** JNI wrapper for deserializing byte arrays to DAAL Naive Bayes models */
JNIEXPORT jlong JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cDeserializeNaiveBayesModel
  (JNIEnv *, jclass, jobject, jlong);

/** JNI wrapper for freeing byte buffer */
JNIEXPORT void JNICALL Java_com_intel_daal_algorithms_ModelSerializer_cFreeByteBuffer
  (JNIEnv *, jclass, jobject);

#ifdef __cplusplus
}
#endif
#endif
