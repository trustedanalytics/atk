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


package org.trustedanalytics.atk.engine.model.plugins.libsvm

import libsvm.{ svm_node, svm_parameter, svm_model }

/**
 * Command for loading model data into existing model in the model database.
 * @param svmModel Trained libSVMModel object
 * @param observationColumns Handle to the observation columns of the data frame
 */
case class LibSvmData(svmModel: svm_model, observationColumns: List[String]) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(svmModel != null, "libsvmModel must not be null")
}
