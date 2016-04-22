/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

/**
 * Object returned on training a principal components model
 * @param k Handle to the model to be used
 * @param observationColumns List of observation column name(s) used to train the model
 * @param meanCentered Indicator whether the columns were mean centered for training
 * @param columnMeans Means of the columns
 * @param singularValues Singular values of the specified columns in the input frame
 * @param rightSingularMatrix Right singular matrix of the specified columns in the input frame
 */
case class DaalPrincipalComponentsTrainReturn(k: Int,
                                              observationColumns: List[String],
                                              meanCentered: Boolean,
                                              columnMeans: Array[Double],
                                              singularValues: Array[Double],
                                              rightSingularMatrix: Array[Array[Double]]) {

  def this(svdData: DaalSvdData) {
    this(
      svdData.k,
      svdData.observationColumns,
      svdData.meanCentered,
      svdData.meanVector,
      svdData.singularValues,
      svdData.rightSingularMatrix
    )
  }
}