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
package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

import org.apache.spark.mllib.linalg.{ Matrix, Vector }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction.PrincipalComponentsData

/**
 * Model data for Intel DAAL Singular Value Decomposition (SVD)
 *
 * @param k Principal component count
 * @param observationColumns Handle to the observation columns of the data frame
 * @param meanCentered Indicator whether the columns were mean centered for training
 * @param meanVector Means of the columns
 * @param singularValues Singular values of the specified columns in the input frame
 * @param vFactor Right singular vectors of the specified columns in the input frame
 * @param leftSingularMatrix Optional RDD with left singular vectors of the specified columns in the input frame
 */
case class DaalSvdData(k: Int,
                       observationColumns: List[String],
                       meanCentered: Boolean,
                       meanVector: Vector,
                       singularValues: Vector,
                       vFactor: Matrix,
                       leftSingularMatrix: Option[RDD[Vector]]) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(k >= 1, "number of Eigen values to use must be greater than equal to 1")
  require(k <= observationColumns.length,
    "k must be less than or equal to number of observation columns")

  /**
   * Convert Intel DAAL SVD data to Principal Components model data
   */
  def toPrincipalComponentsData: PrincipalComponentsData = {
    PrincipalComponentsData(k,
      observationColumns,
      meanCentered,
      meanVector,
      singularValues,
      vFactor)
  }
}

