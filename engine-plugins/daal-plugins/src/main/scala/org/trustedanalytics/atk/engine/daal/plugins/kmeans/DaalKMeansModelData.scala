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
package org.trustedanalytics.atk.engine.daal.plugins.kmeans

/**
 * DAAL KMeans model data
 *
 * @param observationColumns List of columns containing observations
 * @param labelColumn Column with index of cluster each observation belongs to
 * @param centroids Cluster centroids
 * @param k Number of clusters
 * @param columnScalings Optional column scalings for each of the observation columns
 */
case class DaalKMeansModelData(observationColumns: List[String],
                               labelColumn: String,
                               centroids: Array[Array[Double]],
                               k: Int,
                               columnScalings: Option[List[Double]] = None) {
  require(observationColumns != null && observationColumns.nonEmpty, "observation columns must not be null nor empty")
  require(columnScalings != null || columnScalings.isEmpty ||
    observationColumns.length == columnScalings.get.length,
    "column scalings must be empty or the same size as observation columns")
  require(labelColumn != null && labelColumn.nonEmpty, "label column must not be null nor empty")
  require(k > 0, "k must be at least 1")
}