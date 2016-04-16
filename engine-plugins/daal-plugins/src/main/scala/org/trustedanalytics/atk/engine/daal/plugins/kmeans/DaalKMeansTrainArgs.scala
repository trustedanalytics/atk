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

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc
import com.intel.daal.algorithms.kmeans.init.InitMethod

/**
 * Command for loading model data into existing model in the model database.
 */
case class DaalKMeansTrainArgs(model: ModelReference,
                               @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                               @ArgDoc("""Columns containing the
observations.""") observationColumns: List[String],
                               @ArgDoc("""Desired number of clusters.
Default is 2.""") k: Int = 2,
                               @ArgDoc("""Number of iterations for which the algorithm should run.
Default is 20.""") maxIterations: Int = 20,
                               @ArgDoc("""Optional column scalings for each of the observation columns.
The scaling value is multiplied by the corresponding value in the
observation column.""") columnScalings: Option[List[Double]] = None,
                               @ArgDoc("""Optional name of output column with
index of cluster each observation belongs to.""") labelColumn: String = "predicted_cluster",
                               @ArgDoc("""Optional initialization mode for cluster centroids.
random - Random choice of k feature vectors from the data set.
deterministic - Choice of first k feature vectors from the data set.""") initializationMode: String = "random") {

  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "observation columns must not be null nor empty")
  require(columnScalings != null || columnScalings.isEmpty ||
    observationColumns.length == columnScalings.get.length,
    "column scalings must be empty or the same size as observation columns")
  require(labelColumn != null && labelColumn.nonEmpty, "label column must not be null nor empty")
  require(k > 0, "k must be at least 1")
  require(maxIterations > 0, "max iterations must be a positive value")
  require(initializationMode == "random" || initializationMode == "deterministic",
    "initialization mode must be 'random' or 'deterministic'")

  /**
   * Get centroid initialization method
   *
   * random - Random choice of k feature vectors from the data set
   * deterministic - Choice of first k feature vectors from the data set
   *
   * @return centroid initialization method
   */
  def getInitMethod: InitMethod = initializationMode match {
    case "deterministic" => InitMethod.deterministicDense
    case _ => InitMethod.randomDense
  }
}
