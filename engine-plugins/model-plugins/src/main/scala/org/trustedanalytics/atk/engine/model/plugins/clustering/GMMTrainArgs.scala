/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import scala.Long
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }
import spray.json.DefaultJsonProtocol
/**
 * Command for loading model data into existing model in the model database.
 */
case class GMMTrainArgs(model: ModelReference,
                        @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                        @ArgDoc("""Columns containing the observations.""") observationColumns: List[String],
                        @ArgDoc("""Column scalings for each of the observation columns. The scaling value is multiplied by the corresponding value in the
observation column.""") columnScalings: List[Double],
                        @ArgDoc("""Desired number of clusters. Default is 2.""") k: Int = 2,
                        @ArgDoc("""Number of iterations for which the algorithm should run. Default is 100.""") maxIterations: Int = 100,
                        @ArgDoc("""Largest change in log-likelihood at which convergence iis considered to have occurred.""") convergenceTol: Double = 0.01,
                        @ArgDoc("""Random seed""") seed: Long = scala.util.Random.nextLong()) {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(columnScalings != null && columnScalings.nonEmpty, "columnWeights must not be null or empty")
  require(columnScalings.length == observationColumns.length, "Length of columnWeights and observationColumns needs to be the same")
  require(k > 0, "k must be at least 1")
  require(maxIterations > 0, "maxIterations must be a positive value")

}

