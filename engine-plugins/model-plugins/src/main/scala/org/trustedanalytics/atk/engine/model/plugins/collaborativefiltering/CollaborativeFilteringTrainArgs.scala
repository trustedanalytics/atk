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

package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

import spray.json._
import DomainJsonProtocol._
/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringTrainArgs(model: ModelReference,
                                           graph: GraphReference,
                                           @ArgDoc("""max number of super-steps (max iterations) before the algorithm terminates. Default = 10""") maxSteps: Int = 10,
                                           @ArgDoc("""the name of the column containing the propagated label value.""") outputUserVertexPropertyName: String = "preferenceVector",
                                           @ArgDoc("""the name of the column containing the propagated label value.""") outputItemVertexPropertyName: String = "featureVector",
                                           @ArgDoc("""float value between 0 .. 1 """) regularization: Float = 0f,
                                           @ArgDoc("""double value between 0 .. 1 """) alpha: Double = 0.5f,
                                           @ArgDoc("""number of the desired factors (rank)""") numFactors: Int = 3,
                                           @ArgDoc("""use implicit preference""") useImplicit: Boolean = false,
                                           @ArgDoc("""number of user blocks""") numUserBlocks: Int = 2,
                                           @ArgDoc("""number of item blocks""") numItemBlock: Int = 3,
                                           @ArgDoc("""target RMSE""") targetRMSE: Double = 0.05) {

  require(graph != null, "input graph is required")
  require(maxSteps > 1, "min steps must be a positive integer")
  require(regularization > 0, "regularization must be a positive value")
  require(alpha > 0, "regularization must be a positive value")
  require(numFactors > 0, "number of factors must be a positive integer")
  require(numUserBlocks > 0, "number of user blocks must be a positive integer")
  require(numItemBlock > 0, "number of item blocks must be a positive integer")
  require(targetRMSE > 0, "target RMSE must be a positive value")

}
