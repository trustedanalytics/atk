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

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

import spray.json._
import DomainJsonProtocol._
/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringTrainArgs(model: ModelReference,
                                           frame: FrameReference,
                                           @ArgDoc("""source column name.""") sourceColumnName: String,
                                           @ArgDoc("""destination column name.""") destColumnName: String,
                                           @ArgDoc("""weight column name.""") weightColumnName: String,
                                           @ArgDoc("""max number of super-steps (max iterations) before the algorithm terminates. Default = 10""") maxSteps: Int = 10,
                                           @ArgDoc("""float value between 0 .. 1 """) regularization: Float = 0.5f,
                                           @ArgDoc("""double value between 0 .. 1 """) alpha: Double = 0.5f,
                                           @ArgDoc("""number of the desired factors (rank)""") numFactors: Int = 3,
                                           @ArgDoc("""use implicit preference""") useImplicit: Boolean = false,
                                           @ArgDoc("""number of user blocks""") numUserBlocks: Int = 2,
                                           @ArgDoc("""number of item blocks""") numItemBlock: Int = 3,
                                           @ArgDoc("""Number of iterations between checkpoints""") checkpointIterations: Int = 10,
                                           @ArgDoc("""target RMSE""") targetRMSE: Double = 0.05) {

  require(frame != null, "input frame is required")
  require(StringUtils.isNotEmpty(sourceColumnName), "source column name is required")
  require(StringUtils.isNotEmpty(destColumnName), "destination column name is required")
  require(StringUtils.isNotEmpty(weightColumnName), "weight column name is required")
  require(maxSteps > 1, "min steps must be a positive integer")
  require(regularization > 0, "regularization must be a positive value")
  require(alpha > 0, "alpha must be a positive value")
  require(checkpointIterations > 0 or checkpointIterations == -1, "Iterations between checkpoints must be either positive or -1")
  require(numFactors > 0, "number of factors must be a positive integer")
  require(numUserBlocks > 0, "number of user blocks must be a positive integer")
  require(numItemBlock > 0, "number of item blocks must be a positive integer")
  require(targetRMSE > 0, "target RMSE must be a positive value")

}
