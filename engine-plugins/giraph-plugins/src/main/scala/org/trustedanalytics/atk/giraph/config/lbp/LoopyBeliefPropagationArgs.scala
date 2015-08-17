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

package org.trustedanalytics.atk.giraph.config.lbp

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class LoopyBeliefPropagationArgs(frame: FrameReference,
                                      @ArgDoc("""The column name for the
source vertex id.""") srcColName: String,
                                      @ArgDoc("""The column name for the
destination vertex id.""") destColName: String,
                                      @ArgDoc("""The column name for the
edge weight.""") weightColName: String,
                                      @ArgDoc("""The column name for the
label properties for the source vertex.""") srcLabelColName: String,
                                      @ArgDoc("""The column name for the
results (holding the post labels for the vertices).""") resultColName: Option[String] = None,
                                      @ArgDoc("""If True, all vertex will be treated as training data.
Default is False.""") ignoreVertexType: Option[Boolean] = None,
                                      @ArgDoc("""The maximum number of
supersteps that the algorithm will execute.
The valid value range is all positive int.
The default value is 10.""") maxIterations: Option[Int] = None,
                                      @ArgDoc("""The amount of change in cost
function that will be tolerated at convergence.
If the change is less than this threshold, the algorithm exits earlier
before it reaches the maximum number of supersteps.
The valid value range is all float and zero.
The default value is 0.00000001f.""") convergenceThreshold: Option[Float] = None,
                                      @ArgDoc("""The parameter that determines
if a node's posterior will be updated or not.
If a node's maximum prior value is greater than this threshold, the node
will be treated as anchor node, whose posterior will inherit from prior
without update.
This is for the case where we have confident prior estimation for some
nodes and don't want the algorithm updates these nodes.
The valid value range is in [0, 1].
Default is 1.0.""") anchorThreshold: Option[Double] = None,
                                      @ArgDoc("""The Ising smoothing parameter.
This parameter adjusts the relative strength of closeness encoded edge
weights, similar to the width of Gussian distribution.
Larger value implies smoother decay and the edge weight beomes less
important.
Default is 2.0.""") smoothing: Option[Float] = None,
                                      @ArgDoc("""Should LBP use max_product or not.
Default is False.""") maxProduct: Option[Boolean] = None,
                                      @ArgDoc("""Power coefficient for power edge potential.
Default is 0.""") power: Option[Float] = None) {
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(srcColName), "source column name property list is required")
  require(StringUtils.isNotBlank(destColName), "destination column name property list is required")
  require(srcColName != destColName, "source and destination column names cannot be the same")
  require(StringUtils.isNotBlank(weightColName), "edge weight property list is required")
  require(StringUtils.isNotBlank(srcLabelColName), "source label column name property list is required")

  def getResultsColName: String = {
    resultColName.getOrElse("resultLabels")
  }

  def getIgnoreVertexType: Boolean = {
    ignoreVertexType.getOrElse(true)
  }

  def getMaxIterations: Int = {
    val value = maxIterations.getOrElse(10)
    if (value < 1) 10 else value
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.00000001f)
  }

  def getAnchorThreshold: Double = {
    val value = anchorThreshold.getOrElse(1d)
    if (value < 0d) 1d else value
  }

  def getSmoothing: Float = {
    val value = smoothing.getOrElse(2f)
    if (value < 0f) 2f else value
  }

  def getMaxProduct: Boolean = {
    maxProduct.getOrElse(false)
  }

  def getPower: Float = {
    val value = power.getOrElse(0f)
    if (value < 0f) 0f else value
  }
}

case class LoopyBeliefPropagationResult(outputFrame: FrameEntity, report: String) {
  require(outputFrame != null, "label results are required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object LoopyBeliefPropagationJsonFormat {

  implicit val argsFormat = jsonFormat13(LoopyBeliefPropagationArgs)
  implicit val resultFormat = jsonFormat2(LoopyBeliefPropagationResult)
}
