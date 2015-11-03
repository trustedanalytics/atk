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

package org.trustedanalytics.atk.giraph.config.lp

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.apache.commons.lang3.StringUtils

/**
 * Arguments to the plugin - see user docs for more on the parameters
 */
case class LabelPropagationArgs(frame: FrameReference,
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
                                @ArgDoc("""The maximum number of supersteps
that the algorithm will execute.
The valid value range is all positive int.
Default is 10.""") maxIterations: Option[Int] = None,
                                @ArgDoc("""The amount of change in
cost function that will be tolerated at convergence.
If the change is less than this threshold, the algorithm exits earlier
before it reaches the maximum number of supersteps.
The valid value range is all float and zero.
Default is 0.00000001f.""") convergenceThreshold: Option[Float] = None,
                                @ArgDoc("""The tradeoff parameter that
controls how much influence an external
classifier's prediction contributes to the final prediction.
This is for the case where an external classifier is available that can
produce initial probabilistic classification on unlabeled examples, and
the option allows incorporating external classifier's prediction into
the LP training process.
The valid value range is [0.0,1.0].
Default is 0.""") alpha: Option[Float] = None) {

  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(srcColName), "source column name property list is required")
  require(StringUtils.isNotBlank(destColName), "destination column name property list is required")
  require(srcColName != destColName, "source and destination column names cannot be the same")
  require(StringUtils.isNotBlank(weightColName), "edge weight property list is required")
  require(StringUtils.isNotBlank(srcLabelColName), "source label column name property list is required")

  def getResultsColName: String = {
    resultColName.getOrElse("resultLabels")
  }

  def getMaxIterations: Int = {
    val value = maxIterations.getOrElse(10)
    if (value < 1) 10 else value
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.00000001f)
  }

  def getLambda: Float = {
    val value = alpha.getOrElse(0.9999999f)
    1 - Math.min(1, Math.max(0, value))
  }
}

case class LabelPropagationResult(outputFrame: FrameEntity, report: String) {
  require(outputFrame != null, "label results are required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/** Json conversion for arguments and return value case classes */
object LabelPropagationJsonFormat {

  implicit val argsFormat = jsonFormat9(LabelPropagationArgs)
  implicit val resultFormat = jsonFormat2(LabelPropagationResult)
}
