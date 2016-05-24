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

package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Predict using principal components model.",
  extended = """Predicting on a dataframe's columns using a PrincipalComponents Model.""",
  returns =
    """A frame with existing columns and following additional columns\:
      'c' additional columns: containing the projections of V on the the frame
      't_squared_index': column storing the t-square-index value, if requested""")
class PrincipalComponentsPredictPlugin extends SparkCommandPlugin[PrincipalComponentsPredictArgs, FrameEntity] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:principal_components/predict"

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: PrincipalComponentsPredictArgs)(implicit invocation: Invocation): FrameEntity = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    val principalComponentJsObject = model.dataOption.getOrElse(
      throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val principalComponentData = principalComponentJsObject.convertTo[PrincipalComponentsData]

    // Validate arguments
    PrincipalComponentsFunctions.validateInputArguments(arguments, principalComponentData)
    val c = arguments.c.getOrElse(principalComponentData.k)
    val predictColumns = arguments.observationColumns.getOrElse(principalComponentData.observationColumns)

    // Predict principal components and optional T-squared index
    val resultFrame = PrincipalComponentsFunctions.predictPrincipalComponents(frame.rdd,
      principalComponentData, predictColumns, c, arguments.meanCentered, arguments.tSquaredIndex)

    val resultFrameEntity = engine.frames.tryNewFrame(
      CreateEntityArgs(name = arguments.name, description = Some("created from principal components predict"))) {
        newFrame => newFrame.save(resultFrame)
      }

    resultFrameEntity
  }
}
