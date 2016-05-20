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
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Build principal components model.",
  extended = """Creating a PrincipalComponents Model using the observation columns.""",
  returns =
    """dictionary
    |A dictionary with trained Principal Components Model with the following keys\:
    |'column_means': the list of the means of each observation column
    |'k': number of principal components used to train the model
    |'mean_centered': Flag indicating if the model was mean centered during training
    |'observation_columns': the list of observation columns on which the model was trained,
    |'right_singular_vectors': list of a list storing the right singular vectors of the specified columns of the input frame
    |'singular_values': list storing the singular values of the specified columns of the input frame
  """)
class PrincipalComponentsTrainPlugin extends SparkCommandPlugin[PrincipalComponentsTrainArgs, PrincipalComponentsTrainReturn] {

  /**
   * The name of the command
   */
  override def name: String = "model:principal_components/train"

  /**
   * Calculate principal components for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification
   * @return value of type declared as the Return type
   */
  override def execute(arguments: PrincipalComponentsTrainArgs)(implicit invocation: Invocation): PrincipalComponentsTrainReturn = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val observationColumns = arguments.observationColumns
    val meanCentered = arguments.meanCentered
    frame.schema.requireColumnsAreVectorizable(observationColumns)

    val k = arguments.k.getOrElse(observationColumns.length)

    val rowMatrix = PrincipalComponentsFunctions.toRowMatrix(frame.rdd, observationColumns, meanCentered)
    val svd = rowMatrix.computeSVD(k, computeU = false)

    val columnStatistics = frame.rdd.columnStatistics(observationColumns)
    val principalComponentsObject = new PrincipalComponentsData(k, observationColumns,
      meanCentered, columnStatistics.mean, svd.s, svd.V)
    model.data = principalComponentsObject.toJson.asJsObject

    new PrincipalComponentsTrainReturn(principalComponentsObject)
  }
}
