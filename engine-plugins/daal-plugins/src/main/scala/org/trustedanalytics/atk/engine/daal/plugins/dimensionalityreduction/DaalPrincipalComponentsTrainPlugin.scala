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

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Schema }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction.{ PrincipalComponentsData, PrincipalComponentsTrainReturn, PrincipalComponentsTrainArgs }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Build Intel DAAL principal components model.",
  extended = """Creating a PrincipalComponents Model using the observation columns.""",
  returns =
    """dictionary
    |A dictionary with trained Principal Components Model with the following keys\:
    |'column_means': the list of the means of each observation column
    |'k': number of principal components used to train the model
    |'mean_centered': Flag indicating if the model was mean centered during training
    |'observation_columns': the list of observation columns on which the model was trained,
    |'eigen_vectors': list of a list storing the eigen vectors of the specified columns of the input frame
    |'eigen_values': list storing the eigen values of the specified columns of the input frame
  """)
class DaalPrincipalComponentsTrainPlugin extends SparkCommandPlugin[PrincipalComponentsTrainArgs, PrincipalComponentsTrainReturn] {

  /**
   * The name of the command
   */
  override def name: String = "model:daal_principal_components/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

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
    frame.schema.requireColumnsAreVectorizable(observationColumns)

    val k = arguments.k.getOrElse(arguments.observationColumns.length)
    val svdData = DaalSvdAlgorithm(frame.rdd, arguments).compute(k, computeU = false)

    val principalComponentsObject = svdData.toPrincipalComponentsData
    model.data = principalComponentsObject.toJson.asJsObject
    new PrincipalComponentsTrainReturn(principalComponentsObject)
  }

}
