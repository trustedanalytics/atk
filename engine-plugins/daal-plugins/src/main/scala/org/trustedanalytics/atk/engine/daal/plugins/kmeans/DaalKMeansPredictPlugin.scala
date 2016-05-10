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

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalKMeansJsonFormat._

@PluginDoc(oneLine = "Predict the cluster assignments for the data points.",
  extended = "Predicts the clusters for each data point and distance to every cluster center of the frame using the trained model",
  returns = """Frame
    A new frame consisting of the existing columns of the frame and the following new columns:
    'k' columns : Each of the 'k' columns containing squared distance of that observation to the 'k'th cluster center
    predicted_cluster column: The cluster assignment for the observation""")
class DaalKMeansPredictPlugin extends SparkCommandPlugin[DaalKMeansPredictArgs, FrameReference] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_k_means/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalKMeansPredictArgs)(implicit invocation: Invocation): FrameReference = {
    DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Extracting the KMeansModel from the stored JsObject
    val modelData = model.data.convertTo[DaalKMeansModelData]
    if (arguments.observationColumns.isDefined) {
      require(modelData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val assignmentFrame: FrameRdd = DaalKMeansFunctions.predictKMeansModel(arguments, frame.rdd, modelData)
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by DAAL kmeans predict operation"))) {
      frameEntity: FrameEntity =>
        frameEntity.save(assignmentFrame)
    }
  }
}
