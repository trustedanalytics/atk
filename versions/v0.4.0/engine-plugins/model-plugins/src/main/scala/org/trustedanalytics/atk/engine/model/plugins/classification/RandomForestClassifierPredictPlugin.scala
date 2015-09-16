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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, Naming }
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class RandomForestClassifierPredictArgs(@ArgDoc("""Handle of the model to be used""") model: ModelReference,
                                             @ArgDoc("""A frame whose labels are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                             @ArgDoc("""Column(s) containing the observations whose labels are to be predicted.
By default, we predict the labels over columns the RandomForestModel
was trained on. """) observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}

@PluginDoc(oneLine = "Predict the labels for the data points.",
  extended = "",
  returns = """Frame
    A new frame consisting of the existing columns of the frame and a new column with predicted label for each observation.""")
class RandomForestClassifierPredictPlugin extends SparkCommandPlugin[RandomForestClassifierPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_classifier/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: RandomForestClassifierPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RandomForestClassifierPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    //Running MLLib
    val rfData = model.data.convertTo[RandomForestClassifierData]
    val rfModel = rfData.randomForestModel
    if (arguments.observationColumns.isDefined) {
      require(rfData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val rfColumns = arguments.observationColumns.getOrElse(rfData.observationColumns)

    //predicting a label for the observation columns
    val predictionsRDD = frame.rdd.mapRows(row => {
      val array = row.valuesAsArray(rfColumns)
      val doubles = array.map(i => DataTypes.toDouble(i))
      val point = Vectors.dense(doubles)
      val prediction = rfModel.predict(point)
      row.addValue(prediction.toInt)
    })

    val updatedSchema = frame.schema.addColumn("predicted_class", DataTypes.int32)
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by RandomForests as a classifier predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrameRdd)
    }
  }

}
