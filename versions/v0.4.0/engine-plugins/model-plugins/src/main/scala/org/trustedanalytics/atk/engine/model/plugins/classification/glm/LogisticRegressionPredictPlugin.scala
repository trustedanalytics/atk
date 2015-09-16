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

package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.model.plugins.classification.ClassificationWithSGDPredictArgs
import org.apache.spark.mllib.linalg.Vectors
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Make a new frame with a column for label prediction.",
  extended = """Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted label's column.""",
  returns = "Frame containing the original frame's columns and a column with the predicted label.")
class LogisticRegressionPredictPlugin extends SparkCommandPlugin[ClassificationWithSGDPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ClassificationWithSGDPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Running MLLib
    val logRegData = model.data.convertTo[LogisticRegressionData]
    val logRegModel = logRegData.logRegModel
    if (arguments.observationColumns.isDefined) {
      require(logRegData.observationColumns.length == arguments.observationColumns.get.length,
        "Number of columns for train and predict should be same")
    }
    val logRegColumns = arguments.observationColumns.getOrElse(logRegData.observationColumns)

    //predicting a label for the observation columns
    val predictionsRDD = frame.rdd.mapRows(row => {
      val array = row.valuesAsArray(logRegColumns)
      val doubles = array.map(i => DataTypes.toDouble(i))
      val point = Vectors.dense(doubles)
      val prediction = logRegModel.predict(point)
      row.addValue(prediction.toInt)
    })

    val updatedSchema = frame.schema.addColumn("predicted_label", DataTypes.int32)
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by LogisticRegression predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrameRdd)
    }
  }

}
