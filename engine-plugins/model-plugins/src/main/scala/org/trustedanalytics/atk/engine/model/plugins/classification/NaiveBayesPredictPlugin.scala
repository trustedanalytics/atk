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

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vectors

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class NaiveBayesPredictArgs(model: ModelReference,
                                 @ArgDoc("""A frame whose labels are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                 @ArgDoc("""Column(s) containing the
observations whose labels are to be predicted.
By default, we predict the labels over columns the NaiveBayesModel
was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")

}

@PluginDoc(oneLine = "Predict labels for data points using trained Naive Bayes model.",
  extended = """Predict the labels for a test frame using trained Naive Bayes model,
      and create a new frame revision with existing columns and a new predicted label's column.""",
  returns = "Frame containing the original frame's columns and a column with the predicted label.")
class NaiveBayesPredictPlugin extends SparkCommandPlugin[NaiveBayesPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: NaiveBayesPredictArgs)(implicit invocation: Invocation) = 9

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: NaiveBayesPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    // Loading model
    val naiveBayesJsObject = model.dataOption.getOrElse(
      throw new RuntimeException("This model has not be trained yet. Please train before trying to predict")
    )
    val naiveBayesData = naiveBayesJsObject.convertTo[NaiveBayesData]
    val naiveBayesModel = naiveBayesData.naiveBayesModel
    if (arguments.observationColumns.isDefined) {
      require(naiveBayesData.observationColumns.length == arguments.observationColumns.get.length,
        "Number of columns for train and predict should be same")
    }

    //predicting a label for the observation columns
    val naiveBayesColumns = arguments.observationColumns.getOrElse(naiveBayesData.observationColumns)
    val predictColumn = Column("predicted_class", DataTypes.float64)
    val predictFrame = frame.rdd.addColumn(predictColumn, row => {
      val point = row.valuesAsDenseVector(naiveBayesColumns)
      naiveBayesModel.predict(point)
    })

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by NaiveBayes predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrame)
    }
  }

}
