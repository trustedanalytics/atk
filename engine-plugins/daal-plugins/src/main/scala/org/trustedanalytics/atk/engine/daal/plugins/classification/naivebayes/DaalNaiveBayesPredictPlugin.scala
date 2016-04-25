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

package org.trustedanalytics.atk.engine.daal.plugins.classification.naivebayes

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalNaiveBayesModelFormat._
import DaalNaiveBayesArgsFormat._

@PluginDoc(oneLine = "Predict labels for data points using trained Intel DAAL Naive Bayes model.",
  extended = """Predict the labels for a test frame using trained Intel DAAL Naive Bayes model,
      and create a new frame revision with existing columns and a new predicted label's column.""",
  returns = "Frame containing the original frame's columns and a column with the predicted label.")
class DaalNaiveBayesPredictPlugin extends SparkCommandPlugin[DaalNaiveBayesPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_naive_bayes/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalNaiveBayesPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    // Loading model
    val naiveBayesJsObject = model.dataOption.getOrElse(
      throw new RuntimeException("This model has not been trained yet. Please train before trying to predict")
    )
    val naiveBayesModel = naiveBayesJsObject.convertTo[DaalNaiveBayesModelData]
    if (arguments.observationColumns.isDefined) {
      require(naiveBayesModel.observationColumns.length == arguments.observationColumns.get.length,
        "Number of columns for train and predict should be same")
    }

    //predicting a label for the observation columns
    val naiveBayesColumns = arguments.observationColumns.getOrElse(naiveBayesModel.observationColumns)
    val predictColumn = "predicted_class"
    val predictFrame = new DaalNaiveBayesPredictAlgorithm(naiveBayesModel, frame.rdd,
      naiveBayesColumns, predictColumn).predict()

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by Intel DAAL NaiveBayes predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrame)
    }
  }

}
