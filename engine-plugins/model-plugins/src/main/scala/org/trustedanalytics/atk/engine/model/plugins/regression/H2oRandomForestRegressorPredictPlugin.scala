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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.h2o._
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

//Implicits for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.h2o.H2oJsonProtocol._

/**
 * Arguments to H2O Random Forest Regression predict plugin
 */
case class H2oRandomForestRegressorPredictArgs(@ArgDoc("""Handle of the model to be used""") model: ModelReference,
                                               @ArgDoc("""A frame whose values are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference,
                                               @ArgDoc("""Column(s) containing the observations whose values are to be predicted.
By default, we predict the labels over columns the Random Forest model
was trained on. """) observationColumns: Option[List[String]] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
}

/** Json conversion for arguments and return value case classes */
object H2oRandomForestRegressorPredictJsonFormat {
  implicit val drfPredictFormat = jsonFormat3(H2oRandomForestRegressorPredictArgs)
}
import H2oRandomForestRegressorPredictJsonFormat._

@PluginDoc(oneLine = "Predict the values for the data points.",
  extended =
    """Predict the values for a test frame using trained H2O Random Forest Regression model, and create a new frame revision with
existing columns and a new predicted value's column.""",
  returns = """A new frame consisting of the existing columns of the frame and
a new column with predicted value for each observation.""")
class H2oRandomForestRegressorPredictPlugin extends SparkCommandPlugin[H2oRandomForestRegressorPredictArgs, FrameReference] {
  //The Python API for this plugin is made available through the H2oRandomForestRegressor Python wrapper class
  //The H2oRandomForestRegressor wrapper has a train() method that calls a local or distributed train
  //depending on the size of the data since H2O's distributed random forest is slow for large trees
  //https://groups.google.com/forum/#!searchin/h2ostream/histogram%7Csort:relevance/h2ostream/bnyhPyxftX8/0d1ItQiyH98J
  override def name: String = "model:h2o_random_forest_regressor_private/predict"

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: H2oRandomForestRegressorPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val h2oModelData = model.readFromStorage().convertTo[H2oModelData]
    if (arguments.observationColumns.isDefined) {
      require(h2oModelData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val obsColumns = arguments.observationColumns.getOrElse(h2oModelData.observationColumns)
    val predictFrame = H2oRandomForestRegressorFunctions.predict(frame.rdd, h2oModelData, obsColumns)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by H2O RandomForests as a regressor predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrame)
    }
  }
}
