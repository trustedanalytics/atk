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

import hex.tree.drf.DRFModel
import org.apache.spark.frame.FrameRdd
import org.apache.spark.h2o._
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.sql.SQLContext
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.scoring.models.RandomForestRegressorData
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.h2o.H2oJsonProtocol._
import water.H2O

case class H2oRandomForestRegressorPredictArgs(@ArgDoc("""Handle of the model to be used""") model: ModelReference,
                                               @ArgDoc("""A frame whose labels are to be predicted.
By default, predict is run on the same columns over which the model is
trained.""") frame: FrameReference) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
}

/** Json conversion for arguments and return value case classes */
object H2oRandomForestRegressorPredictJsonFormat {
  implicit val drfPredictFormat = jsonFormat2(H2oRandomForestRegressorPredictArgs)
}
import H2oRandomForestRegressorPredictJsonFormat._

@PluginDoc(oneLine = "Predict the values for the data points.",
  extended =
    """Predict the values for a test frame using trained Random Forest Classifier model, and create a new frame revision with
existing columns and a new predicted value's column.""",
  returns = """A new frame consisting of the existing columns of the frame and
a new column with predicted value for each observation.""")
class H2oRandomForestRegressorPredictPlugin extends SparkCommandPlugin[H2oRandomForestRegressorPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:h2o_random_forest_regressor/predict"

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

    implicit val sqlContext = SQLContext.getOrCreate(sc)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext._
    import h2oContext.implicits._

    val drfModel = model.readFromStorage().convertTo[DRFModel]
    val h2oFrame: H2OFrame = h2oContext.asH2OFrame(frame.rdd.toDataFrame)
    val h2oPredictFrame = drfModel.score(h2oFrame)
    val predictFrame = FrameRdd.toFrameRdd(h2oContext.asDataFrame(h2oPredictFrame))

    H2O.orderlyShutdown()
    H2O.closeAll()
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by H2O RandomForests as a regressor predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrame)
    }
  }

}
