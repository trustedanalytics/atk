/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import com.intel.daal.algorithms.ModelSerializer
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, SparkCommandPlugin, ApiMaturityTag, Invocation }

//Implicits needed for JSON conversion
import spray.json._
import DaalLinearRegressionModelFormat._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

import DaalLinearRegressionJsonFormat._

/**
 * Plugin for scoring DAAL's Linear Regression using QR decomposition
 */
@PluginDoc(oneLine = "Predict labels for a test frame using trained DAAL linear regression model.",
  extended = """Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted value column.""",
  returns =
    """frame\:
  Frame containing the original frame's columns and a column with the predicted value.""")
class DaalLinearRegressionPredictPlugin extends SparkCommandPlugin[DaalLinearRegressionArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /** Disable Kryo serialization to prevent seg-faults when using DAAL */
  override def kryoRegistrator: Option[String] = None

  /**
   * Get predictions for DAAL's Linear Regression with QR decomposition using test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalLinearRegressionArgs)(implicit invocation: Invocation): FrameReference =
    {
      DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
      val model: Model = arguments.model

      //create RDD from the frame
      val testFrame: SparkFrame = arguments.frame
      val featureColumns = arguments.featureColumns
      val labelColumns = arguments.labelColumns

      //Load the DAAL linear regression model
      val lrJsObject = model.data
      val modelData = lrJsObject.convertTo[DaalLinearRegressionModel]

      require(modelData.featureColumns.length == arguments.featureColumns.length,
        "Number of feature columns for train and predict should be same")
      require(modelData.labelColumns.length == arguments.labelColumns.length,
        "Number of label columns for train and predict should be same")

      val lrResultsFrameRdd = DaalLinearRegressionFunctions.predictLinearModel(
        modelData,
        testFrame.rdd,
        featureColumns)

      engine.frames.tryNewFrame(CreateEntityArgs(
        description = Some("created by DAAL linear regression predict operation"))) {
        newPredictedFrame: FrameEntity =>
          newPredictedFrame.save(lrResultsFrameRdd)
      }
    }

}
