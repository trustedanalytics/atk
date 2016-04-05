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

package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalLinearRegressionModelFormat._
import DaalLinearRegressionJsonFormat._

/**
 * Plugin for computing test metrics for DAAL Linear Regression using QR decomposition
 */
@PluginDoc(oneLine = "Compute test metrics for trained Intel DAAL linear regression model.",
  extended = """Predict the labels for a test frame, and compute test metrics for trained model.""",
  returns =
    """Test metrics for Intel DAAL linear regression model""")
class DaalLinearRegressionTestPlugin extends SparkCommandPlugin[DaalLinearRegressionTestArgs, DaalLinearRegressionTestReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/test"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Get test metrics for DAAL's Linear Regression with QR decomposition using test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalLinearRegressionTestArgs)(implicit invocation: Invocation): DaalLinearRegressionTestReturn =
    {
      DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)

      //Load the DAAL linear regression model
      val model: Model = arguments.model
      val lrJsObject = model.data
      val trainedModel = lrJsObject.convertTo[DaalLinearRegressionModel]

      //create RDD from the frame
      val testFrame: SparkFrame = arguments.frame
      val observationColumns = arguments.observationColumns.getOrElse(trainedModel.observationColumns)
      val valueColumn = arguments.valueColumn.getOrElse(trainedModel.valueColumn)
      require(trainedModel.observationColumns.length == observationColumns.length,
        "Number of observations columns for train and predict should be same")

      val predictFrame = DaalLinearPredictAlgorithm(trainedModel, testFrame.rdd, observationColumns).predict()
      val summary = DaalLinearRegressionSummary(observationColumns, valueColumn, trainedModel, predictFrame).summarize()

      DaalLinearRegressionTestReturn(
        summary.explainedVariance,
        summary.meanAbsoluteError,
        summary.meanSquaredError,
        summary.r2,
        summary.rootMeanSquaredError
      )
    }

}
