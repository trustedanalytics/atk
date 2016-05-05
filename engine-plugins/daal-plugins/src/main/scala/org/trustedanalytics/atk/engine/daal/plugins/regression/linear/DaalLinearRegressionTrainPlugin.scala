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

import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.daal.plugins.tables.DaalConversionImplicits
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._

//Implicits needed for JSON conversion
import spray.json._
import DaalLinearRegressionModelFormat._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalLinearRegressionJsonFormat._

/** Plugin for training Intel DAAL Linear Regression using QR decomposition */
@PluginDoc(oneLine = "Build Intel DAAL linear regression model.",
  extended = "Create Intel DAAL LinearRegression Model using the observation column and target column of the train frame",
  returns = "Trained Intel DAAL linear regression model")
class DaalLinearRegressionTrainPlugin extends SparkCommandPlugin[DaalLinearRegressionTrainArgs, DaalLinearRegressionTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Run DAAL's Linear Regression with QR decomposition on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalLinearRegressionTrainArgs)(implicit invocation: Invocation): DaalLinearRegressionTrainReturn =
    {
      DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
      val model: Model = arguments.model

      // Create RDD from the frame
      val trainFrame: SparkFrame = arguments.frame
      val observationColumns = arguments.observationColumns
      val valueColumn = arguments.valueColumn
      val fitIntercept = arguments.fitIntercept

      // Train model
      val trainedModel = DaalLinearTrainAlgorithm(trainFrame.rdd, observationColumns, valueColumn, fitIntercept).train()
      model.data = trainedModel.toJson.asJsObject

      // Compute summary statistics for regression model
      val predictFrameRdd = DaalLinearPredictAlgorithm(trainedModel, trainFrame.rdd, observationColumns).predict()

      DaalLinearRegressionSummary(observationColumns, valueColumn, trainedModel, predictFrameRdd).summarize()
    }
}
