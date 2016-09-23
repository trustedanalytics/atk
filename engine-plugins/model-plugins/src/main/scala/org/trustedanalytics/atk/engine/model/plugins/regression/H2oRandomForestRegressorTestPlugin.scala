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

import org.apache.spark.h2o.H2oModelData
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, ArgDoc, Invocation, SparkCommandPlugin }

//Implicits for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.h2o.H2oJsonProtocol._

/**
 * Arguments to H2O Random Forest Regression test plugin
 */
case class H2oRandomForestRegressorTestArgs(model: ModelReference,
                                            @ArgDoc("""The frame to test the random forest regression model on""") frame: FrameReference,
                                            @ArgDoc("""Column name containing the value of each observation""") valueColumn: String,
                                            @ArgDoc("""List of column(s) containing the observations""") observationColumns: Option[List[String]] = None)

/**
 * Return of H2O random forest Regression test plugin
 */
case class H2oRandomForestRegressorTestReturn(@ArgDoc("""Mean absolute error""") mae: Double,
                                              @ArgDoc("""Mean squared error""") mse: Double,
                                              @ArgDoc("""The square root of the mean squared error""") rmse: Double,
                                              @ArgDoc("""r-squared or coefficient of determination""") r2: Double,
                                              @ArgDoc("""Explained variance score""") explainedVarianceScore: Double)

/** Json conversion for arguments and return value case classes */
object H2oRandomForestRegressorTestJsonFormat {
  implicit val drfTestArgsFormat = jsonFormat4(H2oRandomForestRegressorTestArgs)
  implicit val drfTestReturnFormat = jsonFormat5(H2oRandomForestRegressorTestReturn)
}
import H2oRandomForestRegressorTestJsonFormat._

/* Run the  H2O random forest Regression model on the test frame*/
@PluginDoc(oneLine = "Predict test frame values and return metrics.",
  extended = """Predict the labels for a test frame and run regresssion metrics on predicted
and target labels.""",
  returns =
    """object
      An object with the results of the trained Random Forest Regressor:
      |mse : double
      |Mean squared error
      |rmse : double
      |Root mean squared error
      |r2: double
      |R-squared
    """)
class H2oRandomForestRegressorTestPlugin extends SparkCommandPlugin[H2oRandomForestRegressorTestArgs, H2oRandomForestRegressorTestReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:h2o_random_forest_regressor/test"

  /**
   * Run Spark ML's H2oRandomForestRegressor() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: H2oRandomForestRegressorTestArgs)(implicit invocation: Invocation): H2oRandomForestRegressorTestReturn = {

    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val h2oModelData = model.readFromStorage().convertTo[H2oModelData]
    if (arguments.observationColumns.isDefined) {
      require(h2oModelData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val obsColumns = arguments.observationColumns.getOrElse(h2oModelData.observationColumns)

    //predicting a label for the observation columns
    H2oRandomForestRegressorFunctions.getRegressionMetrics(frame.rdd, h2oModelData, obsColumns, arguments.valueColumn)
  }
}