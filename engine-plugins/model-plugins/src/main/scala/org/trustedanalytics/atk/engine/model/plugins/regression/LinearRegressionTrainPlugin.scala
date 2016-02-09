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

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.optimization.{ L1Updater, SquaredL2Updater }
import org.apache.spark.mllib.regression.{ LabeledPoint, LinearRegressionWithSGD }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{ StructType, StructField, DoubleType }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.model.plugins.classification.ClassificationWithSGDTrainArgs
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.ml.atk.plugins.MLJsonProtocol._
//Implicits needed for JSON conversion
import spray.json._

@PluginDoc(oneLine = "Build linear regression model.",
  extended = "Creating a LinearRegression Model using the observation column and target column of the train frame",
  returns = "Trained linear regression model")
class LinearRegressionTrainPlugin extends SparkCommandPlugin[LinearRegressionTrainArgs, LinearRegressionTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:linear_regression_ml/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LinearRegressionTrainArgs)(implicit invocation: Invocation) = arguments.maxIterations + 9
  /**
   * Run MLLib's LinearRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LinearRegressionTrainArgs)(implicit invocation: Invocation): LinearRegressionTrainReturn = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val trainFrameRdd = frame.rdd
    //Running MLLib
    val dataFrame = trainFrameRdd.toLabeledDataFrame(arguments.labelColumn, arguments.observationColumns)

    val linReg = LinearRegressionTrainPlugin.initializeLinearRegressionModel(arguments)
    val linRegModel = linReg.fit(dataFrame)
    val jsonModel = new LinearRegressionData(linRegModel, arguments.observationColumns, arguments.labelColumn)

    model.data = jsonModel.toJson.asJsObject
    val intercept = linRegModel.intercept
    val weights = linRegModel.weights
    val summary = linRegModel.summary
    val explainedVariance = summary.explainedVariance
    val meanAbsoluteError = summary.meanAbsoluteError
    val meanSquaredError = summary.meanSquaredError
    val objectiveHistory = summary.objectiveHistory
    val r2 = summary.r2
    val rootMeanSquaredError = summary.rootMeanSquaredError
    val iterations = summary.totalIterations
    new LinearRegressionTrainReturn(arguments.observationColumns, arguments.labelColumn, intercept, weights.toArray, explainedVariance, meanAbsoluteError,
      meanSquaredError, objectiveHistory, r2, rootMeanSquaredError, iterations)
  }
}
object LinearRegressionTrainPlugin {

  def initializeLinearRegressionModel(arguments: LinearRegressionTrainArgs): LinearRegression = {
    val linReg = new LinearRegression()
    linReg.setElasticNetParam(arguments.elasticNetParameter)
    linReg.setFitIntercept(arguments.fitIntercept)
    linReg.setMaxIter(arguments.maxIterations)
    linReg.setRegParam(arguments.regParam)
    linReg.setStandardization(arguments.standardization)
    linReg.setTol(arguments.tolerance)
    linReg.setLabelCol("label")
    linReg.setFeaturesCol("features")

  }

}

