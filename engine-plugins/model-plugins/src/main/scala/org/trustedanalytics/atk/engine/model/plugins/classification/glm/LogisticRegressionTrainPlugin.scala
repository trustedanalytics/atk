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
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, ApiMaturityTag, Invocation }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.engine.plugin.PluginDoc
import ModelPluginImplicits._

//Implicits needed for JSON conversion
import MLLibJsonProtocol._
import spray.json._

@PluginDoc(oneLine = "Build logistic regression model.",
  extended = "Create a LogisticRegressionModel using the observation column and label column of the train frame.",
  returns = """object
    An object with a summary of the trained model.
    The data returned is composed of multiple components:
numFeatures : Int
    Number of features in the training data
numClasses : Int
    Number of classes in the training data
summaryTable: table
    A summary table composed of:
covarianceMatrix: Frame (optional)
    Covariance matrix of the trained model.
    The covariance matrix is the inverse of the Hessian matrix for the trained model.
    The Hessian matrix is the second-order partial derivatives of the model's log-likelihood function
""""")
class LogisticRegressionTrainPlugin extends SparkCommandPlugin[LogisticRegressionTrainArgs, LogisticRegressionSummaryTable] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation) = arguments.numIterations + 5
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LogisticRegressionTrainArgs)(implicit invocation: Invocation): LogisticRegressionSummaryTable = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //create RDD from the frame
    val labeledTrainRdd = frame.rdd.toLabeledPointRDDWithFrequency(arguments.labelColumn,
      arguments.observationColumns, arguments.frequencyColumn)

    //Running MLLib
    val mlModel = LogisticRegressionModelWrapperFactory.createModel(arguments)
    val logRegModel = mlModel.getModel.run(labeledTrainRdd)

    //Create summary table and covariance frame
    val summaryTable = SummaryTableBuilder(
      logRegModel,
      arguments.observationColumns,
      arguments.intercept,
      mlModel.getHessianMatrix
    )

    val covarianceFrame = summaryTable.approxCovarianceMatrix match {
      case Some(matrix) =>
        val frameRdd = matrix.toFrameRdd(sc, summaryTable.coefficientNames)
        val frame = engine.frames.tryNewFrame(CreateEntityArgs(
          description = Some("covariance matrix created by LogisticRegression train operation"))) {
          newTrainFrame: FrameEntity =>
            newTrainFrame.save(frameRdd)
        }
        Some(frame)
      case _ => None
    }

    // Save model to metastore and return results
    val jsonModel = new LogisticRegressionData(logRegModel, arguments.observationColumns).toJson.asJsObject
    model.data = jsonModel
    summaryTable.build(covarianceFrame)
  }
}
