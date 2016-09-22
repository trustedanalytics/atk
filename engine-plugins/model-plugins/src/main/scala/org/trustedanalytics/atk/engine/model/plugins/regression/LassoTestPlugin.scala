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

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.ClassificationMetricValue
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.frame.plugins.ScoreAndLabel
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.model.plugins.classification.ClassificationWithSGDTestArgs
import org.trustedanalytics.atk.engine.plugin.ApiMaturityTag
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.scoring.models.LassoData

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Return of Linear Regression test plugin
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class LassoTestReturn(@ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                           @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                           @ArgDoc("""The unadjusted coefficient of determination""") r2: Double,
                           @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double)

/* Run the Lasso model on the test frame*/
@PluginDoc(oneLine = "Predict test frame labels and return metrics.",
  extended = """Predict the labels for a test frame and run classification metrics on predicted
and target labels.""",
  returns = """A dictionary with binary classification metrics.
The data returned is composed of the following keys\:

              |  'accuracy' : double
              |  The proportion of predictions that are correctly identified
              |  'confusion_matrix' : dictionary
              |  A table used to describe the performance of a classification model
              |  'f_measure' : double
              |  The harmonic mean of precision and recall
              |  'precision' : double
              |  The proportion of predicted positive instances that are correctly identified
              |  'recall' : double
              |  The proportion of positive instances that are correctly identified.""")
class LassoTestPlugin extends SparkCommandPlugin[ClassificationWithSGDTestArgs, LassoTestReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lasso/test"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ClassificationWithSGDTestArgs)(implicit invocation: Invocation) = 1
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDTestArgs)(implicit invocation: Invocation): LassoTestReturn = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    require(!frame.rdd.isEmpty(), "Test Frame is empty. Please test on a non-empty Frame.")
    //Extracting the model and data to run on
    val lassoData = model.data.convertTo[LassoData]
    val lassoModel = lassoData.lassoModel
    if (arguments.observationColumns.isDefined) {
      require(lassoData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and test should be same")
    }
    val lassoColumns = arguments.observationColumns.getOrElse(lassoData.observationColumns)

    //predicting and testing
    val predictionLabelRdd: RDD[(Double, Double)] = frame.rdd.mapRows(row => {
      val labeledPoint = row.valuesAsLabeledPoint(lassoColumns, arguments.labelColumn)
      val score = lassoModel.predict(labeledPoint.features)
      (score, labeledPoint.label)
    })

    val metrics = new RegressionMetrics(predictionLabelRdd)

    LassoTestReturn(metrics.meanAbsoluteError, metrics.meanSquaredError, metrics.r2, metrics.rootMeanSquaredError)
  }
}

