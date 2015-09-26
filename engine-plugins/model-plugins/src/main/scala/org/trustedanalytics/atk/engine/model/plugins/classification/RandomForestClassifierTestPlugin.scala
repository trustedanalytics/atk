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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.frame.{ FrameReference, ClassificationMetricValue }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.frame.plugins.{ ScoreAndLabel, ClassificationMetrics }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.apache.spark.mllib.regression.LabeledPoint
import ModelPluginImplicits._
import org.apache.spark.rdd.RDD

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Command for testing model data in the model.
 */
case class RandomForestClassifierTestArgs(@ArgDoc("""Handle of the model to be used""") model: ModelReference,
                                          @ArgDoc("""The frame whose labels are to be predicted""") frame: FrameReference,
                                          @ArgDoc("""Column containing the true labels of the observations""") labelColumn: String,
                                          @ArgDoc("""Column(s) containing the observations whose labels are to be predicted.
By default, we predict the labels over columns the RandomForest was trained on.""") observationColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")

}

@PluginDoc(oneLine = "Predict test frame labels and return metrics.",
  extended = """Predict the labels for a test frame and run classification metrics on predicted
and target labels.""",
  returns = """An object with classification metrics.
The data returned is composed of multiple components\:

|  **double** : *accuracy*
|  **table** : *confusion_matrix*
|  **double** : *f_measure*
|  **double** : *precision*
|  **double** : *recall*""")
class RandomForestClassifierTestPlugin extends SparkCommandPlugin[RandomForestClassifierTestArgs, ClassificationMetricValue] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_classifier/test"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: RandomForestClassifierTestArgs)(implicit invocation: Invocation) = 9
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RandomForestClassifierTestArgs)(implicit invocation: Invocation): ClassificationMetricValue = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    //Extracting the model and data to run on
    val rfData = model.data.convertTo[RandomForestClassifierData]
    val rfModel = rfData.randomForestModel
    if (arguments.observationColumns.isDefined) {
      require(rfData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and test should be same")
    }
    val rfColumns = arguments.observationColumns.getOrElse(rfData.observationColumns)

    //predicting and testing
    val scoreAndLabelRdd = frame.rdd.toScoreAndLabelRdd(row => {
      val labeledPoint = row.valuesAsLabeledPoint(rfColumns, arguments.labelColumn)
      val score = rfModel.predict(labeledPoint.features)
      ScoreAndLabel(score, labeledPoint.label)
    })

    val output = rfData.numClasses match {
      case 2 => {
        val posLabel = 1d
        ClassificationMetrics.binaryClassificationMetrics(scoreAndLabelRdd, posLabel)
      }
      case _ => ClassificationMetrics.multiclassClassificationMetrics(scoreAndLabelRdd)
    }
    output
  }
}
