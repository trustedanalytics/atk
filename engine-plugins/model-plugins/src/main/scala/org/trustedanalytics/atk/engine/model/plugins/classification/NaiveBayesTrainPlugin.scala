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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, PluginDoc, ApiMaturityTag, Invocation }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import ModelPluginImplicits._
import org.apache.spark.rdd.RDD

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class NaiveBayesTrainArgs(model: ModelReference,
                               @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                               @ArgDoc("""Column containing the label for each
observation.""") labelColumn: String,
                               @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                               @ArgDoc("""Additive smoothing parameter
Default is 1.0.""") lambdaParameter: Double = 1.0) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
}

@PluginDoc(oneLine = "Build a naive bayes model.",
  extended = """Train a NaiveBayesModel using the observation column, label column of the train frame and an optional lambda value.""",
  returns = """Trained NaiveBayes model""")
class NaiveBayesTrainPlugin extends SparkCommandPlugin[NaiveBayesTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation) = 109

  /**
   * Run MLLib's NaiveBayes() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

    //Running MLLib
    val naiveBayes = new NaiveBayes()
    naiveBayes.setLambda(arguments.lambdaParameter)

    val naiveBayesModel = naiveBayes.run(labeledTrainRdd)
    val jsonModel = new NaiveBayesData(naiveBayesModel, arguments.observationColumns)

    model.data = jsonModel.toJson.asJsObject
  }
}
