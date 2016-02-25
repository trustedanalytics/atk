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
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.LabeledPoint
import ModelPluginImplicits._
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.scoring.models.SVMData

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Build SVM with SGD model",
  extended = """Creating a SVM Model using the observation column and label column of the train frame.""",
  returns = """A trained SVMWithSGD model""")
class SVMWithSGDTrainPlugin extends SparkCommandPlugin[ClassificationWithSGDTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:svm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ClassificationWithSGDTrainArgs)(implicit invocation: Invocation) = 103

  /**
   * Run MLLib's SVMWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ClassificationWithSGDTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val models = engine.models
    val model: Model = arguments.model

    val frame: SparkFrame = arguments.frame
    frame.schema.validateColumnsExist(arguments.observationColumns)

    if (frame.rdd.isEmpty()){
      throw new RuntimeException("Training Frame is empty. Please train with a non-empty Frame")
    }
    val labeledTrainRDD: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

    //Running MLLib
    val svm = SVMWithSGDTrainPlugin.initializeSVMModel(arguments)
    val svmModel = svm.run(labeledTrainRDD)

    val jsonModel = new SVMData(svmModel, arguments.observationColumns)
    model.data = jsonModel.toJson.asJsObject
  }
}

object SVMWithSGDTrainPlugin {

  def initializeSVMModel(arguments: ClassificationWithSGDTrainArgs): SVMWithSGD = {
    val svm = new SVMWithSGD()
    svm.optimizer.setNumIterations(arguments.numIterations)
    svm.optimizer.setStepSize(arguments.stepSize)
    svm.optimizer.setRegParam(arguments.regParam)

    if (arguments.regType.isDefined) {
      svm.optimizer.setUpdater(arguments.regType.get match {
        case "L1" => new L1Updater()
        case other => new SquaredL2Updater()
      })
    }
    svm.optimizer.setMiniBatchFraction(arguments.miniBatchFraction)
    svm.setIntercept(arguments.intercept)
  }
}

