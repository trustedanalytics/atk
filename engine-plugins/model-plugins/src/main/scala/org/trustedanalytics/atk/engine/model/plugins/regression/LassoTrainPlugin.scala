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
import org.apache.spark.mllib.regression.{ LassoWithSGD, LassoModel, LabeledPoint }
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.scoring.models.LassoData
import org.trustedanalytics.atk.spray.json.JsonMaps
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

import org.apache.spark.mllib.linalg.{ DenseVector => MllibDenseVector }

case class LassoTrainArgs(@ArgDoc("""Handle to the model to be used.""") model: ModelReference,
                          @ArgDoc("""A frame to train the model on""") frame: FrameReference,
                          @ArgDoc("""Column name containing the value for each observation.""") valueColumn: String,
                          @ArgDoc("""List of column(s) containing the observations.""") observationColumns: List[String],
                          @ArgDoc("""Initial set of weights to be used. List should be equal in size to the number of features in the data.""") initialWeights: Option[List[Double]] = None,
                          @ArgDoc("""Number of iterations of gradient descent to run""") numIterations: Int = 500,
                          @ArgDoc("""Step size scaling to be used for the iterations of gradient descent.""") stepSize: Double = 1.0d,
                          @ArgDoc("""regParam Regularization parameter.""") regParam: Double = 0.01,
                          @ArgDoc("""Fraction of data to be used per iteration.""") miniBatchFraction: Double = 1.0) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(valueColumn != null && !valueColumn.isEmpty, "valueColumn must not be null nor empty")
  require(numIterations > 0, "numIterations must be a positive value")
  require(regParam >= 0, "regParam should be greater than or equal to 0")
}

case class LassoTrainReturn(@ArgDoc("""A list of n trained weights, where n is the number of features""") weights: List[Double],
                            @ArgDoc("""float value representing the independent term in decision function of the model""") intercept: Double)

@PluginDoc(oneLine = "Train Lasso Model",
  extended = """Train a Lasso model given an RDD of (label, features) pairs. We run a fixed number
 * of iterations of gradient descent using the specified step size. Each iteration uses
 * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
 * in gradient descent are initialized using the initial weights provided.""")
class LassoTrainPlugin extends SparkCommandPlugin[LassoTrainArgs, LassoTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lasso/train"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  //override def numberOfJobs(arguments: LassoTrainArgs)(implicit invocation: Invocation) = ???

  /**
   * Run Spark Mllib's Lasso on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LassoTrainArgs)(implicit invocation: Invocation): LassoTrainReturn = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame
    val weights: Array[Double] = if (arguments.initialWeights.isDefined) {
      arguments.initialWeights.get.toArray
    }
    else {
      Array.fill[Double](arguments.observationColumns.length)(1.0)
    }
    require(weights.length == arguments.observationColumns.length, s"Number of weights provided must be the same as the number of observation columns")

    val labeledPointRdd = frame.rdd.toLabeledPointRDD(arguments.valueColumn, arguments.observationColumns)
    val trainedModel = LassoWithSGD.train(labeledPointRdd,
      arguments.numIterations,
      arguments.stepSize,
      arguments.regParam,
      arguments.miniBatchFraction,
      new MllibDenseVector(weights)) //arguments.initialWeights.toArray))

    val jsonModel = new LassoData(trainedModel, arguments.observationColumns)
    val j = jsonModel.toJson.toString
    println(s"jsonModel=$j")
    model.data = jsonModel.toJson.asJsObject
    LassoTrainReturn(trainedModel.weights.toArray.toList, trainedModel.intercept)
  }

}

