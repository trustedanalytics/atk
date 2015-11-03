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

package org.trustedanalytics.atk.engine.model.plugins.libsvm

import java.util.StringTokenizer

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import libsvm.{ svm_node, svm_problem, svm_parameter, svm }
import org.apache.spark.frame.FrameRdd

//Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import LibSvmJsonProtocol._
import spray.json._

// TODO: all plugins should move out of engine-core into plugin modules

@PluginDoc(oneLine = "Train a Lib Svm model",
  extended = """Creating a lib Svm Model using the observation column and label column of the
train frame.""",
  returns = """A trained Libsvm model""")
class LibSvmTrainPlugin extends SparkCommandPlugin[LibSvmTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LibSvmTrainArgs)(implicit invocation: Invocation) = 1

  /**
   * Run LibSvm on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: LibSvmTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    //Running LibSVM
    val param = initializeParameters(arguments)
    val prob = initializeProblem(frame.rdd, arguments, param)

    val errorMsg = svm.svm_check_parameter(prob, param)
    if (errorMsg != null) {
      throw new IllegalArgumentException("Illegal Argument Exception: " + errorMsg)
    }

    val mySvmModel = svm.svm_train(prob, param)

    val jsonModel = new LibSvmData(mySvmModel, arguments.observationColumns)
    model.data = jsonModel.toJson.asJsObject
  }

  /**
   * Initializes the libsvm params based on user's input else to default values
   *
   * @param arguments user supplied arguments for initializing the libsvm params
   * @return a data structure containing all the user supplied or default values for libsvm
   */
  private def initializeParameters(arguments: LibSvmTrainArgs): svm_parameter = {
    val param = new svm_parameter()

    // values for svm_parameters
    param.svm_type = arguments.getSvmType
    param.kernel_type = arguments.getKernelType
    param.degree = arguments.getDegree
    param.gamma = arguments.getGamma
    param.coef0 = arguments.getCoef0
    param.nu = arguments.getNu
    param.cache_size = arguments.getCacheSize
    param.C = arguments.getC
    param.eps = arguments.getEpsilon
    param.p = arguments.getP
    param.shrinking = arguments.getShrinking
    param.probability = arguments.getProbability
    param.nr_weight = arguments.getNrWeight
    param.weight_label = arguments.getWeightLabel
    param.weight = arguments.getWeight
    param
  }

  /**
   * Extracts the dataset from the provided Dataframe and converts into libsvm format
   *
   * @param trainFrameRdd Rdd containing the label and feature columns
   * @param arguments user supplied arguments for running this plugin
   * @param param data structure containing all the values for libsvm's exposed params
   * @return libsvm problem
   */

  private def initializeProblem(trainFrameRdd: FrameRdd, arguments: LibSvmTrainArgs, param: svm_parameter): svm_problem = {

    val observedRdd = trainFrameRdd.selectColumns(arguments.labelColumn +: arguments.observationColumns)
    val observedSchema = observedRdd.frameSchema.columns

    val output = LibSvmPluginFunctions.LibSvmFormatter(observedRdd)

    var vectory = Vector.empty[Double]
    var vectorx = Vector.empty[Array[svm_node]]
    var max_index: Int = 0
    val prob = new svm_problem()

    for (i <- output.indices) {
      val observation = output(i)
      val splitObs: StringTokenizer = new StringTokenizer(observation, " \t\n\r\f:")

      vectory = vectory :+ LibSvmPluginFunctions.atof(splitObs.nextToken)
      val counter: Int = splitObs.countTokens / 2
      val x: Array[svm_node] = new Array[svm_node](counter)

      var j: Int = 0
      while (j < counter) {
        x(j) = new svm_node
        x(j).index = LibSvmPluginFunctions.atoi(splitObs.nextToken)
        x(j).value = LibSvmPluginFunctions.atof(splitObs.nextToken)
        j += 1
      }
      if (counter > 0) max_index = Math.max(max_index, x(counter - 1).index)
      vectorx = vectorx :+ x
    }

    prob.l = vectory.size
    prob.x = Array.ofDim[Array[svm_node]](prob.l)
    var k: Int = 0
    while (k < prob.l) {
      prob.x(k) = vectorx(k)
      k += 1
    }
    prob.y = Array.ofDim[Double](prob.l)
    var i: Int = 0
    while (i < prob.l) {
      prob.y(i) = vectory(i)
      i += 1
    }

    if (param.gamma == 0 && max_index > 0) param.gamma = 1.0 / max_index
    prob
  }

}
