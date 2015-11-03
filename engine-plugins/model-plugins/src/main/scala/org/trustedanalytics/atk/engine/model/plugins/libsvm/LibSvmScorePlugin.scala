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

import org.trustedanalytics.atk.domain.DoubleValue
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, CommandPlugin, Invocation, PluginDoc }
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import LibSvmJsonProtocol._
import libsvm.{ svm_model, svm, svm_node }
import org.apache.spark.frame.FrameRdd

// TODO: all plugins should move out of engine-core into plugin modules

@PluginDoc(oneLine = "Calculate the prediction label for a single observation.",
  extended = "",
  returns = "Predicted label.")
class LibSvmScorePlugin extends CommandPlugin[LibSvmScoreArgs, DoubleValue] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:libsvm/score"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: LibSvmScoreArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a vector
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */

  override def execute(arguments: LibSvmScoreArgs)(implicit invocation: Invocation): DoubleValue = {
    val model: Model = arguments.model

    val libsvmData = model.data.convertTo[LibSvmData]
    val libsvmModel = libsvmData.svmModel

    LibSvmPluginFunctions.score(libsvmModel, arguments.vector)
  }
}

object LibSvmPluginFunctions extends Serializable {

  def score(libsvmModel: svm_model, vector: Vector[Double]): DoubleValue = {
    val output = columnFormatter(vector.toArray.zipWithIndex)

    val splitObs: StringTokenizer = new StringTokenizer(output, " \t\n\r\f:")
    splitObs.nextToken()
    val counter: Int = splitObs.countTokens / 2
    val x: Array[svm_node] = new Array[svm_node](counter)
    var j: Int = 0
    while (j < counter) {
      x(j) = new svm_node
      x(j).index = atoi(splitObs.nextToken) + 1
      x(j).value = atof(splitObs.nextToken)
      j += 1
    }
    new DoubleValue(svm.svm_predict(libsvmModel, x))
  }

  def LibSvmFormatter(frameRdd: FrameRdd): Array[String] = {
    frameRdd.map(row => columnFormatterForTrain(row.toSeq.toArray.zipWithIndex)).collect()
  }

  private def columnFormatterForTrain(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if index != 0 && value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  private def columnFormatter(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }

  def atof(s: String): Double = {
    s.toDouble
  }

  def atoi(s: String): Int = {
    Integer.parseInt(s)
  }
}
