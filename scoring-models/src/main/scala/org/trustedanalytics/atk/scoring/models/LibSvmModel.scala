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

package org.trustedanalytics.atk.scoring.models

import java.util.StringTokenizer

import org.trustedanalytics.atk.scoring.interfaces.{ Model, Field }
import libsvm.{ svm, svm_node, svm_model }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LibSvmModel(libSvmModel: svm_model, libsvm: LibSvmData) extends svm_model with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val output = columnFormatter(data.zipWithIndex)
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
    data :+ svm.svm_predict(libSvmModel, x)
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

  override def modelMetadata(): Map[String, String] = {
    //TODO: get the created date from Publish
    Map("Model Type" -> "LibSvm Model", "Class Name" -> classOf[LibSvmModel].getName, "Model Reader" -> classOf[LibSvmModelReaderPlugin].getName, "Created On" -> "Jan 29th 2016")
  }

  /**
   *  @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = libsvm.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   *  @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    //Double
    output :+ Field("Prediction", "Double")
  }
}
