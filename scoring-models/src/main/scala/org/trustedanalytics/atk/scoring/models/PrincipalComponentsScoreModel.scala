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

import breeze.linalg
import org.apache.spark.mllib.linalg._
import org.trustedanalytics.atk.scoring.interfaces.{ModelMetaData, Model, Field}
import scala.concurrent.ExecutionContext.Implicits.global
//import scala.collection.mutable.Map
import scala.concurrent._

/**
 * Scoring model for Principal Components
 */
class PrincipalComponentsScoreModel(pcaModel: PrincipalComponentsData) extends PrincipalComponentsData(pcaModel.k, pcaModel.observationColumns,
  pcaModel.meanCentered, pcaModel.meanVector, pcaModel.singularValues, pcaModel.vFactor) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val x: Array[Double] = new Array[Double](data.length)
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = value.asInstanceOf[Double]
    }
    val y: DenseMatrix = computePrincipalComponents(x.slice(0, x.length - 1))
    val pcaScoreOutput: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    pcaScoreOutput.put("principal_components", y.values.toList)
    val t_squared_index = computeTSquaredIndex(y.values, pcaModel.singularValues, x(x.length - 1).toInt)
    pcaScoreOutput.put("t_squared_index", t_squared_index)
    data :+ pcaScoreOutput
  }

  /**
   * Compute the principal components for the observation
   * @param x Each observation stored as an Array[Double]
   * @return (org.apache.spark.mllib)DenseMatrix
   */
  def computePrincipalComponents(x: Array[Double]): DenseMatrix = {
    var inputVector = new org.apache.spark.mllib.linalg.DenseVector(x)
    if (pcaModel.meanCentered) {
      val meanCenteredVector: Array[Double] = (new linalg.DenseVector(x) - new linalg.DenseVector(pcaModel.meanVector.toArray)).toArray
      inputVector = new org.apache.spark.mllib.linalg.DenseVector(meanCenteredVector)
    }
    new DenseMatrix(1, inputVector.size, inputVector.toArray).multiply(pcaModel.vFactor.asInstanceOf[DenseMatrix])
  }

  override def modelMetadata(): ModelMetaData = {
    //TODO: get the created date from Publish
    new ModelMetaData("Principal Components Model", classOf[PrincipalComponentsScoreModel].getName, classOf[PrincipalComponentsModelReaderPlugin].getName, Map("Created_On" -> "Jan 29th 2016"))
  }

  /**
   * Compute the t-squared index for the observation
   * @param y Projection of singular vectors on the input
   * @param E Right singular values of the input
   * @param k Number of principal components
   * @return t-squared index for the observation
   */
  def computeTSquaredIndex(y: Array[Double], E: Vector, k: Int): Double = {
    val yArray: Array[Double] = y
    var t: Double = 0.0
    for (i <- 0 to k - 1) {
      t += ((yArray(i) * yArray(i)) / (E(i) * E(i)))
    }
    t
  }

  /**
   *  @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    val obsCols = pcaModel.observationColumns
    var input = Array[Field]()
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
    output = output :+ Field("principal_components", "List[Double]")
    output :+ Field("t_squared_index", "Double")

  }
}
