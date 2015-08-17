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

package org.trustedanalytics.atk.plugins.query.recommend

import org.trustedanalytics.atk.graphbuilder.elements.GraphElement
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.jblas.DoubleMatrix
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object RecommendFeatureVector {
  /**
   * Parse property value in Double type
   *
   * @param graphElement Graph element which can be a vertex or an edge
   * @param propertyName Property name for prior probabilities
   * @return property value in Double type
   */
  def parseDoubleValue(graphElement: GraphElement, propertyName: String): Double = {
    val value = graphElement.getPropertyValueAsString(propertyName)
    if (value != "") {
      Try { value.toDouble }.getOrElse(0d)
    }
    else {
      throw new RuntimeException("parseDoubleValue, property does not exist in the graph element:" +
        " propertyName=" + propertyName + ", graph element=" + graphElement)
    }
  }

  /**
   * Parse property value in Double type
   *
   * @param graphElement Graph element which can be a vertex or an edge
   * @param propertyName Property name for prior probabilities
   * @return property value in Double Array type
   */
  def parseDoubleArray(graphElement: GraphElement, propertyName: String,
                       sep: String = "[\\s,\\t]+"): Array[Double] = {
    val result = graphElement.getPropertyValueAsString(propertyName)
    if (result != "") {
      result.split(sep).map(v => {
        Try { v.toDouble }.getOrElse(0d)
      })
    }
    else {
      throw new RuntimeException("parseDoubleArray, property does not exist in the graph element:" +
        " propertyName=" + propertyName + ", graph element=" + graphElement)
    }
  }

  /**
   * Parse result vector
   *
   * @param graphElement Graph element which can be a vertex or an edge
   * @param resultPropertyList Property name for prior probabilities
   * @param vectorValue  Whether results is stored as a vector for each vertex
   * @param biasOn  Whether biasOn was turned on/off during ALS/CGD calculation
   *                When bias is enabled, the last property name in the output_vertex_property_list is for bias.
   * @param sep Delimiter (defaults to comma)
   * @return Array of feature probabilities
   */
  def parseResultArray(graphElement: GraphElement, resultPropertyList: Array[String],
                       vectorValue: Boolean, biasOn: Boolean, sep: String = "[\\s,\\t]+"): Array[Double] = {
    val results = ArrayBuffer[Double]()
    val length = resultPropertyList.length
    var valueLength = length

    if (biasOn) {
      results += parseDoubleValue(graphElement, resultPropertyList(length - 1))
      valueLength = length - 1
    }

    //then add the results
    if (vectorValue) {
      results ++= parseDoubleArray(graphElement, resultPropertyList(0), sep)
    }
    else {
      for (i <- 0 until valueLength) {
        results += parseDoubleValue(graphElement, resultPropertyList(i))
      }
    }
    results.toArray
  }

  /**
   * Predict the top selections for input vertex Id.
   *
   * @param sourceVector  vectorValue of source vertex
   * @param targetVectorRDD  RDD of (vertexId, resultVector) pairs.
   * @return RDD of (vertexId, score) pairs.
   */
  def predict(sourceVector: Array[Double], targetVectorRDD: RDD[TargetTuple], biasOn: Boolean): RDD[Rating] = {
    var sum = if (biasOn) {
      sourceVector.head
    }
    else 0

    val sourceVectorValue = if (biasOn) {
      new DoubleMatrix(sourceVector.tail)
    }
    else new DoubleMatrix(sourceVector)

    targetVectorRDD.map {
      case targetTuple =>
        {
          val targetVector = targetTuple.resultVector
          if (sourceVector.length != targetVector.length) {
            throw new RuntimeException("the vector size of left-side vertex and right-side vertex does not match")
          }
          val targetVectorValue = if (biasOn) {
            new DoubleMatrix(targetVector.tail)
          }
          else new DoubleMatrix(targetVector)

          sum = if (biasOn) {
            sum + targetVector.head + sourceVectorValue.dot(targetVectorValue)
          }
          else sourceVectorValue.dot(targetVectorValue)

          Rating(targetTuple.targetVertexId, sum)
        }
    }
  }

}

/**
 * Feature vector containing prior and posterior probabilities.
 *
 * @param resultVector the result vector (one element for each feature)
 */
case class RecommendFeatureVector(resultVector: Array[Double])

/**
 * Rating tuple contains recommended vertex Id and its score.
 *
 * @param vertexId predicted vertex Id
 * @param score predicted score
 */
case class Rating(vertexId: String, score: Double)

/**
 * Rating tuple contains rank, recommended vertex Id, and its score.
 *
 * The rank is generated by sorting the recommendations in descending order based on score.
 *
 * @param rank Rank of recommendation
 * @param vertexId predicted vertex Id
 * @param score predicted score
 */
case class RankedRating(rank: Int, vertexId: String, score: Double)

/**
 * Rating tuple contains recommended vertex Id and its score.
 *
 * @param targetVertexId vertex Id
 * @param resultVector result vector of the vertexId
 */
case class TargetTuple(targetVertexId: String, resultVector: Array[Double])
