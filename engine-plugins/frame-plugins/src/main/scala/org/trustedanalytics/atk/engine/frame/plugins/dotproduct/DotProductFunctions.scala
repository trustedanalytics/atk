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

package org.trustedanalytics.atk.engine.frame.plugins.dotproduct

import java.io.Serializable

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.{ VectorFunctions, RowWrapper }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating dot product
 */
object DotProductFunctions extends Serializable {

  /**
   * Computes the dot product for each row in the frame.
   *
   * Uses the left column values, and the right column values to compute the dot product for each row.
   *
   * @param frameRdd Data frame
   * @param leftColumnNames Left column names
   * @param rightColumnNames Right column names
   * @param defaultLeftValues  Optional default values used to substitute null values in the left columns
   * @param defaultRightValues Optional default values used to substitute null values in the right columns
   * @return Data frame with an additional column containing the dot product
   */
  def dotProduct(frameRdd: FrameRdd, leftColumnNames: List[String], rightColumnNames: List[String],
                 defaultLeftValues: Option[List[Double]] = None,
                 defaultRightValues: Option[List[Double]] = None): RDD[Row] = {

    frameRdd.frameSchema.requireColumnsAreVectorizable(leftColumnNames)
    frameRdd.frameSchema.requireColumnsAreVectorizable(rightColumnNames)

    val leftVectorSize = getVectorSize(frameRdd, leftColumnNames)
    val rightVectorSize = getVectorSize(frameRdd, rightColumnNames)

    require(leftVectorSize == rightVectorSize,
      "number of elements in left columns should equal number in right columns")
    require(defaultLeftValues.isEmpty || defaultLeftValues.get.size == leftVectorSize,
      "size of default left values should match number of elements in left columns")
    require(defaultRightValues.isEmpty || defaultRightValues.get.size == leftVectorSize,
      "size of default right values should match number of elements in right columns")

    frameRdd.mapRows(row => {
      val leftVector = createVector(row, leftColumnNames, defaultLeftValues)
      val rightVector = createVector(row, rightColumnNames, defaultRightValues)
      val dotProduct = computeDotProduct(leftVector, rightVector)
      new GenericRow(row.valuesAsArray() :+ dotProduct)
    })
  }

  /**
   * Create vector of doubles from column values.
   *
   * The columns contain numeric data represented as scalars or vectors
   *
   * @param row Input row
   * @param columnNames Input column names
   * @param defaultValues Default values used to substitute nulls
   * @return Vector of doubles
   */
  def createVector(row: RowWrapper,
                   columnNames: List[String],
                   defaultValues: Option[List[Double]]): Vector[Double] = {
    VectorFunctions.createVector(row, columnNames, defaultValues)
  }

  /**
   * Replace NaNs in input vector with default values
   *
   * @param vector Input vector
   * @param defaultValues Default values
   * @return Vector with NaNs replaced by defaults or zero
   */
  def replaceNaNs(vector: Vector[Double], defaultValues: Option[List[Double]]): Vector[Double] = {
    VectorFunctions.replaceNaNs(vector, defaultValues)
  }

  /**
   * Computes the dot product of two vectors
   *
   * The dot product is the sum of the products of the corresponding entries in the two vectors.
   *
   * @param leftVector Left vector
   * @param rightVector Right vector
   * @return Dot product
   */
  def computeDotProduct(leftVector: Vector[Double], rightVector: Vector[Double]): Double = {
    VectorFunctions.dotProduct(leftVector, rightVector)
  }

  private def getVectorSize(frameRdd: FrameRdd, columnNames: List[String]): Int = {
    val frameSchema = frameRdd.frameSchema
    columnNames.map(name =>
      frameSchema.columnDataType(name) match {
        case DataTypes.vector(length) => length
        case _ => 1
      }).sum.toInt
  }
}
