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


package org.trustedanalytics.atk.engine.frame

import java.io.Serializable

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.DataTypes

/**
 * Object for calculating dot product
 */
object VectorFunctions extends Serializable {

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
    val vector = columnNames.flatMap(columnName => {
      val value = row.value(columnName)

      row.schema.columnDataType(columnName) match {
        case DataTypes.vector(length) =>
          if (value == null) {
            Array.fill[Double](length.toInt)(Double.NaN)
          }
          else {
            DataTypes.toVector(length)(value)
          }
        case _ => if (value == null) Array(Double.NaN) else Array(DataTypes.toDouble(value))
      }
    }).toVector

    replaceNaNs(vector, defaultValues)
  }

  /**
   * Replace NaNs in input vector with default values
   *
   * @param vector Input vector
   * @param defaultValues Default values
   * @return Vector with NaNs replaced by defaults or zero
   */
  def replaceNaNs(vector: Vector[Double], defaultValues: Option[List[Double]]): Vector[Double] = {
    require(defaultValues.isEmpty || defaultValues.get.size == vector.size, s"size in default values should be ${vector.size}")

    vector.zipWithIndex.map {
      case (value, i) =>
        value match {
          case x if x.isNaN => if (defaultValues.isDefined) defaultValues.get(i) else 0d
          case _ => value
        }
    }
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
  def dotProduct(leftVector: Seq[Double], rightVector: Seq[Double]): Double = {
    require(leftVector.nonEmpty, "left vector should not be empty")
    require(leftVector.size == rightVector.size, "size of left vector should equal size of right vector")

    var dotProduct = 0d
    for (i <- 0 until leftVector.size) {
      dotProduct += leftVector(i) * rightVector(i)
    }
    dotProduct
  }

}
