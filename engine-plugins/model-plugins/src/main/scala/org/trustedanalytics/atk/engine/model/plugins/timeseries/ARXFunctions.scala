/**
 *  Copyright (c) 2016 Intel Corporation 
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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import java.io.Serializable
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.Schema
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.trustedanalytics.atk.engine.frame.{ VectorFunctions, RowWrapper }
import org.apache.spark.frame.FrameRdd
import com.cloudera.sparkts._
import breeze.linalg._

import scala.collection.mutable.{ ArrayBuffer, WrappedArray }

/**
 * Object contains utility functions for working with time series
 */
object ARXFunctions extends Serializable {

  /**
   * Checks the frame's schema to verify that the specified column exists and is a vector.
   * @param schema  Frame schema to check
   * @param columnName  Name of the vector column to check
   * @return The length of the specified vector column
   */
  def verifyVectorColumn(schema: Schema, columnName: String): Long = {
    var vectorLength: Long = 0

    if (schema.hasColumn(columnName) == false) {
      // Column does not exist
      throw new IllegalArgumentException(s"Column named $columnName does not exist in the frame's schema.")
    }
    else {
      // Verify that it's a vector column
      schema.columnDataType(columnName) match {
        case DataTypes.vector(length) => vectorLength = length
        case _ => throw new IllegalArgumentException(s"Column $columnName must be a vector.")
      }
    }

    return vectorLength
  }

  def getYandXFromVectors(row: Row, schema: Schema, yColumnName: String, xColumnNames: List[String]): (Array[Double], Array[Array[Double]]) = {
    val yColumnLength = verifyVectorColumn(schema, yColumnName).toInt
    val yColumnIndex = schema.columnIndex(yColumnName)
    val yValues = row.get(yColumnIndex).asInstanceOf[WrappedArray[Double]].toArray
    val xValues = Array.ofDim[Double](yColumnLength, xColumnNames.size)

    var i = 0
    for (xColumnName <- xColumnNames) {
      val xColumnLength = verifyVectorColumn(schema, xColumnName)
      val xColumnIndex = schema.columnIndex(xColumnName)

      if (xColumnLength != yColumnLength) {
        throw new RuntimeException("Length of x column '" + xColumnName + "' must have the same vector length (" + xColumnLength.toString + ") as the y column '" + yColumnName + "' (" + yColumnLength.toString + ").")
      }

      val xValue = row.get(xColumnIndex).asInstanceOf[WrappedArray[Double]].toArray
      for (j <- 0 until xValue.size) {
        xValues(j)(i) = xValue(j)
      }
      i += 1
    }

    return (yValues, xValues)
  }

  def getYandXFromRows(frame: FrameRdd, yColumnName: String, xColumnNames: List[String]): (Array[Double], Array[Array[Double]]) = {
    val schema = frame.frameSchema

    schema.requireColumnIsNumerical(yColumnName)

    for (xColumn <- xColumnNames) {
      schema.requireColumnIsNumerical(xColumn)
    }
    val totalRowCount = frame.count.toInt
    val yValues = Array.ofDim[Double](totalRowCount)
    val xValues = Array.ofDim[Double](totalRowCount, xColumnNames.size)
    var rowCounter = 0

    for (row <- frame.collect()) {
      yValues(rowCounter) = row.getAs[Double](yColumnName)

      var xColumnCounter = 0
      for (xColumn <- xColumnNames) {
        xValues(rowCounter)(xColumnCounter) = row.getAs[Double](xColumn)
        xColumnCounter += 1
      }

      rowCounter += 1
    }

    return (yValues, xValues)

  }

  def getYandXFromFrame(frame: FrameRdd, yColumnName: String, xColumnNames: List[String]): (Vector[Double], Matrix[Double]) = {

    val (yValues, xValues) = (frame.frameSchema.columnDataType(yColumnName)) match {
      case DataTypes.vector(length) => {
        val row = frame.collect()(0) // Grab the first row
        getYandXFromVectors(row, frame.frameSchema, yColumnName, xColumnNames)
      }
      case DataTypes.float64 => {
        getYandXFromRows(frame, yColumnName, xColumnNames)
      }
      case _ => throw new IllegalArgumentException("Unsupported column type.  Time series and exogenous values must be either vectors or float64 data types.")
    }

    val yVector = new DenseVector(yValues)
    val xMatrix = new DenseMatrix(rows = yValues.length, cols = xColumnNames.size, data = xValues.transpose.flatten)

    return (yVector, xMatrix)
  }
}
