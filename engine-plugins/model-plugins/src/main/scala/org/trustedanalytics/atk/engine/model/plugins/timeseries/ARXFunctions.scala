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
    // Verify that it's a vector column
    schema.requireColumnIsType(columnName, DataTypes.isVectorDataType)

    // Return the vector length
    schema.columnDataType(columnName).asInstanceOf[DataTypes.vector].length
  }

  /**
   * Gets values from the specified y and x columns.
   * @param frame Frame to get values from
   * @param yColumnName Name of the y column
   * @param xColumnNames Name of the x columns
   * @return Array of y values, and 2-dimensional array of x values
   */
  private def getYandXFromRows(frame: FrameRdd, yColumnName: String, xColumnNames: List[String]): (Array[Double], Array[Array[Double]]) = {
    val schema = frame.frameSchema

    schema.requireColumnIsNumerical(yColumnName)
    xColumnNames.foreach((xColumn: String) => schema.requireColumnIsNumerical(xColumn))

    val totalRowCount = frame.count.toInt
    val yValues = new Array[Double](totalRowCount)
    val xValues = Array.ofDim[Double](totalRowCount, xColumnNames.size)
    var rowCounter = 0

    for (row <- frame.collect()) {
      yValues(rowCounter) = row.get(schema.columnIndex(yColumnName)).toString.toDouble

      var xColumnCounter = 0
      for (xColumn <- xColumnNames) {
        xValues(rowCounter)(xColumnCounter) = row.get(schema.columnIndex(xColumn)).toString.toDouble
        xColumnCounter += 1
      }

      rowCounter += 1
    }

    (yValues, xValues)

  }

  /**
   * Gets x and y values from the specified frame
   * @param frame  Frame to get values from
   * @param yColumnName Name of the column that has y values
   * @param xColumnNames Name of the columns that have x values
   * @return Vector of y values and Matrix of x values
   */
  def getYandXFromFrame(frame: FrameRdd, yColumnName: String, xColumnNames: List[String]): (Vector[Double], Matrix[Double]) = {

    // Get values in arrays
    val (yValues, xValues) = getYandXFromRows(frame, yColumnName, xColumnNames)

    // Put values into a vector and matrix to return
    val yVector = new DenseVector(yValues)
    val xMatrix = new DenseMatrix(rows = yValues.length, cols = xColumnNames.size, data = xValues.transpose.flatten)

    (yVector, xMatrix)
  }
}
