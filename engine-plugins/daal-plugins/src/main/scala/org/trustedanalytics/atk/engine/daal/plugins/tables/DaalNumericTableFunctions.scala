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

package org.trustedanalytics.atk.engine.daal.plugins.tables

import java.nio.DoubleBuffer
import java.util.Arrays
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils.withDaalContext
import org.apache.spark.mllib.linalg.{ Matrices, Matrix }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import com.intel.daal.data_management.data.NumericTable
import com.intel.daal.services.DaalContext
import scala.collection.mutable.ListBuffer

class DaalNumericTableFunctions(self: NumericTable) extends Serializable {
  private val numRows = self.getNumberOfRows.toInt
  private val numCols = self.getNumberOfColumns.toInt

  /**
   * Convert DAAL numeric table into an array of array of doubles
   *
   * Each feature vector (row) in the numeric table is converted into an array of doubles
   *
   * @return Array of array of doubles
   */
  def toArrayOfDoubleArray(): Array[Array[Double]] = {
    val arrays = new Array[Array[Double]](numRows)

    withDaalContext { context =>
      self.unpack(context)
      val buffer = DoubleBuffer.allocate(numRows * numCols)
      val doubleBuffer = self.getBlockOfRows(0, numRows, buffer)

      for (i <- 0 until numRows) {
        val rowArray = new Array[Double](numCols)
        for (j <- 0 until numCols) {
          rowArray(j) = doubleBuffer.get(i * numCols + j)
        }
        arrays(i) = rowArray
      }
    }.elseError("Could not convert numeric table to array")

    arrays
  }

  /**
   * Convert DAAL numeric table into a column-major dense matrix.
   *
   * Transposes the rows and columns so that each column in the numeric table
   * is converted into an array of doubles
   *
   * @param k Subset of columns to return, Defaults to number of columns in table.
   *
   * @return Column-major dense matrix
   */
  def toMatrix(k: Int = numCols): Matrix = {
    require(k > 0 && k <= numCols, s"k must be smaller than the number of observation columns: ${numCols}")
    Matrices.dense(numRows, k, Arrays.copyOfRange(toDoubleArray(), 0, numRows * k))
  }

  /**
   * Convert DAAL numeric table into an array of vectors
   *
   * Each feature vector (row) in the numeric table is converted into an array of doubles
   *
   * @return Array of doubles
   */
  def toDoubleArray(): Array[Double] = {
    toArrayOfDoubleArray().flatten
  }

  /**
   * Convert DAAL numeric table into iterator of Spark SQL rows
   *
   * @param context Daal context
   * @return Iterator of rows
   */
  def toRowIter(context: DaalContext): Iterator[Row] = {
    val buffer = DoubleBuffer.allocate(numRows * numCols)
    val doubleBuffer = self.getBlockOfRows(0, numRows, buffer)
    val rowBuffer = new ListBuffer[Row]()

    for (i <- 0 until numRows) {
      val rowArray = new Array[Any](numCols)
      for (j <- 0 until numCols) {
        rowArray(j) = doubleBuffer.get(i * numCols + j)
      }
      rowBuffer += new GenericRow(rowArray)
    }

    rowBuffer.iterator
  }

}
