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
package org.trustedanalytics.atk.engine.daal.plugins

import com.intel.daal.data_management.data.NumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.mutable.ListBuffer
import java.nio.DoubleBuffer

/**
 * DAAL numeric table with index
 *
 * The table index is unique and allows each task to know the global offset of the
 * local table. The index allows the master task to establish a global order when
 * partial results.
 *
 * @param index Table index
 * @param table DAAL numeric table
 */
case class IndexedNumericTable(index: Long, private val table: NumericTable) extends Serializable {
  require(table != null, "Numeric table must not be null")
  val numRows: Int = table.getNumberOfRows.toInt
  val numCols: Int = table.getNumberOfColumns.toInt
  table.pack() //serialize table

  /**
   * Deserialize numeric table
   *
   * @param context DAAL context
   * @return deserialized table
   */
  def getUnpackedTable(context: DaalContext): NumericTable = {
    table.unpack(context)
    table
  }

  /**
   * Get DAAL table index and serialized numeric table
   *
   * @return table index and serialized numeric table
   */
  def getIndexTablePair: (Long, NumericTable) = (index, table)

  /**
   * Convert DAAL numeric table into iterator of Spark SQL rows
   *
   * @param context DAAL context
   * @return Row iterator
   */
  def toRowIter(context: DaalContext): Iterator[Row] = {
    if (isEmpty) return List.empty[sql.Row].iterator

    val unpackedTable = getUnpackedTable(context)
    val buffer = DoubleBuffer.allocate(numRows * numCols)
    val doubleBuffer = unpackedTable.getBlockOfRows(0, numRows, buffer)
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

  /**
   * Check if table is empty
   */
  def isEmpty: Boolean = numRows < 1
}
