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

import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.trustedanalytics.atk.domain.schema.FrameSchema

import scala.collection.mutable.ArrayBuffer

/**
 * Distributed DAAL indexed numeric table
 */
class DistributedNumericTable extends Serializable {
  private var tableRdd: RDD[IndexedNumericTable] = null
  private var numRows: Long = 0L
  private var numCols: Long = 0L

  /**
   * Create distributed numeric table from RDD of indexed numeric table
   *
   * @param rdd RDD of indexed numeric table
   */
  def this(rdd: RDD[IndexedNumericTable]) = {
    this()
    tableRdd = rdd

    if (tableRdd != null) {
      numCols = tableRdd.first().numCols
      tableRdd.foreach(table => numRows += table.numRows)
    }
  }

  /**
   * Create distributed numeric table from Vector RDD
   *
   * @param vectorRdd Vector RDD
   * @param maxRowsPerTable  Max number of rows in each numeric table. If this is non-positive
   *                         then all rows in a partition are transformed into a single numeric table
   */
  def this(vectorRdd: RDD[Vector], maxRowsPerTable: Int = -1) = {
    this()
    val first: Vector = vectorRdd.first()

    numCols = first.size

    tableRdd = vectorRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val context = new DaalContext
        var tableRows = 0L
        var tableSize = 0L
        val buf = new ArrayBuffer[Double]()
        val tables = new ArrayBuffer[IndexedNumericTable]()

        while (iter.hasNext) {
          val vector = iter.next()
          buf ++= vector.toArray
          tableRows += 1
          tableSize += vector.size

          if (tableRows == maxRowsPerTable || !iter.hasNext) {
            val tableIndex = i - tableRows + 1
            val table = new HomogenNumericTable(context, buf.toArray, tableSize / tableRows, tableRows)
            tables += new IndexedNumericTable(tableIndex, table)
            numRows += tableRows
            tableRows = 0L
            tableSize = 0L
            buf.clear()
          }
        }
        tables.toIterator
    }
  }

  /**
   *  Create distributed numeric table using subset of columns from frame
   *
   * @param frameRdd Input frame
   * @param columnNames List of columns for creating numeric table
   * @param maxRowsPerTable  Max number of rows in each numeric table. If this is non-positive
   *                         then all rows in a partition are transformed into a single numeric table
   */
  def this(frameRdd: FrameRdd, columnNames: List[String], maxRowsPerTable: Int = -1) = {
    this(frameRdd.toDenseVectorRDD(columnNames), maxRowsPerTable)
  }

  /**
   * Get number of rows in distributed table
   */
  def getNumRows: Long = numRows

  /**
   * Get number of columns in distributed table
   */
  def getNumCols: Long = numCols

  /**
   * Get RDD of indexed numeric table
   */
  def rdd: RDD[IndexedNumericTable] = tableRdd

  /**
   * Cache distributed table in memory.
   */
  def cache(): Unit = {
    tableRdd.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Unpersist cached distributed table.
   */
  def unpersist(): Unit = {
    tableRdd.unpersist()
  }

  /**
   * Convert table to frame RDD
   *
   * @param schema Frame schema
   * @return Frame RDD
   */
  def toFrameRdd(schema: FrameSchema): FrameRdd = {
    val rowRdd = tableRdd.flatMap(table => {
      val context = new DaalContext
      val rows = table.toRowIter(context)
      context.dispose()
      rows
    })
    new FrameRdd(schema, rowRdd)
  }
}
