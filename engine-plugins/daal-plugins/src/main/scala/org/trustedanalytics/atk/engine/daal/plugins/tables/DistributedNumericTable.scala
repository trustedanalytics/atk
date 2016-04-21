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
import org.trustedanalytics.atk.domain.schema.FrameSchema
import scala.collection.mutable.ArrayBuffer

/**
 * Distributed DAAL indexed numeric table
 *
 * @param tableRdd RDD of indexed numeric table
 * @param numRows Number of rows in table
 */
case class DistributedNumericTable(tableRdd: RDD[IndexedNumericTable],
                                   numRows: Long) extends DistributedTable(tableRdd, numRows) {
  require(tableRdd != null, "DAAL numeric table RDD must not be null")
  val numCols: Long = tableRdd.first().numCols

  /**
   * Convert table to frame RDD
   *
   * @param schema Frame schema
   * @return Frame RDD
   */
  def toFrameRdd(schema: FrameSchema): FrameRdd = {
    val rowRdd = tableRdd.flatMap(table => {
      val context = new DaalContext
      val rows = table.toRowIter(context, Some(schema))
      context.dispose()
      rows
    })
    new FrameRdd(schema, rowRdd)
  }
}

object DistributedNumericTable {

  /**
   * Create distributed numeric table from Vector RDD
   *
   * @param vectorRdd Vector RDD
   * @return distributed numeric table
   */
  def createTable(vectorRdd: RDD[Vector]): DistributedNumericTable = {

    val tableRdd = vectorRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val context = new DaalContext
        var numRows = 0L
        var numElements = 0L
        val buf = new ArrayBuffer[Double]()

        while (iter.hasNext) {
          val vector = iter.next()
          numElements += vector.size
          buf ++= vector.toArray
          numRows += 1
        }

        val table = new HomogenNumericTable(context, buf.toArray, numElements / numRows, numRows)
        val indexedTable = new IndexedNumericTable(i, table)
        buf.clear()
        context.dispose()
        Array(indexedTable).toIterator
    }
    val totalRows = tableRdd.map(table => table.numRows).sum().toLong
    DistributedNumericTable(tableRdd, totalRows)
  }

  /**
   *  Create distributed numeric table using subset of columns from frame
   *
   * @param frameRdd Input frame
   * @param columnNames List of columns for creating numeric table
   * @return distributed numeric table
   */
  def createTable(frameRdd: FrameRdd, columnNames: List[String]): DistributedNumericTable = {
    createTable(frameRdd.toDenseVectorRDD(columnNames))
  }
}