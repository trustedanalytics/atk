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

package org.trustedanalytics.atk.engine.daal.plugins

import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Distributed DAAL numeric table with features and labels
 */
class DistributedLabeledTable extends Serializable {
  private var tableRdd: RDD[IndexedLabeledTable] = null
  private var numRows: Long = 0L
  private var numFeatureCols: Long = 0L
  private var numLabelCols: Long = 0L

  /**
   * Create distributed numeric table with features and labels from Vector RDD
   *
   * @param vectorRdd Vector RDD
   * @param splitIndex Column index at which to split vector into features and labels
   * @param maxRowsPerTable  Max number of rows in each numeric table. If this is non-positive
   *                         then all rows in a partition are transformed into a single numeric table
   */
  def this(vectorRdd: RDD[Vector], splitIndex: Int, maxRowsPerTable: Int = -1) = {
    this()
    val first: Vector = vectorRdd.first()

    val numCols = first.size
    numFeatureCols = splitIndex
    numLabelCols = numCols - splitIndex

    tableRdd = vectorRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val context = new DaalContext
        var tableRows = 0L
        val featureBuf = new ArrayBuffer[Double]()
        val labelBuf = new ArrayBuffer[Double]()
        val tables = new ArrayBuffer[IndexedLabeledTable]()

        while (iter.hasNext) {
          val array = iter.next().toArray
          featureBuf ++= array.slice(0, splitIndex)
          labelBuf ++= array.slice(splitIndex, numCols)
          tableRows += 1

          //partition can be split into multiple numeric tables if maximum rows per table is set
          if (tableRows == maxRowsPerTable || !iter.hasNext) {
            val tableIndex = i - tableRows + 1
            val featureTable = new IndexedNumericTable(tableIndex,
              new HomogenNumericTable(context, featureBuf.toArray, numFeatureCols, tableRows))
            val labelTable = new IndexedNumericTable(tableIndex,
              new HomogenNumericTable(context, labelBuf.toArray, numLabelCols, tableRows))

            tables += IndexedLabeledTable(featureTable, labelTable)
            numRows += tableRows
            tableRows = 0
            featureBuf.clear()
            labelBuf.clear()
          }
        }
        tables.toIterator
    }
  }

  /**
   * Create distributed numeric table with features and labels from frame
   *
   * @param frameRdd Input frame
   * @param featureColumns List of feature columns
   * @param labelColums List of label columns
   * @param maxRowsPerTable  Max number of rows in each numeric table. If this is non-positive
   *                         then all rows in a partition are transformed into a single numeric table
   */
  def this(frameRdd: FrameRdd, featureColumns: List[String], labelColums: List[String], maxRowsPerTable: Int = -1) = {
    this(frameRdd.toDenseVectorRDD(featureColumns ++ labelColums), featureColumns.size, maxRowsPerTable)
  }

  /**
   * Get number of rows in distributed table
   */
  def getNumRows: Long = numRows

  /**
   * Get number of feature columns in distributed table
   */
  def getNumFeatureCols: Long = numFeatureCols

  /**
   * Get number of label columns in distributed table
   */
  def getNumLabelCols: Long = numLabelCols

  /**
   * Get Pair RDD of feature and label numeric tables
   */
  def rdd: RDD[(IndexedNumericTable, IndexedNumericTable)] = {
    tableRdd.map(table => (table.features, table.labels))
  }
}
