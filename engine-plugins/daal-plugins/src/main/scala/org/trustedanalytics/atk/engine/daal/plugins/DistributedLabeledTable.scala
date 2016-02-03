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
package org.trustedanalytics.atk.engine.daal.plugins

import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class DistributedLabeledTable extends Serializable {
  private var tableRdd: RDD[(NumericTableWithIndex, NumericTableWithIndex)] = null
  private var numRows: Long = 0L
  private var numFeatureCols: Int = 0
  private var numLabelCols: Int = 0

  def this(vectorRdd: RDD[Vector], splitIndex: Int, maxRowsPerTable: Int = -1) = {
    this()
    val first: Vector = vectorRdd.first()

    val numCols = first.size
    numFeatureCols = splitIndex
    numLabelCols = numCols - splitIndex

    tableRdd = vectorRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val context = new DaalContext
        var tableRows = 0
        val featureBuf = new ArrayBuffer[Double]()
        val labelBuf = new ArrayBuffer[Double]()
        val tables = new ArrayBuffer[(NumericTableWithIndex, NumericTableWithIndex)]()

        while (iter.hasNext) {
          val array = iter.next().toArray
          featureBuf ++= array.slice(0, splitIndex)
          labelBuf ++= array.slice(splitIndex, numCols)
          tableRows += 1

          if (tableRows == maxRowsPerTable || !iter.hasNext) {
            val tableIndex = i - tableRows + 1
            val featureTable = new NumericTableWithIndex(tableIndex,
              new HomogenNumericTable(context, featureBuf.toArray, numFeatureCols, tableRows))
            val labelTable = new NumericTableWithIndex(tableIndex,
              new HomogenNumericTable(context, labelBuf.toArray, numLabelCols, tableRows))

            tables += ((featureTable, labelTable))
            numRows += tableRows
            tableRows = 0
            featureBuf.clear()
            labelBuf.clear()
          }
        }
        tables.toIterator
    }
  }

  def this(frameRdd: FrameRdd, featureColumns: List[String], labelColums: List[String]) = {
    this(frameRdd.toDenseVectorRDD(featureColumns ++ labelColums), featureColumns.size)
  }

  def rdd: RDD[(NumericTableWithIndex, NumericTableWithIndex)] = tableRdd
}
