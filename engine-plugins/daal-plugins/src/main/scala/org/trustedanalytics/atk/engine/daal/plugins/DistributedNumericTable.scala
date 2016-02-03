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

class DistributedNumericTable extends Serializable {
  private var tableRdd: RDD[NumericTableWithIndex] = null
  private var numRows: Long = 0L
  private var numCols: Int = 0

  def this(vectorRdd: RDD[Vector], maxRowsPerTable: Int = -1) = {
    this()
    val first: Vector = vectorRdd.first()

    numCols = first.size
    tableRdd = vectorRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        val daalContext = new DaalContext
        var tableRows = 0
        val buf = new ArrayBuffer[Double]()
        val tables = new ArrayBuffer[NumericTableWithIndex]()

        while (iter.hasNext) {
          val vector = iter.next()
          buf ++= vector.toArray
          tableRows += 1

          if (tableRows == maxRowsPerTable || !iter.hasNext) {
            val tableIndex = i - tableRows + 1
            val table = new HomogenNumericTable(daalContext, buf.toArray, numCols, tableRows)
            tables += new NumericTableWithIndex(tableIndex, table)
            numRows += tableRows
            tableRows = 0
            buf.clear()
          }
        }
        tables.toIterator
    }
  }

  def this(frameRdd: FrameRdd, columnNames: List[String]) = {
    this(frameRdd.toDenseVectorRDD(columnNames))
  }

  def rdd: RDD[NumericTableWithIndex] = tableRdd
}
