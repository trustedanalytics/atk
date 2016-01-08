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
package org.trustedanalytics.atk.engine.daal.plugins.conversions

import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.data_management.data_source.StringDataSource
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Functions for extending frames with DAAL-related methods.
 * <p>
 * This is best used by importing DaalPluginImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */
class DaalFrameRddFunctions(self: FrameRdd) extends Serializable {
  import DaalFrameRddFunctions._
  /**
   * Convert ATK data frame into DAAL homogeneous numeric table
   *
   * @param columnNames Column names to select
   * @return DAAL homogeneous numeric table
   */
  def toNumericTableRdd(columnNames: List[String]): RDD[(Integer, HomogenNumericTable)] = {
    val numericTableRdd = self.mapPartitionsWithIndex {
      case (i, iter) =>
        // Convert rows in a Spark partition into a single comma-delimited string
        convertRowsToNumericTable(self.rowWrapper, columnNames, iter) match {
          case Some(dataTable) => List((new Integer(i), dataTable)).toIterator
          case _ => List.empty[(Integer, HomogenNumericTable)].iterator
        }
    }
    numericTableRdd
  }

}

object DaalFrameRddFunctions extends Serializable {
  /**
   * Convert row iterator to DAAL homogeneous numeric table
   *
   * @param columnNames Column names to select
   * @param iter Row iterator
   * @return Optional numeric table if iterator is not empty
   */
  def convertRowsToNumericTable(rowWrapper: RowWrapper, columnNames: List[String],
                                iter: Iterator[Row]): Option[HomogenNumericTable] = {
    if (iter.isEmpty) return None

    var numRows = 0
    val stringBuilder = new StringBuilder()
    while (iter.hasNext) {
      val row = rowWrapper(iter.next()).valuesAsArray(columnNames)
      stringBuilder ++= row.mkString(",") + "\n"
      numRows += 1
    }

    // Use DAAL's String data source to convert comma-delimited string into a DAAL homogeneous numeric table
    val context = new DaalContext()
    val stringDataSource = new StringDataSource(context, "")
    stringDataSource.setData(stringBuilder.toString())
    stringDataSource.createDictionaryFromContext()
    stringDataSource.allocateNumericTable
    stringDataSource.loadDataBlock(numRows)

    val dataTable = stringDataSource.getNumericTable().asInstanceOf[HomogenNumericTable]
    dataTable.pack()
    context.dispose()
    Some(dataTable)
  }
}