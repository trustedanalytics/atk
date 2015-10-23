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

import java.nio.DoubleBuffer

import com.intel.daal.data_management.data.{ HomogenNumericTable, NumericTable }
import com.intel.daal.data_management.data_source.StringDataSource
import com.intel.daal.services.DaalContext
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.engine.plugin.Invocation

object DaalDataConverters extends Serializable {
  /**
   * Convert results of PCA algorithm into ATK data frame
   *
   * @param sparkContext sparkContext
   * @param eigenVectorTable Eigen vector table
   * @param eigenValueTable Eigen value table
   * @return ATK data frame with PCA eigen vectors, and eigen values
   */
  def convertPcaResultsToFrame(daalContext: DaalContext,
                               eigenVectorTable: HomogenNumericTable,
                               eigenValueTable: HomogenNumericTable): Seq[Row] = {
    val eigenVectors = createArrayOfVectors(daalContext, eigenVectorTable)
    val eigenValues = createArrayOfVectors(daalContext, eigenValueTable)

    val results: Seq[Row] = for {
      i <- 0 until eigenVectors.length
      eigenVector = eigenVectors(i).asInstanceOf[Any]
      eigenValue = eigenValues(0)(i).asInstanceOf[Any]
    } yield new GenericRow(Array(eigenVector, eigenValue))

    results
  }

  /**
   * Convert ATK data frame into DAAL homogeneous numeric table
   *
   * @param frameRdd ATK data frame
   * @param columnNames Column names to select
   * @return DAAL homogeneous numeric table
   */
  def convertFrameToNumericTableRdd(frameRdd: FrameRdd,
                                    columnNames: List[String]): RDD[(Integer, HomogenNumericTable)] = {
    val rowWrapper = frameRdd.rowWrapper
    val numericTableRdd = frameRdd.mapPartitionsWithIndex {
      case (i, iter) =>
        // Convert rows in a Spark partition into a single comma-delimited string
        convertRowsToNumericTable(columnNames, rowWrapper, iter) match {
          case Some(dataTable) => List((new Integer(i), dataTable)).toIterator
          case _ => List.empty[(Integer, HomogenNumericTable)].iterator
        }
    }
    numericTableRdd
  }

  /**
   * Convert row iterator to DAAL homogeneous numeric table
   *
   * @param columnNames Column names to select
   * @param rowWrapper Row wrapper with helper methods
   * @param iter Row iterator
   * @return Optional numeric table if iterator is not empty
   */
  def convertRowsToNumericTable(columnNames: List[String],
                                rowWrapper: RowWrapper,
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

  /**
   * Convert DAAL numeric table into an array of vectors
   *
   * Each feature vector (row) in the numeric table is converted into a vector of doubles
   *
   * @param numericTable DAAL homogeneous numeric table
   * @return Array of vectors
   */
  def createArrayOfVectors(daalContext: DaalContext, numericTable: NumericTable): Array[Vector[Double]] = {
    numericTable.unpack(daalContext)
    val numRows = numericTable.getNumberOfRows().toInt
    val numCols = numericTable.getNumberOfColumns().toInt

    val result = DoubleBuffer.allocate(numRows * numCols)
    val doubleBuffer = numericTable.getBlockOfRows(0, numRows, result)

    val dataArray = new Array[Vector[Double]](numRows)
    var resultIndex = 0
    for (i <- 0 until numRows) {
      val rowArray = new Array[Double](numCols)
      for (j <- 0 until numCols) {
        rowArray(j) = doubleBuffer.get(i * numCols + j)
        resultIndex += 1
      }
      dataArray(i) = DataTypes.toVector(numCols)(rowArray)
    }
    dataArray
  }

}
