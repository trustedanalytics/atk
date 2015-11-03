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

package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FlattenColumnArgs
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import java.util.regex.Pattern

/**
 * Take a row with multiple values in a column and 'flatten' it into multiple rows.
 *
 */
@PluginDoc(oneLine = "Spread data to multiple rows based on cell data.",
  extended = """Splits cells in the specified columns into multiple rows according to a string
delimiter.
New rows are a full copy of the original row, but the specified columns only
contain one value.
The original row is deleted.""")
class FlattenColumnPlugin extends SparkCommandPlugin[FlattenColumnArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/flatten_columns"

  override def numberOfJobs(arguments: FlattenColumnArgs)(implicit invocation: Invocation): Int = 2

  /**
   * Take a row with multiple values in a column and 'flatten' it into multiple rows.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments input specification for column flattening
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FlattenColumnArgs)(implicit invocation: Invocation): UnitReturn = {
    // validate arguments
    val frame: SparkFrame = arguments.frame
    var schema = frame.schema
    var flattener: RDD[Row] => RDD[Row] = null
    val columnIndices = arguments.columns.map(c => schema.columnIndex(c))
    val columnDataTypes = arguments.columns.map(c => schema.columnDataType(c))
    var delimiters = arguments.defaultDelimiters

    var stringDelimiterCount = 0 // counter used to track delimiters for string columns

    for (i <- arguments.columns.indices) {
      columnDataTypes(i) match {
        case DataTypes.vector(length) =>
          schema = schema.convertType(arguments.columns(i), DataTypes.float64)
        case DataTypes.string =>
          if (arguments.delimiters.isDefined && arguments.delimiters.get.size > 1) {
            if (arguments.delimiters.get.size > stringDelimiterCount) {
              delimiters(i) = arguments.delimiters.get(stringDelimiterCount)
              stringDelimiterCount += 1
            }
            else
              throw new IllegalArgumentException(s"The number of delimiters provided is less than the number of string columns being flattened.")
          }
        case _ =>
          val illegalDataType = columnDataTypes(i).toString
          throw new IllegalArgumentException(s"Invalid column ('${arguments.columns(i)}') data type provided: ${illegalDataType}. Only string or vector columns can be flattened.")
      }
    }

    if (stringDelimiterCount > 0 && stringDelimiterCount < arguments.delimiters.get.size)
      throw new IllegalArgumentException(s"The number of delimiters provided is more than the number of string columns being flattened.")

    flattener = FlattenColumnFunctions.flattenRddByColumnIndices(columnIndices, columnDataTypes, delimiters.toList)

    // run the operation
    val flattenedRDD = flattener(frame.rdd)

    // save results
    frame.save(new FrameRdd(schema, flattenedRDD))
  }

}

/**
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
object FlattenColumnFunctions extends Serializable {

  /**
   * Flatten RDD by the column with specified column indices
   * @param indices column indices
   * @param dataTypes column dataTypes
   * @param delimiters separators for splitting string columns
   * @param rdd RDD for flattening
   * @return new RDD with columns flattened
   */
  def flattenRddByColumnIndices(indices: List[Int],
                                dataTypes: List[DataType],
                                delimiters: List[String] = null)(rdd: RDD[Row]): RDD[Row] = {
    val flattener = flattenRowByColumnIndices(indices, dataTypes, delimiters)_
    rdd.flatMap(row => flattener(row))
  }

  /**
   * When flattening multiple columns, there could be a case where one field has more values after
   * the flattening than another field in the same row.  We needs to fill the extra spots in the
   * column that has less values.  This is a helper function to return the value that we're filling
   * in the extra spots, depending on the column's data type.  The columns should be either strings
   * or vectors, since those are the types of columns that current support flattening.
   * @param dataType column data type
   * @return missing value
   */
  private def getMissingValue(dataType: DataType): Any = {
    if (dataType == DataTypes.string) {
      null
    }
    else {
      0.0
    }
  }

  /**
   * flatten a row by the column with specified column indices.  Columns must be a string or vector.
   * @param indices column indices
   * @param dataTypes column data types
   * @param delimiters separators for splitting string columns
   * @param row row data
   * @return flattened out row/rows
   */
  private[frame] def flattenRowByColumnIndices(indices: List[Int],
                                               dataTypes: List[DataType],
                                               delimiters: List[String])(row: Row): Array[Row] = {
    val rowBuffer = new scala.collection.mutable.ArrayBuffer[Row]()
    for (i <- indices.indices) {
      val columnIndex = indices(i)

      dataTypes(i) match {
        case DataTypes.string =>
          val delimiter = if (delimiters != null && delimiters(i) != "") delimiters(i) else ","
          val splitItems = row(columnIndex).asInstanceOf[String].split(Pattern.quote(delimiter))

          if (splitItems.length > 1) {
            // Loop through items being split from the string
            for (rowIndex <- splitItems.indices) {
              val isNewRow = rowBuffer.length <= rowIndex
              val r = if (isNewRow) row.toSeq.toArray.clone() else rowBuffer(rowIndex).toSeq.toArray.clone()

              r(columnIndex) = splitItems(rowIndex)

              if (isNewRow) {
                for (tempColIndex <- indices.indices) {
                  if (tempColIndex != i) {
                    r(indices(tempColIndex)) = getMissingValue(dataTypes(tempColIndex))
                  }
                }

                rowBuffer += Row.fromSeq(r)
              }
              else
                rowBuffer(rowIndex) = Row.fromSeq(r)
            }
          }
          else {
            // There's nothing to split, just update first row in the rowBuffer
            if (rowBuffer.length == 0)
              rowBuffer += row
            else {
              val r = rowBuffer(0).toSeq.toArray.clone()
              r(columnIndex) = splitItems(0)
              rowBuffer(0) = Row.fromSeq(r)
            }
          }
        case DataTypes.vector(length) =>
          val vectorItems = DataTypes.toVector(length)(row(columnIndex)).toArray
          // Loop through items in the vector
          for (vectorIndex <- vectorItems.indices) {
            val isNewRow = rowBuffer.length <= vectorIndex
            val r = if (isNewRow) row.toSeq.toArray.clone() else rowBuffer(vectorIndex).toSeq.toArray.clone()
            // Set vector item in the column being flattened
            r(columnIndex) = vectorItems(vectorIndex)
            if (isNewRow) {
              // Empty out other columns that are being flattened in the new row
              for (tempColIndex <- indices.indices) {
                if (tempColIndex != i) {
                  r(indices(tempColIndex)) = getMissingValue(dataTypes(tempColIndex))
                }
              }
              // Add new row to the rowBuffer
              rowBuffer += Row.fromSeq(r)
            }
            else
              rowBuffer(vectorIndex) = Row.fromSeq(r)
          }
        case _ =>
          throw new IllegalArgumentException("Flatten column does not support type: " + dataTypes(i).toString)
      }

    }

    return rowBuffer.toArray
  }
}
