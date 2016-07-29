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

package org.trustedanalytics.atk.engine.frame.plugins.timeseries

import java.io.Serializable
import java.sql.Timestamp
import java.time.ZonedDateTime

import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, VectorFunctions, RowWrapper }
import org.apache.spark.frame.FrameRdd
import com.cloudera.sparkts._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.apache.spark.mllib.linalg.{ Vector => SparkVector, DenseVector => SparkDenseVector, Matrix => SparkMatrix, DenseMatrix => SparkDenseMatrix }

/**
 * Object contains utility functions for working with time series
 */
object TimeSeriesFunctions extends Serializable {

  // Spark SQL UDF for converting our datetime column (which is string based) to a Timestamp datatype
  val toTimestamp: UserDefinedFunction = udf((t: String) => Timestamp.from(ZonedDateTime.parse(t).toInstant))

  /**
   * Creates a FrameRdd for the specified TimeSeriesRdd
   *
   * @param timeseriesRdd TimeSeriesRDD
   * @param keyColumn Name of the key column
   * @param valueColumn Name of column that contains a series of values for each key
   * @return FrameRdd
   */
  def createFrameRdd(timeseriesRdd: TimeSeriesRDD[String], keyColumn: String, valueColumn: String): FrameRdd = {
    // Create frame schema
    val timeseriesSchema = FrameSchema(List(Column(keyColumn, DataTypes.string), Column(valueColumn, DataTypes.vector(timeseriesRdd.index.size))))

    // Map the column of values so that it uses a Scala Vector rather than a Spark DenseVector.
    val withVector = timeseriesRdd.map(row => {
      val originalColumns = row.productIterator.toList
      val newVectorCol = originalColumns(1).asInstanceOf[SparkDenseVector].toArray.iterator.toVector
      Array[Any](originalColumns(0), newVectorCol)
    })

    // Create FrameRdd to return
    FrameRdd.toFrameRdd(timeseriesSchema, withVector)
  }

  /**
   * Creates a DateTimeIndex from the ist of Date/Times
   * @param dateTimeStrings List of Date/Times
   * @return DateTimeIndex
   */
  def createDateTimeIndex(dateTimeStrings: List[DateTime]): DateTimeIndex = {
    // Create DateTimeIndex after parsing the strings as ZonedDateTime
    DateTimeIndex.irregular(dateTimeStrings.map(dt => parseZonedDateTime(dt)).toArray)
  }

  /**
   * Parses the DateTime as a ZonedDateTime
   * @param dateTime Date/time
   * @return ZonedDateTime
   */
  def parseZonedDateTime(dateTime: DateTime): ZonedDateTime = {
    ZonedDateTime.parse(dateTime.toString(ISODateTimeFormat.dateTime))
  }

  /**
   * Discovers the names of the column that contain the string key and vector of time series values.  The schema
   * provided is expected to be for a time series frame, where we just have 2 columns: (1) String column that contains
   * the key, and (2) Vector column tha contains the time series values.  If these exact columns are not found,
   * exceptions are thrown.
   * @param schema Schema for a time series frame
   * @return Name of the key and value columns
   */
  def discoverKeyAndValueColumns(schema: Schema): (String, String) = {
    var keyColumn = ""
    var valueColumn = ""

    if (schema.columns.size != 2)
      throw new RuntimeException("Frame has unsupported number of columns.  Time series frames are only expected to have 2 columns -- a string column (key) and a vector column (series values).")

    // Get key and series column names.
    // The frame should have just one string column for key and one vector column that has the time series values.
    for (column <- schema.columns) {
      val columnName = column.name

      column.dataType match {
        case DataTypes.string => {
          if (keyColumn.isEmpty) {
            keyColumn = columnName
          }
          else {
            // Found two string columns
            throw new RuntimeException(s"Frame has more than one string column ('$columnName' and '$keyColumn').  Time series frames should only have one string key column.")
          }
        }
        case DataTypes.vector(length) => {
          if (valueColumn.isEmpty) {
            valueColumn = columnName
          }
          else {
            // Found two vector columns
            throw new RuntimeException(s"Frame has more than one vector column('$columnName' and '$valueColumn'). Time series frames should only have one vector column, which contains the series values.")
          }
        }
        case _ => {
          throw new RuntimeException(s"Frame has unsupported column type (${column.dataType.getClass.toString}.  Time series frames are only expected to have a string column (key) and a vector column (series values).")
        }
      }
    }

    (keyColumn, valueColumn)
  }

  /**
   * Creates a TimeSeriesRDD from the specified SparkFrame, with the DateTimeIndex provided
   * @param keyColumn Name of the key colum
   * @param valueColumn Name of the value column
   * @param frame SparkFrame to use to create the TimeSeries RDD.  This frame should already be formatted
   *              as a time series.
   * @param dateTimeIndex DateTime index for the time series
   * @return TimeSeriesRDD
   */
  def createTimeSeriesRDD(keyColumn: String, valueColumn: String, frame: SparkFrame, dateTimeIndex: DateTimeIndex): TimeSeriesRDD[String] = {
    if (dateTimeIndex == null)
      throw new IllegalArgumentException("DateTimeIndex is required for creating a TimeSeriesRDD.")

    // Create TimeSeriesRDD
    val rdd = frame.rdd.mapRows(row => {
      val key = row.stringValue(keyColumn)
      val series = row.vectorValue(valueColumn)
      val vector = new SparkDenseVector(series.toArray)
      (key.asInstanceOf[String], vector.asInstanceOf[SparkVector])
    })

    new TimeSeriesRDD[String](dateTimeIndex, rdd)
  }

  /**
   * Gets values from the specified y and x columns.
   * @param frame Frame to get values from
   * @param yColumnName Name of the y column
   * @param xColumnNames Name of the x columns
   * @return Array of y values, and 2-dimensional array of x values
   */
  private def getYAndXFromRows(frame: FrameRdd, yColumnName: String, xColumnNames: Seq[String]): (Array[Double], Array[Array[Double]]) = {
    val schema = frame.frameSchema

    schema.requireColumnIsNumerical(yColumnName)
    xColumnNames.foreach((xColumn: String) => schema.requireColumnIsNumerical(xColumn))

    val totalRowCount = frame.count.toInt
    val yValues = new Array[Double](totalRowCount)
    val xValues = Array.ofDim[Double](totalRowCount, xColumnNames.size)
    var rowCounter = 0
    val yColumnIndex = schema.columnIndex(yColumnName)

    for (row <- frame.collect()) {
      yValues(rowCounter) = DataTypes.toDouble(row.get(yColumnIndex))

      var xColumnCounter = 0
      for (xColumn <- xColumnNames) {
        xValues(rowCounter)(xColumnCounter) = DataTypes.toDouble(row.get(schema.columnIndex(xColumn)))
        xColumnCounter += 1
      }

      rowCounter += 1
    }

    (yValues, xValues)

  }

  /**
   * Gets x (spark matrix) and y (spark matrix) values from the specified frame
   * @param frame  Frame to get values from
   * @param yColumnName Name of the column that has y values
   * @param xColumnNames Name of the columns that have x values
   * @return Spark Vector of y values and Spark Matrix of x values
   */
  def getSparkVectorYAndXFromFrame(frame: FrameRdd, yColumnName: String, xColumnNames: Seq[String]): (SparkVector, SparkMatrix) = {

    // Get values in arrays
    val (yValues, xValues) = getYAndXFromRows(frame, yColumnName, xColumnNames)

    // Put values into a vector and matrix to return
    val yVector = new SparkDenseVector(yValues)
    val xMatrix = new SparkDenseMatrix(numRows = yValues.length, numCols = xColumnNames.size, values = xValues.transpose.flatten)

    (yVector, xMatrix)
  }

  /**
   * Returns a spark vector that contains the data from the specified column in the frame provided.
   *
   * @param frame Frame of data
   * @param columnName Name of the column to get data.  Data must be numerical
   * @return DenseVector of data from the column
   */
  def getVectorFromFrame(frame: FrameRdd, columnName: String): SparkVector = {
    frame.frameSchema.requireColumnIsNumerical(columnName)
    val dataType = frame.frameSchema.columnDataType(columnName)
    val column = frame.selectColumn(columnName).map(row => DataTypes.toDouble(row.get(0))).collect()
    //val column = frame.toDataFrame.select(columnName).collect().map(row => {
    //  DataTypes.toDouble(row.get(0))
    //})
    new SparkDenseVector(column)
  }

}
