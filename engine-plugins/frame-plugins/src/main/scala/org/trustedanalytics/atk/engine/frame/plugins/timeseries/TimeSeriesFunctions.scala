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

package org.trustedanalytics.atk.engine.frame.plugins.timeseries

import java.io.Serializable
import java.sql.Timestamp
import java.time.ZonedDateTime

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.trustedanalytics.atk.engine.frame.{ VectorFunctions, RowWrapper }
import org.apache.spark.frame.FrameRdd
import com.cloudera.sparkts._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

/**
 * Object contains utility functions for working with time series
 */
object TimeSeriesFunctions extends Serializable {

  // UDF for converting datetime column to a Timestamp datatype
  val toTimestamp = udf((t: String) => Timestamp.from(ZonedDateTime.parse(t).toInstant))

  /**
   * Creates a FrameRdd for the specified TimeSeriesRdd
   *
   * @param timeseriesRdd TimeSeriesRDD
   * @param keyColumn Name of the key column
   * @param valueColumn Name of column that contains a series of values for each key
   * @return FrameRdd
   */
  def getFrameFromTimeSeriesRdd(timeseriesRdd: TimeSeriesRDD[String], keyColumn: String, valueColumn: String): FrameRdd = {
    // Create frame schema
    val timeseriesSchema = FrameSchema(List(Column(keyColumn, DataTypes.string), Column(valueColumn, DataTypes.vector(timeseriesRdd.index.size))))

    // Map the column of values so that it uses a Scala Vector rather than a Spark DenseVector.
    val toVectorDouble = udf((v: DenseVector) => v.toArray.iterator.toVector)
    val withVector = timeseriesRdd.map(row => {
      val originalColumns = row.productIterator.toList
      val newVectorCol = originalColumns(1).asInstanceOf[DenseVector].toArray.iterator.toVector
      Array[Any](originalColumns(0), newVectorCol)
    })

    // Create FrameRdd to return
    FrameRdd.toFrameRdd(timeseriesSchema, withVector)
  }

  /**
   * Creates a DateTimeIndex from the specified ISO 8601 strings
   * @param dateTimeStrings List of ISO 8601 date/time strings
   * @return DateTimeIndex
   */
  def getDateTimeIndexFromStrings(dateTimeStrings: List[String]): DateTimeIndex = {
    // Create DateTimeIndex after parsing the strings as ZonedDateTime
    DateTimeIndex.irregular(dateTimeStrings.map(str => ZonedDateTime.parse(str)).toArray)
  }

  /**
   * Creates a ZonedDateTime object from the DateTime provided
   * @param dateTime Date/time
   * @return ZonedDateTime
   */
  def getZonedDateTime(dateTime: DateTime): ZonedDateTime = {
    ZonedDateTime.parse(dateTime.toString(ISODateTimeFormat.dateTime))
  }

}
