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

import java.sql.Timestamp
import java.time.format.DateTimeParseException

import com.cloudera.sparkts.{ TimeSeriesRDD, DayFrequency, DateTimeIndex }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, DataFrame, SQLContext }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers
import org.joda.time.DateTime
import java.time.{ ZoneId, ZonedDateTime }

import scala.collection.mutable

class TimeSeriesTest extends TestingSparkContextFlatSpec with Matchers {

  "getZonedDateTime" should "return ZonedDateTime for the DateTime provided" in {
    val dateTime = DateTime.parse("2016-01-05T12:15:55Z")
    val zonedDateTime = TimeSeriesFunctions.getZonedDateTime(dateTime)
    assertResult(2016) { zonedDateTime.getYear }
    assertResult(1) { zonedDateTime.getMonthValue }
    assertResult(5) { zonedDateTime.getDayOfMonth }
    assertResult(12) { zonedDateTime.getHour }
    assertResult(15) { zonedDateTime.getMinute }
    assertResult(55) { zonedDateTime.getSecond }
    assertResult("Z") { zonedDateTime.getZone.toString }
  }

  "getDateTimeIndexFromStrings" should "return a DateTimeIndex if a valid list of date/time strings is provided" in {
    val dateTimeStrings = List("2016-01-01T12:00Z", "2016-01-03T12:00Z", "2016-01-05T12:00Z")

    val dateTimeIndex = TimeSeriesFunctions.getDateTimeIndexFromStrings(dateTimeStrings)
    dateTimeIndex.size shouldBe 3
    val x = 0
    for (x <- 0 until dateTimeIndex.size) {
      assertResult(dateTimeStrings(x)) { dateTimeIndex.dateTimeAtLoc(x).toString }
    }
  }

  "getDateTimeIndexFromStrings" should "throw an DateTimeParseException if invalid strings are provided" in {
    val invalidDateStrings = List("4", "invalid", "tuesday")
    intercept[DateTimeParseException] {
      TimeSeriesFunctions.getDateTimeIndexFromStrings(invalidDateStrings)
    }
  }

  // Used for creating a frame of observations
  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val symbol = tokens(3)
      val price = tokens(4).toDouble
      Row(Timestamp.from(dt.toInstant), symbol, price)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("symbol", StringType, true),
      StructField("price", DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  "getFrameFromTimeSeriesRdd" should "create a frame from a TimeSeriesRDD" in {
    val sqlContext = new SQLContext(sparkContext)
    val dateTimeCol = "dates"
    val tsCol = "timestamp"
    val keyCol = "keys"
    val valCol = "values"
    val xCol = "temp"

    val inputData = Array(
      Array("2016-01-01T12:00:00Z", "a", 1.0, 88),
      Array("2016-01-01T12:00:00Z", "b", 2.0, 89),
      Array("2016-01-02T12:00:00Z", "a", Double.NaN, 100),
      Array("2016-01-02T12:00:00Z", "b", 3.0, 78),
      Array("2016-01-03T12:00:00Z", "a", 3.0, 72),
      Array("2016-01-03T12:00:00Z", "b", 4.0, 85),
      Array("2016-01-04T12:00:00Z", "a", 4.0, 87),
      Array("2016-01-04T12:00:00Z", "b", 5.0, 88),
      Array("2016-01-05T12:00:00Z", "a", Double.NaN, 88),
      Array("2016-01-05T12:00:00Z", "b", 6.0, 87),
      Array("2016-01-06T12:00:00Z", "a", 6.0, 86),
      Array("2016-01-06T12:00:00Z", "b", 7.0, 84)
    )

    // Create date/time index from interval
    val dtIndex = DateTimeIndex.uniformFromInterval(ZonedDateTime.parse("2016-01-01T12:00:00Z"), ZonedDateTime.parse("2016-01-06T12:00:00Z"), new DayFrequency(1))

    // Try using ATK FrameScheme/FrameRdd wrappers
    val frameSchema = FrameSchema(List(Column(dateTimeCol, DataTypes.datetime), Column(keyCol, DataTypes.string), Column(valCol, DataTypes.float64), Column(xCol, DataTypes.int32)))
    val rowArrayRdd = sparkContext.parallelize(inputData)
    val frameRdd = FrameRdd.toFrameRdd(frameSchema, rowArrayRdd)
    var frameDataFrame = frameRdd.toDataFrame

    // Add a "timestamp" column using the Timestamp data type
    val toTimestamp = udf((t: String) => Timestamp.from(ZonedDateTime.parse(t).toInstant))
    frameDataFrame = frameDataFrame.withColumn(tsCol, toTimestamp(frameDataFrame(dateTimeCol))).select(tsCol, keyCol, valCol)

    // Create a timeseries RDD
    val timeseriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, frameDataFrame, tsCol, keyCol, valCol)
    assertResult(2) { timeseriesRdd.count } // we should have one row per key in the timeseries rdd

    // Create frame from the timeseries RDD
    var frame = TimeSeriesFunctions.getFrameFromTimeSeriesRdd(timeseriesRdd, keyCol, valCol)
    assertResult(2) { frame.count() }
    val frameData = frame.sortByColumns(List((keyCol, true))).take(frame.count.toInt)
    val expectedData = Array(
      Row("a", Vector(1.0, Double.NaN, 3.0, 4.0, Double.NaN, 6.0)),
      Row("b", Vector(2.0, 3.0, 4.0, 5.0, 6.0, 7.0))
    )
    val row_i = 0
    for (row_i <- 0 until expectedData.length) {
      // compare key column
      assertResult(expectedData(row_i).get(0)) {
        frameData(row_i).get(0)
      }
      val expectedVector = expectedData(row_i).get(1).asInstanceOf[Vector[Double]]
      val frameVector = frameData(row_i).get(1).asInstanceOf[mutable.WrappedArray[Double]]
      assertResult(expectedVector.length) {
        frameVector.length
      }
      val vector_i = 0
      for (vector_i <- 0 until expectedVector.length) {
        assert(expectedVector(vector_i).equals(frameVector(vector_i)))
      }
    }
  }
}
