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

package org.trustedanalytics.atk.engine.frame.plugins.load

import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter

import breeze.linalg.DenseVector
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions._
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.scalatest.{ Matchers }
import org.scalatest.Assertions._
import org.trustedanalytics.atk.engine.frame.plugins.bincolumn.DiscretizationFunctions
import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.CsvRowParser
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec
import com.cloudera.sparkts._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, PythonRddStorage }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.util.Random

class TimeSeriesTest extends TestingSparkContextWordSpec with Matchers {

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

  "timeseries" should {
    /*
    "create timeseries rdd with stock data" in {
      val sqlContext = new SQLContext(sparkContext)

      val tickerObs = loadObservations(sqlContext, "integration-tests/datasets/ticker.tsv")

      // Create an daily DateTimeIndex over August and September 2015
      val startDate = LocalDate.parse("2015-08-03")
      val endDate = LocalDate.parse("2015-09-22")

      val dtIndex = DateTimeIndex.uniformFromInterval(startDate.atStartOfDay(ZoneOffset.UTC), endDate.atStartOfDay(ZoneOffset.UTC),
        new BusinessDayFrequency(1))

      // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
      val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
        "timestamp", "symbol", "price")

      // Cache it in memory
      tickerTsrdd.cache()

      // Count the number of series (number of symbols)
      println("Timeseries RDD count: " + tickerTsrdd.count().toString)

      // Impute missing values using linear interpolation
      val filled = tickerTsrdd.fill("linear")

      // Compute return rates
      val returnRates = filled.returnRates()

      // Compute Durbin-Watson stats for each series
      val dwStats = returnRates.mapValues { x =>
        TimeSeriesStatisticalTests.dwtest(new DenseVector[Double](x.toArray))
      }

      println("Durbin-Watson Stats min: " + dwStats.map(_.swap).min.toString())
      println("Durbin-Watson Stats max: " + dwStats.map(_.swap).max.toString())
    }
    */

    "create timeseries rdd" in {
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
      //assert(frameDataFrame.count == inputData.count)
      // Create a timeseries RDD
      val timeseriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, frameDataFrame, tsCol, keyCol, valCol)
      assert(timeseriesRdd.count == 2) // we should have one row per key in the timeseries rdd
      // Back to a FrameRdd
      val timeseriesSchema = FrameSchema(List(Column(keyCol, DataTypes.string), Column(valCol, DataTypes.vector(dtIndex.size))))
      println("*************")
      println("*************\n frameDataFrame count: " + frameDataFrame.count().toString)
      println("*************\ntimeseriesRdd count: " + timeseriesRdd.count.toString)

      val mapped = timeseriesRdd.map(row => Array[Any](row._1, row._2))
      val timeseriesFrame = FrameRdd.toFrameRdd(timeseriesSchema, mapped)
      println("*************\ntimeseries frame count: " + timeseriesFrame.count.toString)

      for (row <- timeseriesFrame.take(timeseriesFrame.count().toInt)) {
        println("ROW: " + row(0).toString + "\t" + row(1).toString)
      }

      // Implute missing values
      val filled = timeseriesRdd.fill("nearest")
      filled.cache()
      assert(filled.count == timeseriesRdd.count) // We should still have the same number of series

      // After filing in missing values, we shouldn't have any NaNs
      val seriesA = filled.findSeries("a")
      for (value <- seriesA.toArray) {
        assert(value != Double.NaN)
      }
      val seriesB = filled.findSeries("b")
      for (value <- seriesB.toArray) {
        assert(value != Double.NaN)
      }

      // Slice by date/time
      val sliceStart = LocalDate.parse("2016-01-02").atStartOfDay(ZoneId.systemDefault())
      val sliceEnd = LocalDate.parse("2016-01-04").atStartOfDay(ZoneId.systemDefault())
      val sliced = filled.slice(sliceStart, sliceEnd)
      assert(sliced.count == filled.count) // we should still have the same series count
      for (date <- sliced.index.toZonedDateTimeArray()) {
        if (!date.isEqual(sliceStart) && !date.isEqual(sliceEnd)) {
          assert(date.isAfter(sliceStart))
          assert(date.isBefore(sliceEnd))
        }
      }

    }
  }

  /*
    // Generate time series data
    val r = Random
    val inputBuffer = new ListBuffer[Array[Any]]()
    val numDays = 4
    val keys = Array("a","b","c")
    val dates = Array("2016-01-01","2016-01-02")

    val startDate = LocalDate.parse("2016-01-01").atStartOfDay(ZoneOffset.UTC)
    for (day <- 0 to numDays) {
      val date = if (day > 0) startDate.plusDays(day).toInstant else startDate.toInstant
      for (key <- keys) {
        inputBuffer.append(Array[Any](Timestamp.from(date), key, r.nextInt(10)))
        println(inputBuffer.last(0).toString + " " + inputBuffer.last(1) + "  " + inputBuffer.last(2).toString)
      }
    }


    for (date <- dates) {
      val dt = LocalDate.parse(date).atStartOfDay(ZoneOffset.UTC)
      for (key <- keys) {
        inputBuffer.append(Array[Any](Timestamp.from(dt.toInstant), key, r.nextInt(10)))
        println(inputBuffer.last(0).toString + " " + inputBuffer.last(1) + "  " + inputBuffer.last(2).toString)
      }
    }

    // Generate DateTimeIndex
    val dtIndex = DateTimeIndex.uniformFromInterval(LocalDate.parse(dates(0)).atStartOfDay(ZoneOffset.UTC),
      LocalDate.parse(dates(dates.length-1)).atStartOfDay(ZoneOffset.UTC), new DayFrequency(1))
    println("start: " + dtIndex.first.toString + ", end: " + dtIndex.last.toString)

    // Create data frame from our time series data
    val rowRdd = sparkContext.parallelize(inputBuffer.toList) //.map(row => Row(row(0), row(1), row(2)))
    val schema = FrameSchema(List(Column("date", DataTypes.datetime), Column("key", DataTypes.string), Column("value", DataTypes.int32)))
    val dataFrame = FrameRdd.toFrameRdd(schema, rowRdd).toDataFrame

    val tsRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, dataFrame, "date", "key", "value")

*/
  /*
    val fields = Seq(
    StructField("date", TimestampType, true),
    StructField("key", StringType, true),
    StructField("value", DoubleType, true)
    )
    val schema = StructType(fields)
    val dataFrame = sqlContext.createDataFrame(rowRdd, schema)


    // Create time series RDD
    val tsRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, dataFrame, "date", "key", "value")
    */

}
