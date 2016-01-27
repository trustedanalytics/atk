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

package org.trustedanalytics.atk.engine.frame.plugins

import java.time.{ ZonedDateTime, ZoneId, LocalDate }

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ DataTypes, FrameSchema, Column }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, PythonRddStorage }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import com.cloudera.sparkts._

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Reformats a frame of observations as a time series.
 */
@PluginDoc(oneLine = "Reformats a frame of observations as a time series.",
  extended = """Reformats a frame of observations as a time series, using the
                specified timestamp, key, and value columns and the date/time
                index provided.""")
class TimeSeriesFromObservationsPlugin extends SparkCommandPlugin[TimeSeriesFromObservationsArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/timeseries_from_observations"

  /* This plugin executes python udfs; by default sparkcommandplugins have this property as false */
  override def executesPythonUdf = false

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: TimeSeriesFromObservationsArgs)(implicit invocation: Invocation) = 1

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesFromObservationsArgs)(implicit invocation: Invocation): UnitReturn = {

    val frame: SparkFrame = arguments.frame

    val timestampColumn: String = arguments.timestampColumn
    val keyColumn: String = arguments.keyColumn
    val valueColumn: String = arguments.valueColumn

    if (frame.schema.hasColumn(timestampColumn) == false)
      throw new IllegalArgumentException(s"Invalid timestampColumn provided. Column named '$timestampColumn' does not exist in the frame's schema.")

    if (frame.schema.hasColumn(keyColumn) == false)
      throw new IllegalArgumentException(s"Invalid keyColumn provided. Column named '$keyColumn' does not exist in the frame's schema.")

    if (frame.schema.hasColumn(valueColumn) == false)
      throw new IllegalArgumentException(s"Invalid valueColumn provided. Column named '$valueColumn' does not exist in the frame's schema.")

    val dateTimeArray = new Array[ZonedDateTime](arguments.dateTimeIndex.size)
    var i = 0

    for (datetime <- arguments.dateTimeIndex) {
      dateTimeArray(i) = LocalDate.parse(datetime).atStartOfDay(ZoneId.systemDefault())
      i += 1
    }

    // Create DateTimeIndex
    val dateTimeIndex = DateTimeIndex.irregular(dateTimeArray)
    val dataFrameWithTimestamp = frame.rdd.toDataFrame
    val newTimestampColumn = timestampColumn + "_new"
    val timeseriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dateTimeIndex, dataFrameWithTimestamp, timestampColumn, keyColumn, valueColumn)

    val timeseriesSchema = FrameSchema(List(Column(keyColumn, DataTypes.string), Column(valueColumn, DataTypes.vector(i))))
    val timeseriesFrameRdd = FrameRdd.toFrameRdd(timeseriesSchema, timeseriesRdd.map(row => Array(row)))

    frame.save(frame.rdd.zipFrameRdd(timeseriesFrameRdd))

  }
}