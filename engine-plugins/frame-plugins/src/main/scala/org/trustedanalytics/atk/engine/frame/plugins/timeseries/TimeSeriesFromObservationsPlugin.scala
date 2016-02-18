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

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import com.cloudera.sparkts._

/** Json conversion for arguments and return value case classes */
object TimeSeriesFromObservationsJsonFormat {
  implicit val dotProductFormat = jsonFormat5(TimeSeriesFromObservationsArgs)
}

// Implicits needed for JSON conversion
import TimeSeriesFromObservationsJsonFormat._

/**
 * Reformats a frame of observations as a time series.
 */
@PluginDoc(oneLine = "Returns a frame that has the observations formatted as a time series.",
  extended = """Uses the specified timestamp, key, and value columns and the date/time
                index provided to format the observations as a time series.  The time series
                frame will have columns for the key and a vector of the observed values that
                correspond to the date/time index.""")
class TimeSeriesFromObservationsPlugin extends SparkCommandPlugin[TimeSeriesFromObservationsArgs, FrameReference] {

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
   * Returns a frame formatted as a timeseries, based on the specified frame of observations.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesFromObservationsArgs)(implicit invocation: Invocation): FrameReference = {

    val frame: SparkFrame = arguments.frame

    // Column validation
    frame.schema.validateColumnsExist(List(arguments.timestampColumn, arguments.keyColumn, arguments.valueColumn))
    frame.schema.requireColumnIsType(arguments.timestampColumn, DataTypes.datetime)
    frame.schema.requireColumnIsType(arguments.valueColumn, DataTypes.float64)

    // Get DateTimeIndex
    val dateTimeIndex = TimeSeriesFunctions.createDateTimeIndex(arguments.dateTimeIndex)

    // Create DataFrame with a new column that's formatted as a Timestamp (because this is what timeSeriesRDDFromObservations requires)
    val newTimestampColumn = arguments.timestampColumn + "_as_timestamp" // name for the new timestamp formatted column
    val dataFrame = frame.toDataFrame
    val dataFrameWithTimestamp = dataFrame.withColumn(newTimestampColumn, TimeSeriesFunctions.toTimestamp(dataFrame(arguments.timestampColumn)))

    // Convert the frame of observations to a TimeSeriesRDD
    val timeseriesRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dateTimeIndex, dataFrameWithTimestamp, newTimestampColumn, arguments.keyColumn, arguments.valueColumn)

    // Convert back to a frame to return
    val timeseriesFrameRdd = TimeSeriesFunctions.createFrameRdd(timeseriesRdd, arguments.keyColumn, arguments.valueColumn)
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by timeseries_from_observations command"))) {
      newFrame => newFrame.save(timeseriesFrameRdd)
    }

  }
}