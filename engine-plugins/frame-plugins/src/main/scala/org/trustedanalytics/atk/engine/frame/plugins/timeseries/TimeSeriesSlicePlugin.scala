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
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import com.cloudera.sparkts._
import org.joda.time.DateTime
import org.apache.spark.mllib.linalg.{ Vector, DenseVector }

/** Json conversion for arguments and return value case classes */
object TimeSeriesSliceFormat {
  implicit val sliceArgFormat = jsonFormat4(TimeSeriesSliceArgs)
}

// Implicits needed for JSON conversion
import TimeSeriesSliceFormat._

/**
 * Reformats a frame of observations as a time series.
 */
@PluginDoc(oneLine = "Returns a frame that is a sub-slice of the given series.",
  extended = """Splits a time series frame on the specified start and end date/times.""")
class TimeSeriesSlicePlugin extends SparkCommandPlugin[TimeSeriesSliceArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/timeseries_slice"

  /* This plugin executes python udfs; by default sparkcommandplugins have this property as false */
  override def executesPythonUdf = false

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: TimeSeriesSliceArgs)(implicit invocation: Invocation) = 1

  /**
   * Returns a frame split on the specified start and end date/times.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesSliceArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val start: DateTime = arguments.start
    val end: DateTime = arguments.end

    // Get DateTimeIndex
    val dateTimeIndex = TimeSeriesFunctions.createDateTimeIndex(arguments.dateTimeIndex)

    // Discover the column names of the key and value column
    val (keyColumn, valueColumn) = TimeSeriesFunctions.discoverKeyAndValueColumns(frame.schema)

    // Create TimeSeriesRDD
    val timeseriesRdd = TimeSeriesFunctions.createTimeSeriesRDD(keyColumn, valueColumn, frame, dateTimeIndex)

    // Perform Slice
    val sliced = timeseriesRdd.slice(TimeSeriesFunctions.parseZonedDateTime(start), TimeSeriesFunctions.parseZonedDateTime(end))

    // Convert back to a frame to return
    val timeseriesFrameRdd = TimeSeriesFunctions.createFrameRdd(sliced, keyColumn, valueColumn)
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by timeseries_slice command"))) {
      newFrame => newFrame.save(timeseriesFrameRdd)
    }
  }
}