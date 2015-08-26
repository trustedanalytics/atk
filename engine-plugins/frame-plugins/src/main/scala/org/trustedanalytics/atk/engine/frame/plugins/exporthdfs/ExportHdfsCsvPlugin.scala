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

package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.ExportHdfsCsvArgs
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.HdfsFileStorage
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.hadoop.fs.Path

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to csv file
 */
@PluginDoc(oneLine = "Write current frame to HDFS in csv format.",
  extended = "Export the frame to a file in csv format as a Hadoop file.")
class ExportHdfsCsvPlugin extends SparkCommandPlugin[ExportHdfsCsvArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_csv"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsCsvArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsCsvArgs)(implicit invocation: Invocation): UnitReturn = {

    val fileStorage = new HdfsFileStorage
    require(!fileStorage.exists(new Path(arguments.folderName)), "File or Directory already exists")
    val frame: SparkFrame = arguments.frame
    // load frame as RDD
    FrameExportHdfs.exportToHdfsCsv(frame.rdd, arguments.folderName, arguments.separator.getOrElse(','), arguments.count, arguments.offset)
  }
}
