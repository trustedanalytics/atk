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


package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.ExportHdfsJsonArgs
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ EngineConfig, HdfsFileStorage }
import org.trustedanalytics.atk.engine.frame.{ MiscFrameFunctions, SparkFrame }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to json file
 */
@PluginDoc(oneLine = "Write current frame to HDFS in JSON format.",
  extended = "Export the frame to a file in JSON format as a Hadoop file.")
class ExportHdfsJsonPlugin extends SparkCommandPlugin[ExportHdfsJsonArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_json"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsJsonArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsJsonArgs)(implicit invocation: Invocation): UnitReturn = {
    val fileStorage = new HdfsFileStorage
    require(!fileStorage.exists(new Path(arguments.folderName)), "File or Directory already exists")
    val frame: SparkFrame = arguments.frame
    exportToHdfsJson(frame.rdd, arguments.folderName, arguments.count, arguments.offset)
  }

  /**
   * Export to a file in JSON format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   */
  private def exportToHdfsJson(
    frameRdd: FrameRdd,
    filename: String,
    count: Option[Int],
    offset: Option[Int]) {

    val recCount = count.getOrElse(-1)
    val recOffset = offset.getOrElse(0)

    val filterRdd = if (recCount > 0) MiscFrameFunctions.getPagedRdd(frameRdd, recOffset, recCount, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames
    val jsonRDD = filterRdd.map {
      row =>
        {
          val value = row.toSeq.zip(headers).map {
            case (k, v) => new String("\"" + v.toString + "\":" + (if (k == null) "null"
            else if (k.isInstanceOf[String]) { "\"" + k.toString + "\"" }
            else if (k.isInstanceOf[ArrayBuffer[_]]) { k.asInstanceOf[ArrayBuffer[Double]].mkString("[", ",", "]") }
            else k.toString)
            )
          }
          value.mkString("{", ",", "}")
        }
    }
    jsonRDD.saveAsTextFile(filename)
  }

}
