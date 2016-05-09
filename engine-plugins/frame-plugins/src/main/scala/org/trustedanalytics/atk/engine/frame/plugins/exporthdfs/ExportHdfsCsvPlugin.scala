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

import java.nio.file.FileSystem

import org.apache.commons.csv.{ CSVPrinter, CSVFormat }
import org.apache.commons.lang3.StringUtils
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.datacatalog.{ ExportMetadata, TapDataCatalogResponse }
import org.trustedanalytics.atk.domain.frame.ExportHdfsCsvArgs
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ EngineConfig, FileStorage }
import org.trustedanalytics.atk.engine.frame.{ MiscFrameFunctions, SparkFrame }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogRestResponseJsonProtocol._

/**
 * Export a frame to csv file
 */
@PluginDoc(oneLine = "Write current frame to HDFS in csv format.",
  extended = "Export the frame to a file in csv format as a Hadoop file.",
  returns = """The URI of the created file""")
class ExportHdfsCsvPlugin extends SparkCommandPlugin[ExportHdfsCsvArgs, ExportMetadata] {

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
  override def execute(arguments: ExportHdfsCsvArgs)(implicit invocation: Invocation): ExportMetadata = {

    val fileStorage = new FileStorage
    require(!fileStorage.exists(new Path(arguments.folderName)), "File or Directory already exists")
    val frame: SparkFrame = arguments.frame
    // load frame as RDD
    val sample = exportToHdfsCsv(frame.rdd, arguments.folderName, arguments.separator.charAt(0), arguments.count, arguments.offset)

    val artifactPath = new Path(s"${fileStorage.hdfs.getHomeDirectory()}/${arguments.folderName}")
    ExportMetadata(artifactPath.toString, "all", "csv", frame.rowCount, sample,
      fileStorage.size(artifactPath.toString), Some(arguments.folderName))
  }

  /**
   * Export to a file in CSV format
   *
   * @param frameRdd input rdd containing all columns
   * @param filename file path where to store the file
   */
  private def exportToHdfsCsv(
    frameRdd: FrameRdd,
    filename: String,
    separator: Char,
    count: Int,
    offset: Int) = {

    val filterRdd = if (count > 0) MiscFrameFunctions.getPagedRdd(frameRdd, offset, count, -1) else frameRdd
    val headers = frameRdd.frameSchema.columnNames.mkString(separator.toString)
    val csvFormat = CSVFormat.RFC4180.withDelimiter(separator)

    val csvRdd = filterRdd.map(row => {
      val stringBuilder = new java.lang.StringBuilder
      val printer = new CSVPrinter(stringBuilder, csvFormat)
      val array = row.toSeq.map(col =>
        col match {
          case null => ""
          case arr: ArrayBuffer[_] => arr.mkString(",")
          case seq: Seq[_] => seq.mkString(",")
          case x => x.toString
        })
      for (i <- array) printer.print(i)
      stringBuilder.toString
    })

    val dataSample = if (csvRdd.isEmpty()) StringUtils.EMPTY else csvRdd.first()
    val addHeaders = frameRdd.sparkContext.parallelize(List(headers)) ++ csvRdd
    addHeaders.saveAsTextFile(filename)
    dataSample
  }
}
