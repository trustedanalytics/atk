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

package org.trustedanalytics.atk.plugins.export.csv

import org.apache.commons.csv.{ CSVFormat, CSVPrinter }
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, ExportHdfsCsvArgs }
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.engine.FileStorage
import org.trustedanalytics.atk.engine.frame.{ MiscFrameFunctions, SparkFrame }
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

import scala.collection.mutable.ArrayBuffer

// Implicits needed for JSON conversion

/**
 * Export a frame to csv file
 */
// TODO:update the doc
@PluginDoc(oneLine = "Write current frame to HDFS in csv format.",
  extended = "Export the frame to a file in csv format as a Hadoop file.",
  returns = "dict with .....")
class ExportGraphCsvPlugin extends SparkCommandPlugin[ExportGraphCsvArgs, GraphExportMetadata] {

  /**
   * The name of the command
   */
  override def name: String = "graph:/export_to_csv"

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportGraphCsvArgs)(implicit invocation: Invocation): GraphExportMetadata = {

    val fileStorage = new FileStorage
    require(!fileStorage.exists(new Path(arguments.folderName)), "File or Directory already exists")

    //get the graph meta data
    val graph = arguments.graph

    // TODO: get the list of frame  (of the graph ) from the meta data
    val graphMeta = engine.graphs.expectSeamless(graph)
    val vertexFrames = graphMeta.vertexFrames.map(_.toReference)

    //TODO: export each frame to CSV file in HDFS
    val vertexFolderName = arguments.folderName + "/vertices"
    val edgeFolderName = arguments.folderName + "/edges"
    val vertexMetadata = exportFramesToCsv(vertexFolderName, graphMeta.vertexFrames, arguments, fileStorage)
    val edgeMetadata = exportFramesToCsv(edgeFolderName, graphMeta.edgeFrames, arguments, fileStorage)
    GraphExportMetadata(vertexMetadata ++ edgeMetadata)
  }
  // TODO: add scala doc

  def exportFramesToCsv(folderName: String, frameEntities: List[FrameEntity], arguments: ExportGraphCsvArgs, fileStorage: FileStorage)(implicit invocation: Invocation): List[ExportMetadata] = {
    val frames = frameEntities.map(_.toReference)
    val metadata = frames.map(frame => {
      // load frame as RDD
      val sparkFrame: SparkFrame = frame
      val sample = sparkFrame.rdd.exportToHdfsCsv(arguments.folderName, arguments.separator.charAt(0), arguments.count, arguments.offset)

      val artifactPath = new Path(s"${fileStorage.hdfs.getHomeDirectory()}/${folderName}/${sparkFrame.label.getOrElse(sparkFrame.frameId)}")
      ExportMetadata(artifactPath.toString, "all", "csv", sparkFrame.rowCount, sample,
        fileStorage.size(artifactPath.toString), Some(folderName))

    })
    metadata
  }

}
