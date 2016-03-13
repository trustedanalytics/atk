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
package org.trustedanalytics.atk.plugins

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.graph.construction.{ AddEdgesArgs, AddVerticesArgs }
import org.trustedanalytics.atk.domain.schema.{ DataTypes, GraphSchema }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.graph.{ SparkEdgeFrame, SparkGraph }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Add Vertices to a Vertex Frame
 */
@PluginDoc(oneLine = "Add edges to a graph.",
  extended = "Includes appending to a list of existing edges.",
  returns = "")
class AddEdgesPlugin extends SparkCommandPlugin[AddEdgesArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:edge/add_edges"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: AddEdgesArgs)(implicit invocation: Invocation): Int = {
    if (arguments.isCreateMissingVertices) {
      15
    }
    else {
      8
    }
  }

  /**
   * Add Vertices to a Seamless Graph
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddEdgesArgs)(implicit invocation: Invocation): UnitReturn = {

    val edgeFrame: SparkEdgeFrame = arguments.edgeFrame
    val graph: SparkGraph = edgeFrame.graph
    val sourceFrame: SparkFrame = arguments.sourceFrame
    sourceFrame.schema.validateColumnsExist(arguments.allColumnNames)
    if (sourceFrame.rowCount.getOrElse(0L) > 0L || !sourceFrame.rdd.isEmpty()) {

      // assign unique ids to source data
      val edgeDataToAdd = sourceFrame.rdd.selectColumns(arguments.allColumnNames).assignUniqueIds(GraphSchema.edgeProperty, startId = graph.nextId)
      edgeDataToAdd.cache()
      graph.incrementIdCounter(edgeDataToAdd.count())

      // convert to appropriate schema, adding edge system columns
      val edgesWithoutVids = edgeDataToAdd.convertToNewSchema(edgeDataToAdd.frameSchema.addColumn(GraphSchema.srcVidProperty, DataTypes.int64).addColumn(GraphSchema.destVidProperty, DataTypes.int64))
      edgesWithoutVids.cache()
      edgeDataToAdd.unpersist(blocking = false)

      val srcLabel = edgeFrame.schema.srcVertexLabel
      val destLabel = edgeFrame.schema.destVertexLabel

      // create vertices from edge data and append to vertex frames
      if (arguments.isCreateMissingVertices) {
        val sourceVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForSourceVertexId))
        val destVertexData = edgesWithoutVids.selectColumns(List(arguments.columnNameForDestVertexId))
        val addVerticesPlugin = new AddVerticesPlugin
        addVerticesPlugin.addVertices(AddVerticesArgs(edgeFrame.graphMeta.vertexMeta(srcLabel).toReference, null, arguments.columnNameForSourceVertexId), sourceVertexData, preferNewVertexData = false)
        addVerticesPlugin.addVertices(AddVerticesArgs(edgeFrame.graphMeta.vertexMeta(destLabel).toReference, null, arguments.columnNameForDestVertexId), destVertexData, preferNewVertexData = false)
      }

      // load src and dest vertex ids
      val srcVertexIds = graph.vertexRdd(srcLabel).idColumns.groupByKey()
      srcVertexIds.cache()
      val destVertexIds = if (srcLabel == destLabel) {
        srcVertexIds
      }
      else {
        graph.vertexRdd(destLabel).idColumns.groupByKey()
      }

      // check that at least some source and destination vertices actually exist
      if (srcVertexIds.count() == 0) {
        throw new IllegalArgumentException("Source vertex frame does NOT contain any vertices.  Please add source vertices first or enable create_missing_vertices=True.")
      }
      if (destVertexIds.count() == 0) {
        throw new IllegalArgumentException("Destination vertex frame does NOT contain any vertices.  Please add destination vertices first or enable create_missing_vertices=True.")
      }

      // match ids with vertices
      val edgesByTail = edgesWithoutVids.groupByRows(row => row.value(arguments.columnNameForDestVertexId))
      val edgesWithTail = destVertexIds.join(edgesByTail).flatMapValues { value =>
        val idMap = value._1
        val vid = idMap.head
        val edgeRows = value._2
        edgeRows.map(e => edgesWithoutVids.rowWrapper(e).setValue(GraphSchema.destVidProperty, vid))
      }.values

      val edgesByHead = new FrameRdd(edgesWithoutVids.frameSchema, edgesWithTail).groupByRows(row => row.value(arguments.columnNameForSourceVertexId))
      val edgesWithVids = srcVertexIds.join(edgesByHead).flatMapValues(value => {
        val idMap = value._1
        val vid = idMap.head
        val edges = value._2
        edges.map(e => edgesWithoutVids.rowWrapper(e).setValue(GraphSchema.srcVidProperty, vid))
      }).values

      srcVertexIds.unpersist(blocking = false)
      destVertexIds.unpersist(blocking = false)
      edgesWithoutVids.unpersist(blocking = false)

      // convert convert edges to add to correct schema
      val correctedSchema = edgesWithoutVids.frameSchema
        //.convertType("_src_vid", DataTypes.int64)
        //.convertType("_dest_vid", DataTypes.int64)
        .dropColumns(List(arguments.columnNameForSourceVertexId, arguments.columnNameForDestVertexId))
      val edgesToAdd = new FrameRdd(edgesWithoutVids.frameSchema, edgesWithVids).convertToNewSchema(correctedSchema)

      // append to existing data
      val existingEdgeData = edgeFrame.rdd
      val combinedRdd = existingEdgeData.append(edgesToAdd)
      edgeFrame.save(combinedRdd)
    }

  }

}
