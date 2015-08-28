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

package org.trustedanalytics.atk.domain.graph

import org.trustedanalytics.atk.domain.StorageFormats
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema.{ GraphElementSchema, Schema, VertexSchema }

/**
 * Wrapper for Seamless Graph Meta data stored in the database
 *
 * "Seamless Graph" is a graph that provides a "seamless user experience" between graphs and frames.
 * The same data can be treated as frames one moment and as a graph the next without any import/export.
 *
 * @param graphEntity the graph meta data
 * @param frameEntities the vertex and edge frames owned by this graph (might be empty but never null)
 */
case class SeamlessGraphMeta(graphEntity: GraphEntity, frameEntities: List[FrameEntity]) {
  require(graphEntity != null, "graph is required")
  require(frameEntities != null, "frame is required, it can be empty but not null")
  require(graphEntity.storageFormat == StorageFormats.SeamlessGraph, s"Storage format should be ${StorageFormats.SeamlessGraph}")
  frameEntities.foreach(frame => require(frame.graphId.isDefined, "frame should be owned by the graph, graphId is required"))
  frameEntities.foreach(frame => require(frame.graphId.get == graphEntity.id, "frame should be owned by the graph, graphId did not match"))

  /** Labels to frames */
  @transient private lazy val edgeFramesMap = frameEntities.filter(frame => frame.isEdgeFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, FrameEntity]

  /** Labels to frames */
  @transient private lazy val vertexFramesMap = frameEntities.filter(frame => frame.isVertexFrame)
    .map(frame => (frame.label.get, frame))
    .toMap[String, FrameEntity]

  // TODO: this might be good but worried it might be introducing issues, commenting out for now -- Todd 1/9/2015
  //require(frameEntities.size == (edgeFramesMap.size + vertexFramesMap.size), "labels should not be duplicated within a graph, this is a bug")

  /** convenience method for getting the id of the graph */
  def id: Long = graphEntity.id

  /** Next unique id for edges and vertices */
  def nextId(): Long = {
    graphEntity.nextId()
  }

  /**
   * Convenience method for getting a reference to this graph
   */
  def toReference: GraphReference = {
    GraphReference(id)
  }

  /**
   * Get the frame meta data for an edge list in this graph
   * @param label the type of edges
   * @return frame meta data
   */
  def edgeMeta(label: String): FrameEntity = {
    edgeFramesMap.getOrElse(label, throw new IllegalArgumentException(s"No edge frame with label $label in this graph.  Defined edge labels: $edgeLabelsAsString"))
  }

  /**
   * Get the frame meta data for an vertex list in this graph
   * @param label the type of vertices
   * @return frame meta data
   */
  def vertexMeta(label: String): FrameEntity = {
    vertexFramesMap.getOrElse(label, throw new IllegalArgumentException(s"No vertex frame with label $label in this graph.  Defined vertex labels: $vertexLabelsAsString"))
  }

  /**
   * True if the supplied label is already in use in this graph
   */
  def isVertexOrEdgeLabel(label: String): Boolean = {
    frameEntities.exists(frame => frame.label.get == label)
  }

  /**
   * True if the supplied label is a vertex label within this graph
   */
  def isVertexLabel(label: String): Boolean = {
    vertexFramesMap.contains(label)
  }

  /**
   * Get a list of  all vertex frames for this graph
   * @return list of frame meta data
   */
  def vertexFrames: List[FrameEntity] = {
    vertexFramesMap.map {
      case (label: String, frame: FrameEntity) => frame
    }.toList
  }

  /**
   * Get a list of  all vertex frames for this graph
   * @return list of frame meta data
   */
  def edgeFrames: List[FrameEntity] = {
    edgeFramesMap.map {
      case (label: String, frame: FrameEntity) => frame
    }.toList
  }

  def framesWithLabels(labels: List[String]): List[FrameEntity] = {
    frameEntities.filter(frame => labels.contains(frame.name))
  }

  def framesWithoutLabels(labelsToExclude: List[String]): List[FrameEntity] = {
    frameEntities.filter(frame => !labelsToExclude.contains(frame.name))
  }

  def labels: List[String] = {
    frameEntities.map(f => f.schema.asInstanceOf[GraphElementSchema].label)
  }

  def vertexLabels: List[String] = {
    vertexFrames.map(frame => frame.label.get)
  }

  def vertexLabelsAsString: String = {
    vertexLabels.mkString(", ")
  }

  def edgeLabels: List[String] = {
    edgeFrames.map(frame => frame.label.get)
  }

  def edgeLabelsAsString: String = {
    edgeLabels.mkString(", ")
  }

  def vertexCount: Option[Long] = {
    countList(vertexFrames.map(frame => frame.rowCount))
  }

  def edgeCount: Option[Long] = {
    countList(edgeFrames.map(frame => frame.rowCount))
  }

  private def countList(list: List[Option[Long]]): Option[Long] = list match {
    case Nil => Some(0)
    case x :: xs => for {
      left <- x
      right <- countList(xs)
    } yield left + right
  }

  /**
   * Create a list of ElementIDName objects corresponding to the IDColumn of all Vertices in this graph.
   */
  def getFrameSchemaList: List[Schema] = {
    this.frameEntities.map {
      frame => frame.schema
    }
  }

}
