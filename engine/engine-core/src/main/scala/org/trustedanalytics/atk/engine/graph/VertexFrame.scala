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
package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.domain.schema.VertexSchema
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.{ GraphStorage, FrameStorage }
import org.trustedanalytics.atk.engine.frame._
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.atk.graph.VertexFrameRdd

/**
 * Interface for working with VertexFrames for plugin authors
 */
trait VertexFrame extends Frame {

  /**
   * A frame Schema with the extra info needed for a VertexFrame
   */
  override def schema: VertexSchema

  def graphMeta: SeamlessGraphMeta

  def graph: Graph
}

object VertexFrame {

  implicit def vertexFrameToFrameReference(vertexFrame: VertexFrame): FrameReference = vertexFrame.entity.toReference

  implicit def vertexFrameToFrameEntity(vertexFrame: VertexFrame): FrameEntity = vertexFrame.entity
}

/**
 * Interface for working with VertexFrames for plugin authors, including Spark related methods
 */
trait SparkVertexFrame extends VertexFrame {

  /** Load the frame's data as an RDD */
  def rdd: VertexFrameRdd

  /** Update the data for this frame */
  def save(rdd: VertexFrameRdd): SparkVertexFrame

  def graph: SparkGraph

}

class VertexFrameImpl(frame: FrameReference, frameStorage: FrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends FrameImpl(frame, frameStorage)(invocation)
    with VertexFrame {

  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  override def entity: FrameEntity = {
    val e = super.entity
    require(e.isVertexFrame, s"VertexFrame is required but found other frame type: $e")
    e
  }

  /**
   * A frame Schema with the extra info needed for a VertexFrame
   */
  override def schema: VertexSchema = super.schema.asInstanceOf[VertexSchema]

  /** The graph this VertexFrame is a part of */
  override def graphMeta: SeamlessGraphMeta = {
    sparkGraphStorage.expectSeamless(entity.graphId.getOrElse(throw new RuntimeException("VertxFrame is required to have a graphId but this one didn't")))
  }

  override def graph: Graph = {
    new GraphImpl(graphMeta.toReference, sparkGraphStorage)
  }
}

class SparkVertexFrameImpl(frame: FrameReference, sc: SparkContext, sparkFrameStorage: SparkFrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends VertexFrameImpl(frame, sparkFrameStorage, sparkGraphStorage)(invocation)
    with SparkVertexFrame {

  /** Load the frame's data as an RDD */
  override def rdd: VertexFrameRdd = sparkGraphStorage.loadVertexRDD(sc, frame)

  /** Update the data for this frame */
  override def save(rdd: VertexFrameRdd): SparkVertexFrame = {
    val result = sparkGraphStorage.saveVertexRdd(frame, rdd)
    new SparkVertexFrameImpl(result, sc, sparkFrameStorage, sparkGraphStorage)
  }

  override def graph: SparkGraph = {
    new SparkGraphImpl(graphMeta.toReference, sc, sparkGraphStorage)
  }

}