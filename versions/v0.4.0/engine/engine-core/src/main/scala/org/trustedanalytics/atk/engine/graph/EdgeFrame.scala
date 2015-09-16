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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.domain.schema.EdgeSchema
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.{ GraphStorage, FrameStorage }
import org.trustedanalytics.atk.engine.frame._
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.atk.graph.EdgeFrameRdd

/**
 * Interface for working with EdgeFrames for plugin authors
 */
trait EdgeFrame extends Frame {

  /**
   * A frame Schema with the extra info needed for a EdgeFrame
   */
  override def schema: EdgeSchema

  def graphMeta: SeamlessGraphMeta

  def graph: Graph
}

object EdgeFrame {

  implicit def EdgeFrameToFrameReference(EdgeFrame: EdgeFrame): FrameReference = EdgeFrame.entity.toReference

  implicit def EdgeFrameToFrameEntity(EdgeFrame: EdgeFrame): FrameEntity = EdgeFrame.entity
}

/**
 * Interface for working with EdgeFrames for plugin authors, including Spark related methods
 */
trait SparkEdgeFrame extends EdgeFrame {

  /** Load the frame's data as an RDD */
  def rdd: EdgeFrameRdd

  /** Update the data for this frame */
  def save(rdd: EdgeFrameRdd): SparkEdgeFrame

  def graph: SparkGraph

}

class EdgeFrameImpl(frame: FrameReference, frameStorage: FrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends FrameImpl(frame, frameStorage)(invocation)
    with EdgeFrame {

  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  override def entity: FrameEntity = {
    val e = super.entity
    require(e.isEdgeFrame, s"EdgeFrame is required but found other frame type: $e")
    e
  }

  /**
   * A frame Schema with the extra info needed for a EdgeFrame
   */
  override def schema: EdgeSchema = super.schema.asInstanceOf[EdgeSchema]

  /** The graph this EdgeFrame is a part of */
  override def graphMeta: SeamlessGraphMeta = {
    sparkGraphStorage.expectSeamless(entity.graphId.getOrElse(throw new RuntimeException("VertxFrame is required to have a graphId but this one didn't")))
  }

  override def graph: Graph = {
    new GraphImpl(graphMeta.toReference, sparkGraphStorage)
  }

}

class SparkEdgeFrameImpl(frame: FrameReference, sc: SparkContext, sparkFrameStorage: SparkFrameStorage, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends EdgeFrameImpl(frame, sparkFrameStorage, sparkGraphStorage)(invocation)
    with SparkEdgeFrame {

  /** Load the frame's data as an RDD */
  override def rdd: EdgeFrameRdd = sparkGraphStorage.loadEdgeRDD(sc, frame)

  /** Update the data for this frame */
  override def save(rdd: EdgeFrameRdd): SparkEdgeFrame = {
    val result = sparkGraphStorage.saveEdgeRdd(frame, rdd)
    new SparkEdgeFrameImpl(result, sc, sparkFrameStorage, sparkGraphStorage)
  }

  override def graph: SparkGraph = {
    new SparkGraphImpl(graphMeta.toReference, sc, sparkGraphStorage)
  }
}