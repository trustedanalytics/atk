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

package org.trustedanalytics.atk.engine

import org.trustedanalytics.atk.domain.graph._
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, VertexSchema }
import org.trustedanalytics.atk.engine.plugin.Invocation

/**
 * Manages multiple graphs in the underlying graph database.
 */
trait GraphStorage {

  /** Lookup a Graph, throw an Exception if not found */
  def expectGraph(graphRef: GraphReference)(implicit invocation: Invocation): GraphEntity

  def expectSeamless(graphRef: GraphReference): SeamlessGraphMeta

  @deprecated("please use expectGraph() instead")
  def lookup(id: Long)(implicit invocation: Invocation): Option[GraphEntity]

  def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): GraphEntity

  def renameGraph(graph: GraphEntity, newName: String)(implicit invocation: Invocation): GraphEntity

  def dropGraph(graph: GraphEntity)(implicit invocation: Invocation)

  def copyGraph(graph: GraphEntity, name: Option[String])(implicit invocation: Invocation): GraphEntity

  def getGraphs()(implicit invocation: Invocation): Seq[GraphEntity]

  def getGraphByName(name: Option[String])(implicit invocation: Invocation): Option[GraphEntity]

  def defineVertexType(graphRef: GraphReference, vertexSchema: VertexSchema)(implicit invocation: Invocation): SeamlessGraphMeta

  def defineEdgeType(graphRef: GraphReference, edgeSchema: EdgeSchema)(implicit invocation: Invocation): SeamlessGraphMeta

  /*get build to trigger based on changes*/
  var test = "test"

}
