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
package org.trustedanalytics.atk.rest.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.rest.v1.viewmodels.RelLink
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.joda.time.DateTime

class GraphDecoratorTest extends FlatSpec with Matchers {

  val uri = "http://www.example.com/graphs"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val graph = new GraphEntity(1, Some("name"), None, "storage", 1L, "atk/frame", new DateTime, new DateTime)
  val vertex1 = GetGraphComponent("v1", 532, List("pA", "pB"))
  val vertex2 = GetGraphComponent("v2", 95, Nil)
  val edge1 = GetGraphComponent("e1", 844, List("pC"))
  val decorateReadyGraph = DecorateReadyGraphEntity(graph, List(vertex1, vertex2), List(edge1))

  "GraphDecorator" should "be able to decorate a graph" in {
    val decoratedGraph = GraphDecorator.decorateEntity(null, relLinks, decorateReadyGraph)
    decoratedGraph.uri should be("graphs/1")
    decoratedGraph.name should be(Some("name"))
    decoratedGraph.entityType should be("graph:")
    decoratedGraph.links.head.uri should be("http://www.example.com/graphs")
    decoratedGraph.vertices.size should be(2)
    decoratedGraph.vertices(0) should be(vertex1)
    decoratedGraph.vertices(1) should be(vertex2)
    decoratedGraph.edges.size should be(1)
    decoratedGraph.edges(0) should be(edge1)
  }

  it should "set the correct URL in decorating a list of graphs" in {
    val graphHeaders = GraphDecorator.decorateForIndex(uri, Seq(decorateReadyGraph))
    val graphHeader = graphHeaders.toList.head
    graphHeader.url should be("http://www.example.com/graphs/1")
    graphHeader.entityType should be("graph:")
  }
}
