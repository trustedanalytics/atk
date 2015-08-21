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

package org.trustedanalytics.atk.rest.v1.decorators

import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.rest.v1.viewmodels.RelLink
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.joda.time.DateTime

class GraphDecoratorTest extends FlatSpec with Matchers {

  val uri = "http://www.example.com/graphs"
  val relLinks = Seq(RelLink("foo", uri, "GET"))
  val graph = new GraphEntity(1, Some("name"), None, "storage", 1L, "hbase/titan", new DateTime, new DateTime)

  "GraphDecorator" should "be able to decorate a graph" in {
    val decoratedGraph = GraphDecorator.decorateEntity(null, relLinks, graph)
    decoratedGraph.uri should be("graphs/1")
    decoratedGraph.name should be(Some("name"))
    decoratedGraph.entityType should be("graph:titan")
    decoratedGraph.links.head.uri should be("http://www.example.com/graphs")
  }

  it should "set the correct URL in decorating a list of graphs" in {
    val graphHeaders = GraphDecorator.decorateForIndex(uri, Seq(graph))
    val graphHeader = graphHeaders.toList.head
    graphHeader.url should be("http://www.example.com/graphs/1")
    graphHeader.entityType should be("graph:titan")
  }
}
