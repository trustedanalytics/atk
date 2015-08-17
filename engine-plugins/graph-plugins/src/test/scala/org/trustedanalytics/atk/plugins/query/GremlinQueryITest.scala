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

package org.trustedanalytics.atk.plugins.query

import org.trustedanalytics.atk.testutils.{ TestingTitan, MatcherUtils }
import MatcherUtils._
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import spray.json.JsNumber

class GremlinQueryITest extends FlatSpec with Matchers with TestingTitan with BeforeAndAfter {
  before {
    setupTitan()

    // Create schema before setting properties -- Needed in Titan 0.5.4+
    val graphManager = titanGraph.getManagementSystem()
    graphManager.makePropertyKey("name").dataType(classOf[String]).make()
    graphManager.makePropertyKey("age").dataType(classOf[Integer]).make()
    graphManager.makeEdgeLabel("knows").make()
    graphManager.commit()
  }

  after {
    cleanupTitan()
  }

  "executeGremlinQuery" should "execute valid Gremlin queries" in {
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)
    val edge = titanGraph.addEdge(null, vertex1, vertex2, "knows")

    vertex1.setProperty("name", "alice")
    vertex1.setProperty("age", 23)
    vertex2.setProperty("name", "bob")
    vertex2.setProperty("age", 27)

    titanGraph.commit()

    val gremlinQuery = new GremlinQuery()
    val gremlinScript = """g.V("name", "alice").out("knows")"""

    val bindings = gremlinQuery.gremlinExecutor.createBindings()
    bindings.put("g", titanGraph)

    val results = gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    val vertexCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.V.count()", bindings).toArray
    val edgeCount = gremlinQuery.executeGremlinQuery(titanGraph, "g.E.count()", bindings).toArray

    results.size should equal(1)
    vertexCount.size should equal(1)
    edgeCount.size should equal(1)

    vertex2 should equalsGraphSONVertex(results(0))
    vertexCount(0) should equal(JsNumber(2))
    edgeCount(0) should equal(JsNumber(1))
  }
  "executeGremlinQuery" should "throw a Runtime exception when executing invalid Gremlin" in {
    intercept[java.lang.RuntimeException] {
      val gremlinQuery = new GremlinQuery()
      val gremlinScript = """InvalidGremlin"""

      val bindings = gremlinQuery.gremlinExecutor.createBindings()
      bindings.put("g", titanGraph)

      gremlinQuery.executeGremlinQuery(titanGraph, gremlinScript, bindings).toArray
    }
  }

}
