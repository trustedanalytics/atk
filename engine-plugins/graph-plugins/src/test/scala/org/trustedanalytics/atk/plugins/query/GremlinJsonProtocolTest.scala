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


package org.trustedanalytics.atk.plugins.query

import java.util

import org.trustedanalytics.atk.testutils.{ TestingTitan, MatcherUtils }
import MatcherUtils._
import com.tinkerpop.blueprints.{ Edge, Element, Vertex }
import com.tinkerpop.pipes.util.structures.Row
import org.scalatest.{ BeforeAndAfter, FlatSpec, Matchers }
import scala.collection.JavaConverters._
import spray.json._

import scala.collection.JavaConversions._

class GremlinJsonProtocolTest extends FlatSpec with Matchers with TestingTitan with BeforeAndAfter {
  import org.trustedanalytics.atk.plugins.query.GremlinJsonProtocol._

  before {
    setupTitan()

    // Create schema before setting properties -- Needed in Titan 0.5.4+
    val graphManager = titanGraph.getManagementSystem()
    graphManager.makePropertyKey("name").dataType(classOf[String]).make()
    graphManager.makePropertyKey("age").dataType(classOf[Integer]).make()
    graphManager.makePropertyKey("weight").dataType(classOf[Integer]).make()
    graphManager.makeEdgeLabel("test").make()
    graphManager.commit()
  }

  after {
    cleanupTitan()
  }

  "GraphSONFormat" should "serialize a Blueprint's vertex into GraphSON" in {
    implicit val graphSONFormat = new GraphSONFormat(titanIdGraph)
    val vertex = titanIdGraph.addVertex(1)
    vertex.setProperty("name", "marko")
    vertex.setProperty("age", 29)

    val json = vertex.asInstanceOf[Element].toJson
    vertex should equalsGraphSONVertex(json)
  }

  "GraphSONFormat" should "serialize a Blueprint's edge into GraphSON" in {
    implicit val graphSONFormat = new GraphSONFormat(titanIdGraph)
    val vertex1 = titanIdGraph.addVertex(1)
    val vertex2 = titanIdGraph.addVertex(2)
    val edge = titanIdGraph.addEdge(3, vertex1, vertex2, "test")
    edge.setProperty("weight", 5)

    val json = edge.asInstanceOf[Element].toJson

    edge should equalsGraphSONEdge(json)
  }

  "GraphSONFormat" should "deserialize GraphSON into a Blueprint's vertex" in {
    implicit val graphSONFormat = new GraphSONFormat(titanIdGraph)

    val json = JsonParser("""{"name":"marko", "age":29, "_id":10, "_type":"vertex" }""")
    val vertex = json.convertTo[Element]

    vertex.asInstanceOf[Vertex] should equalsGraphSONVertex(json)
  }

  "GraphSONFormat" should "deserialize GraphSON into a Blueprint's edge" in {
    implicit val graphSONFormat = new GraphSONFormat(titanGraph)
    val vertex1 = titanGraph.addVertex(null)
    val vertex2 = titanGraph.addVertex(null)

    val json = JsonParser(s"""{"weight": 10, "_label":"test", "_outV":${vertex1.getId}, "_inV":${vertex2.getId}, "_type":"edge"}""")
    val edge = json.convertTo[Element]

    edge.asInstanceOf[Edge] should equalsGraphSONEdge(json)
  }

  "GraphSONFormat" should "throw an exception when deserializing invalid GraphSON" in {
    intercept[spray.json.DeserializationException] {
      implicit val graphSONFormat = new GraphSONFormat(titanIdGraph)
      val json = "[1, 2, 3]"
      JsonParser(json).convertTo[Element]
    }
  }

  "BlueprintsRowFormat" should "serialize a Blueprint's row into a JSON map" in {
    import org.trustedanalytics.atk.plugins.query.GremlinJsonProtocol._
    val rowMap = Map("col1" -> "val1", "col2" -> "val2")
    val row = new Row(rowMap.values.toList, rowMap.keys.toList)

    val json = row.toJson
    val jsonFields = json.asJsObject.fields

    jsonFields.keySet should contain theSameElementsAs rowMap.keySet
    jsonFields.values.toList should contain theSameElementsAs List(JsString("val1"), JsString("val2"))

  }

  "BlueprintsRowFormat" should "deserialize a JSON map to a Blueprint's row" in {
    import org.trustedanalytics.atk.plugins.query.GremlinJsonProtocol._
    val json = Map("col1" -> 1, "col2" -> 2).toJson
    val jsonFields = json.asJsObject.fields

    val row = json.convertTo[Row[Int]]

    row.getColumnNames should contain theSameElementsAs jsonFields.keySet
    row.getColumnNames.map(row.getColumn(_)) should contain theSameElementsAs List(1, 2)
  }

  "BlueprintsRowFormat" should "throw a deserialization exception when JSON is not a valid JSON map" in {
    intercept[spray.json.DeserializationException] {
      import org.trustedanalytics.atk.plugins.query.GremlinJsonProtocol._
      val json = """["test1", "test2"]"""
      JsonParser(json).convertTo[Row[String]]
    }
  }

  "isGraphElement" should "return true if GraphSON represents a vertex or edge" in {
    val vertexJson = JsonParser("""{"name":"saturn",  "_id":10, "_type":"vertex" }""")
    val edgeJson = JsonParser("""{"_label":"brother", "_inV":1, "_outV":2, "_type":"edge" }""")
    val invalidGraphSON = JsonParser("""{"test1" : 2}""")
    GremlinJsonProtocol.isGraphElement(vertexJson) should be(true)
    GremlinJsonProtocol.isGraphElement(edgeJson) should be(true)
    GremlinJsonProtocol.isGraphElement(invalidGraphSON) should be(false)
  }

}
