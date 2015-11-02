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


package org.trustedanalytics.atk.graphbuilder.write.dao

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanIdUtils.titanId
import com.tinkerpop.blueprints.Direction
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }
import org.trustedanalytics.atk.testutils.TestingTitan

class EdgeDAOTest extends WordSpec with Matchers with TestingTitan with BeforeAndAfter {

  var vertexDAO: VertexDAO = null
  var edgeDAO: EdgeDAO = null

  before {
    setupTitan()

    // Create schema before setting properties -- Needed in Titan 0.5.4+
    val graphManager = titanGraph.getManagementSystem()
    graphManager.makePropertyKey("gbId").dataType(classOf[Integer]).make()
    graphManager.makePropertyKey("newKey").dataType(classOf[String]).make()
    graphManager.makePropertyKey("key1").dataType(classOf[String]).make()
    graphManager.makePropertyKey("key2").dataType(classOf[String]).make()
    graphManager.makeEdgeLabel("myLabel").make()
    graphManager.commit()

    vertexDAO = new VertexDAO(titanGraph)
    edgeDAO = new EdgeDAO(titanGraph, vertexDAO)
  }

  after {
    cleanupTitan()
    vertexDAO = null
    edgeDAO = null
  }

  "EdgeDAO" should {
    "require a graph" in {
      an[IllegalArgumentException] should be thrownBy new EdgeDAO(null, vertexDAO)
    }
  }

  "EdgeDAO create and update methods" should {

    "create a blueprints edge from a graphbuilder edge" in {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new GBVertex(new Property("gbId", 10001), Set.empty[Property]))
      vertexDAO.create(new GBVertex(new Property("gbId", 10002), Set.empty[Property]))

      // create input data
      val gbEdge = new GBEdge(None, new Property("gbId", 10001), new Property("gbId", 10002), "myLabel", Set.empty[Property])

      // invoke method under test
      val bpEdge = edgeDAO.create(gbEdge)

      // validate
      bpEdge.getVertex(Direction.IN).getProperty("gbId").asInstanceOf[Int] shouldBe 10002
      bpEdge.getVertex(Direction.OUT).getProperty("gbId").asInstanceOf[Int] shouldBe 10001
      bpEdge.getLabel shouldBe "myLabel"
    }

    "updateOrCreate when create is needed" in {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new GBVertex(new Property("gbId", 20001), Set.empty[Property]))
      vertexDAO.create(new GBVertex(new Property("gbId", 20002), Set.empty[Property]))

      // create input data
      val gbEdge = new GBEdge(None, new Property("gbId", 20001), new Property("gbId", 20002), "myLabel", Set.empty[Property])

      // invoke method under test
      val bpEdge = edgeDAO.updateOrCreate(gbEdge)

      // validate
      bpEdge.getVertex(Direction.IN).getProperty("gbId").asInstanceOf[Int] shouldBe 20002
      bpEdge.getVertex(Direction.OUT).getProperty("gbId").asInstanceOf[Int] shouldBe 20001
      bpEdge.getLabel shouldBe "myLabel"
    }

    "updateOrCreate when update is needed" in {
      // setup data dependency - vertices are needed before edges
      val gbId1 = new Property("gbId", 30001)
      val gbId2 = new Property("gbId", 30002)
      vertexDAO.create(new GBVertex(gbId1, Set.empty[Property]))
      vertexDAO.create(new GBVertex(gbId2, Set.empty[Property]))

      // create an edge
      val gbEdge = new GBEdge(None, gbId1, gbId2, "myLabel", Set.empty[Property])
      val bpEdgeOriginal = edgeDAO.create(gbEdge)
      titanGraph.commit()

      // define an updated version of the same edge
      val updatedEdge = new GBEdge(None, gbId1, gbId2, "myLabel", Set(new Property("newKey", "newValue")))

      // invoke method under test
      val bpEdgeUpdated = edgeDAO.updateOrCreate(updatedEdge)
      titanGraph.commit()

      titanId(edgeDAO.find(gbEdge).get) shouldBe titanId(edgeDAO.find(updatedEdge).get)

      // validate
      bpEdgeUpdated.getProperty("newKey").asInstanceOf[String] shouldBe "newValue"
    }

    "create should fail when tail vertex does not exist" in {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new GBVertex(new Property("gbId", 40002), Set.empty[Property]))

      // create input data
      val gbEdge = new GBEdge(None, new Property("gbId", 40001), new Property("gbId", 40002), "myLabel", Set.empty[Property])

      // invoke method under test
      an[IllegalArgumentException] should be thrownBy edgeDAO.create(gbEdge)
    }

    "create should fail when head vertex does not exist" in {
      // setup data dependency - vertices are needed before edges
      vertexDAO.create(new GBVertex(new Property("gbId", 50001), Set.empty[Property]))

      // create input data
      val gbEdge = new GBEdge(None, new Property("gbId", 50001), new Property("gbId", 50002), "myLabel", Set.empty[Property])

      // invoke method under test
      an[IllegalArgumentException] should be thrownBy edgeDAO.create(gbEdge)
    }

    "be able to update properties on existing edges" in {
      // setup data
      val gbId1 = new Property("gbId", 60001)
      val gbId2 = new Property("gbId", 60002)

      vertexDAO.create(new GBVertex(gbId1, Set.empty[Property]))
      vertexDAO.create(new GBVertex(gbId2, Set.empty[Property]))

      val bpEdge1 = edgeDAO.create(new GBEdge(None, gbId1, gbId2, "myLabel", Set(new Property("key1", "original"))))

      // invoke method under test
      edgeDAO.update(new GBEdge(None, gbId1, gbId2, "myLabel", Set(new Property("key2", "added"))), bpEdge1)

      // validate
      bpEdge1.getProperty("key1").asInstanceOf[String] shouldBe "original"
      bpEdge1.getProperty("key2").asInstanceOf[String] shouldBe "added"
    }
  }

  // before / after
  trait FindSetup {

    val gbId1 = new Property("gbId", 10001)
    val gbId2 = new Property("gbId", 10002)
    val gbId3 = new Property("gbId", 10003)
    val gbId4 = new Property("gbId", 10004)
    val gbIdNotInGraph = new Property("gbId", 99999)

    val v1 = vertexDAO.create(new GBVertex(gbId1, Set.empty[Property]))
    val v2 = vertexDAO.create(new GBVertex(gbId2, Set.empty[Property]))
    val v3 = vertexDAO.create(new GBVertex(gbId3, Set.empty[Property]))
    val v4 = vertexDAO.create(new GBVertex(gbId4, Set.empty[Property]))

    val label = "myLabel"

    val bpEdge1 = edgeDAO.create(new GBEdge(None, gbId1, gbId2, label, Set.empty[Property]))
    val bpEdge2 = edgeDAO.create(new GBEdge(None, gbId2, gbId3, label, Set.empty[Property]))
    val bpEdge3 = edgeDAO.create(new GBEdge(None, gbId2, gbId4, label, Set.empty[Property]))

    titanGraph.commit()
  }

  "EdgeDAO find methods" should {

    "find blueprints Edges using graphbuilder Edge definitions" in new FindSetup {
      val bpEdge = edgeDAO.find(new GBEdge(None, gbId1, gbId2, label, Set.empty[Property]))
      bpEdge.get shouldBe bpEdge1
    }

    "find blueprints Edges using graphbuilder GbId's and Label" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId2, gbId3, label)
      bpEdge.get shouldBe bpEdge2
    }

    "find blueprints Edges using blueprints Vertices and Label (a)" in new FindSetup {
      val bpEdge = edgeDAO.find(v1, v2, label)
      bpEdge.get shouldBe bpEdge1
    }

    "find blueprints Edges using blueprints Vertices and Label (b)" in new FindSetup {
      val bpEdge = edgeDAO.find(v2, v4, label)
      bpEdge.get shouldBe bpEdge3
    }

    "not find blueprints Edge when the Edge does not exist (a)" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId2, gbId1, label)
      bpEdge.isEmpty shouldBe true
    }

    "not find blueprints Edge when the Edge label does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId1, gbId2, "labelThatDoesNotExist")
      bpEdge.isEmpty shouldBe true
    }

    "not find blueprints Edge when the Edge does not exist (b)" in new FindSetup {
      val bpEdge = edgeDAO.find(v3, v4, label)
      bpEdge.isEmpty shouldBe true
    }

    "not find blueprints Edge when the tail Vertex does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(new GBEdge(None, gbIdNotInGraph, gbId2, label, Set.empty[Property]))
      bpEdge.isEmpty shouldBe true
    }

    "not find blueprints Edge when the head Vertex does not exist" in new FindSetup {
      val bpEdge = edgeDAO.find(gbId1, gbIdNotInGraph, label)
      bpEdge.isEmpty shouldBe true
    }
  }

}
