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

package org.trustedanalytics.atk.graphbuilder.write.dao

import org.trustedanalytics.atk.graphbuilder.elements.Property
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanIdUtils
import org.trustedanalytics.atk.graphbuilder.elements._
import org.trustedanalytics.atk.graphbuilder.write.titan.TitanIdUtils
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }
import org.trustedanalytics.atk.testutils.TestingTitan

class VertexDAOTest extends WordSpec with Matchers with TestingTitan with BeforeAndAfter {

  var vertexDAO: VertexDAO = null

  before {
    setupTitan()
    // Create schema before setting properties -- Needed in Titan 0.5.4+
    val graphManager = titanGraph.getManagementSystem()
    graphManager.makePropertyKey("gbId").dataType(classOf[Integer]).make()
    graphManager.makePropertyKey("name").dataType(classOf[String]).make()
    graphManager.commit()
    vertexDAO = new VertexDAO(titanGraph)
  }

  after {
    cleanupTitan()
    vertexDAO = null
  }

  "VertexDAO" should {

    "require a graph" in {
      an[IllegalArgumentException] should be thrownBy new VertexDAO(null)
    }

    "create a blueprints vertex from a graphbuilder vertex" in {
      val gbVertex = new GBVertex(new Property("gbId", 10001), Set.empty[Property])
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() shouldBe 1
      bpVertex.getProperty("gbId").asInstanceOf[Int] shouldBe 10001
    }

    "set properties on a newly created blueprints vertex" in {
      val gbVertex = new GBVertex(new Property("gbId", 10002), Set(new Property("name", "My Name")))
      val bpVertex = vertexDAO.create(gbVertex)
      bpVertex.getPropertyKeys.size() shouldBe 2
      bpVertex.getProperty("name").asInstanceOf[String] shouldBe "My Name"
    }

    "update properties on vertices" in {
      // setup data
      val gbVertexOriginal = new GBVertex(new Property("gbId", 10003), Set(new Property("name", "Original Name")))
      val gbVertexUpdated = new GBVertex(new Property("gbId", 10003), Set(new Property("name", "Updated Name")))
      val bpVertexOriginal = vertexDAO.create(gbVertexOriginal)

      // invoke method under test
      val bpVertexUpdated = vertexDAO.update(gbVertexUpdated, bpVertexOriginal)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Updated Name"
    }

    "update properties on vertices when no create is needed" in {
      // setup data
      val gbVertexOriginal = new GBVertex(new Property("gbId", 10004), Set(new Property("name", "Original Name")))
      val gbVertexUpdated = new GBVertex(new Property("gbId", 10004), Set(new Property("name", "Updated Name")))
      vertexDAO.create(gbVertexOriginal)
      titanGraph.commit()

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertexUpdated)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Updated Name"
    }

    "create vertices when create is needed" in {
      // setup data
      val gbVertex = new GBVertex(new Property("gbId", 10005), Set(new Property("name", "Original Name")))

      // invoke method under test
      val bpVertexUpdated = vertexDAO.updateOrCreate(gbVertex)

      // validate
      bpVertexUpdated.getPropertyKeys.size() shouldBe 2
      bpVertexUpdated.getProperty("name").asInstanceOf[String] shouldBe "Original Name"
    }

    "find a blueprints vertex using a physical titan id" in {
      // setup data
      val gbVertex = new GBVertex(new Property("gbId", 10006), Set.empty[Property])
      val createdBpVertex = vertexDAO.create(gbVertex)
      val id = TitanIdUtils.titanId(createdBpVertex)
      titanGraph.commit()

      // invoke method under test
      val foundBpVertex = vertexDAO.findByPhysicalId(id.asInstanceOf[AnyRef]).get

      // validate
      createdBpVertex shouldBe foundBpVertex
    }

    "handle null id's gracefully" in {
      val v = vertexDAO.findByGbId(null)
      v.isEmpty shouldBe true
    }

  }
}
