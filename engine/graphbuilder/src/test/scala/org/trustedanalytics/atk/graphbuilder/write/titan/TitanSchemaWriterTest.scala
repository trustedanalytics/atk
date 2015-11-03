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


package org.trustedanalytics.atk.graphbuilder.write.titan

import org.trustedanalytics.atk.graphbuilder.schema.{ EdgeLabelDef, GraphSchema, PropertyDef, PropertyType }
import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.blueprints.{ Direction, Edge, Vertex }
import org.mockito.Mockito._
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfter, Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.testutils.TestingTitan

class TitanSchemaWriterTest extends WordSpec with Matchers with MockitoSugar with TestingTitan with BeforeAndAfter {

  var titanSchemaWriter: TitanSchemaWriter = null

  before {
    setupTitan()
    titanSchemaWriter = new TitanSchemaWriter(titanGraph)
  }

  after {
    cleanupTitan()
    titanSchemaWriter = null
  }

  "TitanSchemaWriter" should {

    "write an edge label definition" in {
      // setup

      val edgeLabel = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      val titanManager = titanGraph.getManagementSystem
      titanManager.getEdgeLabel("myLabel").isEdgeLabel shouldBe true
      titanManager.commit()
    }

    "ignore duplicate edge label definitions" in {
      // setup
      val edgeLabel = new EdgeLabelDef("myLabel")
      val edgeLabelDup = new EdgeLabelDef("myLabel")
      val schema = new GraphSchema(List(edgeLabel, edgeLabelDup), Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      val titanManager = titanGraph.getManagementSystem
      titanManager.getEdgeLabel("myLabel").isEdgeLabel shouldBe true
      titanManager.commit()
    }

    "write a property definition" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      val titanManager = titanGraph.getManagementSystem
      titanGraph.getRelationType("propName").isPropertyKey shouldBe true
      titanManager.getGraphIndex("propName") shouldBe null
      titanManager.commit()
    }

    "write a property definition that is unique and indexed" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = true, indexed = true)
      val schema = new GraphSchema(Nil, List(propertyDef))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      val titanManager = titanGraph.getManagementSystem
      titanGraph.getRelationType("propName").isPropertyKey shouldBe true
      titanManager.getGraphIndex("propName").isUnique() shouldBe true
      titanManager.commit()
    }

    "ignore duplicate property definitions" in {
      val propertyDef = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val propertyDup = new PropertyDef(PropertyType.Vertex, "propName", classOf[String], unique = false, indexed = false)
      val schema = new GraphSchema(Nil, List(propertyDef, propertyDup))

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      val titanManager = titanGraph.getManagementSystem
      titanGraph.getRelationType("propName").isPropertyKey shouldBe true
      titanManager.getGraphIndex("propName") shouldBe null
      titanManager.commit()
    }

    "handle empty lists" in {
      val schema = new GraphSchema(Nil, Nil)

      // invoke method under test
      titanSchemaWriter.write(schema)

      // validate
      titanGraph.getRelationType("propName") should be(null)
    }

    "require a graph" in {
      an[IllegalArgumentException] should be thrownBy new TitanSchemaWriter(null)
    }

    "require an open graph" in {
      // setup mocks
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(false)

      // invoke method under test
      an[IllegalArgumentException] should be thrownBy new TitanSchemaWriter(graph)
    }

    "determine indexType for Edges " in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      new TitanSchemaWriter(graph).indexType(PropertyType.Edge) shouldBe classOf[Edge]
    }

    "determine indexType for Vertices " in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      new TitanSchemaWriter(graph).indexType(PropertyType.Vertex) shouldBe classOf[Vertex]
    }

    "fail for unexpected types" in {
      val graph = mock[TitanGraph]
      when(graph.isOpen).thenReturn(true)
      an[RuntimeException] should be thrownBy new TitanSchemaWriter(graph).indexType(new PropertyType.Value {
        override def id: Int = -99999999
      })
    }
  }
}
