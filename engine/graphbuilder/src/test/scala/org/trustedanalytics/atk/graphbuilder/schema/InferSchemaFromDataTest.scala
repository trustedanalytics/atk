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

package org.trustedanalytics.atk.graphbuilder.schema

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, Property, GBEdge }
import java.util.Date

class InferSchemaFromDataTest extends WordSpec with Matchers {

  "InferSchemaFromData" should {

    "infer one Edge label from one Edges" in {
      val edge = new GBEdge(None, null, null, "myLabel", Set.empty[Property])
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size shouldBe 1
      edgeLabelDefs.head.label shouldBe "myLabel"
    }

    "infer one Edge label from two Edges with the same label" in {
      val edge1 = new GBEdge(None, null, null, "myLabel", Set.empty[Property])
      val edge2 = new GBEdge(None, null, null, "myLabel", Set.empty[Property])
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size shouldBe 1
      edgeLabelDefs.head.label shouldBe "myLabel"
    }

    "infer two Edge labels from two Edges with different labels" in {
      val edge1 = new GBEdge(None, null, null, "myLabel", Set.empty[Property])
      val edge2 = new GBEdge(None, null, null, "secondLabel", Set.empty[Property])
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val edgeLabelDefs = inferSchemaFromData.graphSchema.edgeLabelDefs
      edgeLabelDefs.size shouldBe 2
      edgeLabelDefs.count(_.label == "myLabel") shouldBe 1
      edgeLabelDefs.count(_.label == "secondLabel") shouldBe 1
    }

    "infer no properties from an Edge when none are present" in {
      val edge = new GBEdge(None, null, null, null, Set.empty[Property])
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      inferSchemaFromData.graphSchema.propertyDefs.size shouldBe 0
    }

    "infer an Edge property from an Edge" in {
      val edge = new GBEdge(None, null, null, null, Set(new Property("key", "value")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val propertyDefs = inferSchemaFromData.graphSchema.propertyDefs
      propertyDefs.size shouldBe 1

      propertyDefs.head shouldBe new PropertyDef(PropertyType.Edge, "key", classOf[String], false, false)
    }

    "infer one Edge property from two Edges with the same property" in {
      val edge1 = new GBEdge(None, null, null, null, Set(new Property("key", "value1")))
      val edge2 = new GBEdge(None, null, null, null, Set(new Property("key", "value2")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge1)
      inferSchemaFromData.add(edge2)

      val propertyDefs = inferSchemaFromData.graphSchema.propertyDefs
      propertyDefs.size shouldBe 1

      propertyDefs.head shouldBe new PropertyDef(PropertyType.Edge, "key", classOf[String], false, false)
    }

    "infer two Edge properties from an Edge" in {
      val edge = new GBEdge(None, null, null, null, Set(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size shouldBe 2

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 shouldBe new PropertyDef(PropertyType.Edge, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 shouldBe new PropertyDef(PropertyType.Edge, "key2", classOf[Date], false, false)
    }

    "infer the gbId property from a Vertex" in {
      val vertex = new GBVertex(new Property("gbId", 10001L), Set.empty[Property])
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size shouldBe 1

      val propDef = schema.propertiesWithName("gbId").head
      propDef shouldBe new PropertyDef(PropertyType.Vertex, "gbId", classOf[java.lang.Long], true, true)
    }

    "infer a Vertex property from a Vertex" in {
      val vertex = new GBVertex(new Property("gbId", 10001L), Set(new Property("key", "value")))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size shouldBe 2

      val propDef1 = schema.propertiesWithName("gbId").head
      propDef1 shouldBe new PropertyDef(PropertyType.Vertex, "gbId", classOf[java.lang.Long], true, true)

      val propDef2 = schema.propertiesWithName("key").head
      propDef2 shouldBe new PropertyDef(PropertyType.Vertex, "key", classOf[String], false, false)
    }

    "infer two Vertex properties from a Vertex" in {
      val vertex = new GBVertex(new Property("gbId", 10001L), Set(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema
      schema.propertyDefs.size shouldBe 3

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 shouldBe new PropertyDef(PropertyType.Vertex, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 shouldBe new PropertyDef(PropertyType.Vertex, "key2", classOf[Date], false, false)
    }

    "combine results from Edges and Vertices" in {
      val edge = new GBEdge(None, null, null, "myLabel", Set(new Property("key3", "value3")))
      val vertex = new GBVertex(new Property("gbId", 10001L), Set(new Property("key1", "value1"), new Property("key2", new Date())))
      val inferSchemaFromData = new InferSchemaFromData()

      inferSchemaFromData.add(edge)
      inferSchemaFromData.add(vertex)

      val schema = inferSchemaFromData.graphSchema

      schema.edgeLabelDefs.size shouldBe 1
      schema.edgeLabelDefs.head.label shouldBe "myLabel"

      schema.propertyDefs.size shouldBe 4

      val propDef1 = schema.propertiesWithName("key1").head
      propDef1 shouldBe new PropertyDef(PropertyType.Vertex, "key1", classOf[String], false, false)

      val propDef2 = schema.propertiesWithName("key2").head
      propDef2 shouldBe new PropertyDef(PropertyType.Vertex, "key2", classOf[Date], false, false)

      val propDef3 = schema.propertiesWithName("key3").head
      propDef3 shouldBe new PropertyDef(PropertyType.Edge, "key3", classOf[String], false, false)
    }
  }

}
