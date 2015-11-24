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

import org.trustedanalytics.atk.graphbuilder.parser._
import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ DataTypeResolver, EdgeRule, ParsedValue, VertexRule }
import org.scalatest.{ Matchers, WordSpec }

class InferSchemaFromRulesTest extends WordSpec with Matchers {

  "InferSchemaFromRules when given enough rules to be able to infer" should {

    val columnDefs = List(
      new ColumnDef("userId", classOf[Long]),
      new ColumnDef("userName", classOf[String]),
      new ColumnDef("age", classOf[Int]),
      new ColumnDef("movieId", classOf[Long]),
      new ColumnDef("movieTitle", classOf[String]),
      new ColumnDef("rating", classOf[Int]),
      new ColumnDef("date", classOf[String]),
      new ColumnDef("emptyColumn", classOf[String]),
      new ColumnDef("noneColumn", null))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(
      VertexRule(property("userId"), List(constant("name") -> column("userName"))),
      VertexRule(property("movieId"), List(constant("title") -> column("movieTitle"))))
    val edgeRules = List(new EdgeRule(property("userId"), property("movieId"), constant("watched"), property("date")))

    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val schema = inferSchemaFromRules.inferGraphSchema()

    "infer the watched edge label" in {
      schema.edgeLabelDefs.size shouldBe 1
      schema.edgeLabelDefs.head.label shouldBe "watched"
    }

    "infer the correct number of properties" in {
      schema.propertyDefs.size shouldBe 5
    }

    "infer the date edge property" in {
      schema.propertiesWithName("date").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("date").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Edge, "date", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the userName vertex property" in {
      schema.propertiesWithName("name").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("name").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "name", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the movieTitle vertex property" in {
      schema.propertiesWithName("title").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("title").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "title", classOf[String], false, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the user vertex gbId property" in {
      schema.propertiesWithName("userId").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("userId").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "userId", classOf[Long], true, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "infer the movie vertex gbId property" in {
      schema.propertiesWithName("movieId").size shouldBe 1
      val actualPropertyDef = schema.propertiesWithName("movieId").head
      val expectedPropertyDef = new PropertyDef(PropertyType.Vertex, "movieId", classOf[Long], true, true)
      actualPropertyDef shouldBe expectedPropertyDef
    }

    "report it can infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe true
    }

    "report it can infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe true
    }

    "report it can infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe true
    }

  }

  "InferSchemaFromRules when given incomplete information in the vertex and edge rules" should {

    val columnDefs = List(
      new ColumnDef("id", classOf[Long]),
      new ColumnDef("dynamicPropertyName", classOf[String]), // can't be inferred
      new ColumnDef("dynamicPropertyValue", classOf[Int]),
      new ColumnDef("dynamicLabel", classOf[String]), // can't be inferred
      new ColumnDef("date", classOf[java.util.Date]))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(
      VertexRule(property("id"), List(column("dynamicPropertyName") -> column("dynamicPropertyValue"))))
    val edgeRules = List(new EdgeRule(property("id"), property("id"), column("dynamicLabel"), property("date")))
    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val graphSchema = inferSchemaFromRules.inferGraphSchema()

    "infer as many properties as possible" in {
      graphSchema.propertyDefs.size shouldBe 2
    }

    "NOT infer edge labels if they aren't available" in {
      graphSchema.edgeLabelDefs.size shouldBe 0
    }

    "report it can't infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe false
    }

    "report it can't infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe false
    }

    "report it can't infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe false
    }
  }

  "InferSchemaFromRules when given incomplete information in the edge rules" should {

    val columnDefs = List(
      new ColumnDef("id", classOf[Long]),
      new ColumnDef("dynamicPropertyName", classOf[String]), // can't be inferred
      new ColumnDef("dynamicPropertyValue", classOf[Int]),
      new ColumnDef("dynamicLabel", classOf[String]), // can't be inferred
      new ColumnDef("date", classOf[java.util.Date]))
    val inputSchema = new InputSchema(columnDefs)
    val dataTypeParser = new DataTypeResolver(inputSchema)
    val vertexRules = List(VertexRule(property("id"), Nil))
    val edgeRules = List(new EdgeRule(property("id"), property("id"), constant("myLabel"), column("dynamicPropertyName") -> column("date")))

    val inferSchemaFromRules = new InferSchemaFromRules(dataTypeParser, vertexRules, edgeRules)
    val graphSchema = inferSchemaFromRules.inferGraphSchema()

    "infer as many properties as possible" in {
      graphSchema.propertyDefs.size shouldBe 1
    }

    "infer edge labels if they are available" in {
      graphSchema.edgeLabelDefs.size shouldBe 1
    }

    "report it can infer edge labels" in {
      inferSchemaFromRules.canInferEdgeLabels shouldBe true
    }

    "report it can't infer all properties" in {
      inferSchemaFromRules.canInferAllPropertyKeyNames shouldBe false
    }

    "report it can't infer the schema" in {
      inferSchemaFromRules.canInferAll shouldBe false
    }
  }

  "InferScheamFromRules" should {

    "throw exception if safeValue() method gets wrong input type" in {
      val inferSchemaFromRules = new InferSchemaFromRules(null, null, null)
      an[RuntimeException] should be thrownBy inferSchemaFromRules.safeValue(new ParsedValue("parsed"))
    }
  }

}
