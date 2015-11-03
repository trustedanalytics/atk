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


package org.trustedanalytics.atk.graphbuilder.driver.spark

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ EdgeRule, VertexRule }
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, InputSchema }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.testutils.TestingTitan
import com.tinkerpop.blueprints.Direction
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfter, Matchers }
import org.trustedanalytics.atk.testutils.{ TestingTitan, TestingSparkContextWordSpec }

import scala.collection.JavaConversions._

/**
 * End-to-end Integration Test
 */
class GraphBuilderITest extends TestingSparkContextWordSpec with Matchers with TestingTitan with BeforeAndAfter {

  before {
    setupTitan()
  }

  after {
    cleanupTitan()
  }

  "GraphBuilder" should {

    "support an end-to-end flow of the numbers example with append" in {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Connect to the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val titanConnector = new TitanGraphConnector(titanConfig)

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      titanGraph.getEdges.size shouldBe 5
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 5 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      // need to shutdown because only one connection can be open at a time
      titanGraph.shutdown()

      // Now we'll append to the existing graph

      // define more input
      val additionalInputRows = List(
        List("5", "{(1)}", "101", "Y", "5", "Y"), // this row overlaps with above
        List("6", "{(1),(2),(3)}", "110", "N", "6", "Y"),
        List("7", "{(1)}", "111", "Y", "7", "Y"))

      val inputRdd2 = sparkContext.parallelize(additionalInputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Append to the existing Graph
      val gb2 = new GraphBuilder(config.copy(append = true, retainDanglingEdges = true, inferSchema = false))
      gb2.build(inputRdd2)

      // Validate
      titanGraph = titanConnector.connect()
      titanGraph.getEdges.size shouldBe 7
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 7

    }
    "support an end-to-end flow of the numbers example with append using broadcast variables" in {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Connect to the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val titanConnector = new TitanGraphConnector(titanConfig)

      // Build the Graph
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, broadcastVertexIds = true)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      titanGraph.getEdges.size shouldBe 5
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 5 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      // need to shutdown because only one connection can be open at a time
      titanGraph.shutdown()

      // Now we'll append to the existing graph

      // define more input
      val additionalInputRows = List(
        List("5", "{(1)}", "101", "Y", "5", "Y"), // this row overlaps with above
        List("6", "{(1),(2),(3)}", "110", "N", "6", "Y"),
        List("7", "{(1)}", "111", "Y", "7", "Y"))

      val inputRdd2 = sparkContext.parallelize(additionalInputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Append to the existing Graph
      val gb2 = new GraphBuilder(config.copy(append = true, retainDanglingEdges = true, inferSchema = false, broadcastVertexIds = true))
      gb2.build(inputRdd2)

      // Validate
      titanGraph = titanConnector.connect()
      titanGraph.getEdges.size shouldBe 7
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 7

    }

    "support unusual cases of dynamic parsing, dangling edges" in {

      // Input Data
      val inputRows = List(
        List("userId", 1001L, "President Obama", "", "", ""), // a vertex that is a user named President Obama
        List("movieId", 1001L, "When Harry Met Sally", "", "", ""), // a vertex representing a movie
        List("movieId", 1002L, "Frozen", "", "", ""),
        List("movieId", 1003L, "The Hobbit", "", "", ""),
        List("", "", "", 1001L, "likes", 1001L), // edge that means "Obama likes When Harry Met Sally"
        List("userId", 1004L, "Abraham Lincoln", "", "", ""),
        List("", "", "", 1004L, "likes", 1001L), // edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "likes", 1001L), // duplicate edge that means "Lincoln likes When Harry Met Sally"
        List("", "", "", 1004L, "hated", 1002L), // edge that means "Lincoln hated Frozen"
        List("", "", "", 1001L, "hated", 1003L), // edge that means "Obama hated The Hobbit"
        List("", "", "", 1001L, "hated", 1007L) // edge that means "Obama hated a movie that is a dangling edge"
      )

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("idType", classOf[String]), // describes the next column, if it is a movieId or userId
        new ColumnDef("id", classOf[java.lang.Long]),
        new ColumnDef("name", classOf[String]), // movie title or user name
        new ColumnDef("userIdOfRating", classOf[String]),
        new ColumnDef("liking", classOf[String]),
        new ColumnDef("movieIdOfRating", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(column("idType") -> column("id"), List(property("name"))))

      val edgeRules = List(
        EdgeRule(constant("userId") -> column("userIdOfRating"), constant("movieId") -> column("movieIdOfRating"), column("liking"), Nil),
        EdgeRule(constant("movieId") -> column("movieIdOfRating"), constant("userId") -> column("userIdOfRating"), "was-watched-by", Nil))

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // Build the Graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val config = new GraphBuilderConfig(inputSchema, vertexRules, edgeRules, titanConfig, retainDanglingEdges = true)
      val gb = new GraphBuilder(config)
      gb.build(inputRdd)

      // Validate
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 6 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+
      titanGraph.getEdges.size shouldBe 12

      val obama = titanGraph.getVertices("userId", 1001L).iterator().next()
      obama.getProperty("name").asInstanceOf[String] shouldBe "President Obama"
      obama.getEdges(Direction.OUT).size shouldBe 3
    }

    "support inferring schema from the data" in {

      // Input data, as edge list
      val inputEdges = List(
        "1 2",
        "1 3",
        "1 4",
        "2 1",
        "2 5",
        "3 1",
        "3 4",
        "3 6",
        "3 7",
        "3 8",
        "4 1",
        "4 3",
        "5 2",
        "5 6",
        "5 7",
        "6 3",
        "6 5",
        "7 3",
        "7 5",
        "8 3")

      val inputRows = sparkContext.parallelize(inputEdges)

      // Create edge set RDD, make up properties
      val inputRdd = inputRows.map(row => row.split(" "): Seq[String])
      val edgeRdd = inputRdd.map(e => new GBEdge(None, new Property("userId", e(0)), new Property("userId", e(1)), "tweeted", Set(new Property("tweet", "blah blah blah..."))))

      // Create vertex set RDD, make up properties
      val rawVertexRdd = inputRdd.flatMap(row => row).distinct()
      val vertexRdd = rawVertexRdd.map(v => new GBVertex(new Property("userId", v), Set(new Property("location", "Oregon"))))

      // Build the graph
      val titanConfig = new SerializableBaseConfiguration()
      titanConfig.copy(titanBaseConfig)
      val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig))
      gb.buildGraphWithSpark(vertexRdd, edgeRdd)

      // Validate
      val titanConnector = new TitanGraphConnector(titanConfig)
      titanGraph = titanConnector.connect()

      titanGraph.getEdges.size shouldBe 20
      TitanGraphConnector.getVertices(titanGraph).size shouldBe 8 //Need wrapper due to ambiguous reference errors in Titan 0.5.1+

      val vertexOne = titanGraph.getVertices("userId", "1").iterator().next()
      vertexOne.getProperty("location").asInstanceOf[String] shouldBe "Oregon"
      vertexOne.getEdges(Direction.OUT).size shouldBe 3
      vertexOne.getEdges(Direction.OUT).iterator().next().getProperty("tweet").asInstanceOf[String] shouldBe "blah blah blah..."
    }

  }
}
