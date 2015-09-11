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

package org.trustedanalytics.atk.engine.graph.plugins

import java.util
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderConfig
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.engine.graph.{ GraphBuilderConfigFactory, TestingTitanWithSparkWordSpec, SparkGraphStorage }
import org.trustedanalytics.atk.plugins.exporttotitan.ExportToTitanGraphPlugin
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec
import com.tinkerpop.blueprints.Direction
import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.joda.time.DateTime
import org.scalatest.Matchers
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConversions._

class ExportToTitanGraphPluginTest extends TestingTitanWithSparkWordSpec with Matchers with MockitoSugar {
  val edgeColumns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("startDate", DataTypes.string))
  val edgeSchema = new EdgeSchema(edgeColumns, "worksUnder", "srclabel", "destlabel", true)

  val employeeColumns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("employee", DataTypes.int64))
  val employeeSchema = new VertexSchema(employeeColumns, "employee", null)

  val divisionColumns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("divisionID", DataTypes.int64))
  val divisionSchema = new VertexSchema(divisionColumns, "division", null)

  "ExportToTitanGraph" should {
    "create an expected graphbuilder config " in {
      val plugin = new ExportToTitanGraphPlugin
      val config = plugin.createGraphBuilderConfig("backendName")
      config.titanConfig.getProperty("storage.hbase.table").toString should include("backendName")
      config.append should be(false)
      config.edgeRules.size should be(0)
      config.vertexRules.size should be(0)
    }

    "load a titan graph from an Edge and vertex RDD" in {
      val employees = List(
        new GenericRow(Array(1L, "employee", "Bob", 100L)),
        new GenericRow(Array(2L, "employee", "Joe", 101L)))
      val employeeRdd = sparkContext.parallelize[Row](employees)

      val employeeFrameRdd = new VertexFrameRdd(employeeSchema, employeeRdd)

      val divisions = List(new GenericRow(Array(3L, "division", "development", 200L)))
      val divisionRdd = sparkContext.parallelize[Row](divisions)
      val divisionFrameRdd = new VertexFrameRdd(employeeSchema, divisionRdd)

      val vertexFrame = employeeFrameRdd.toGbVertexRDD union divisionFrameRdd.toGbVertexRDD
      val works = List(
        new GenericRow(Array(4L, 1L, 3L, "worksIn", "10/15/2012")),
        new GenericRow(Array(5L, 2L, 3L, "worksIn", "9/01/2014")))

      val edgeRDD = sparkContext.parallelize[Row](works)
      val edgeFrameRdd = new EdgeFrameRdd(edgeSchema, edgeRDD)

      val edgeFrame = edgeFrameRdd.toGbEdgeRdd
      val edgeTaken = edgeFrame.take(10)

      val plugin = new ExportToTitanGraphPlugin
      val config = new GraphBuilderConfig(new InputSchema(List()),
        List(),
        List(),
        this.titanConfig)
      plugin.loadTitanGraph(config, vertexFrame, edgeFrame)

      // Need to explicitly specify type when getting vertices to resolve the error
      // "Helper method to resolve ambiguous reference error in TitanGraph.getVertices() in Titan 0.5.1+"
      val titanVertices: Iterable[com.tinkerpop.blueprints.Vertex] = this.titanGraph.getVertices
      this.titanGraph.getEdges().size should be(2)
      titanVertices.size should be(3)

      val bobVertex = this.titanGraph.getVertices(GraphSchema.vidProperty, 1l).iterator().next()
      bobVertex.getProperty[String]("name") should be("Bob")
      val bobsDivisionIterator = bobVertex.getVertices(Direction.OUT)
      bobsDivisionIterator.size should be(1)

      val bobsDivision = bobsDivisionIterator.iterator().next()
      bobsDivision.getProperty[String]("name") should be("development")

      val bobEdges = bobVertex.getEdges(Direction.OUT)

      bobEdges.size should be(1)
    }

    "unallowed titan naming elements will throw proper exceptions" in {
      intercept[IllegalArgumentException] {
        val edgeColumns1 = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("label1", DataTypes.string), Column("label3", DataTypes.string))
        val edgeSchema1 = new EdgeSchema(edgeColumns1, "label1", "srclabel", "destlabel")
        val frame1 = new FrameEntity(1, Some("name"), edgeSchema1, 0L, new DateTime, new DateTime)

        val edgeColumns2 = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("label2", DataTypes.string))
        val edgeSchema2 = new EdgeSchema(edgeColumns2, "label2", "srclabel", "destlabel")
        val frame2 = new FrameEntity(1, Some("name"), edgeSchema2, 0L, new DateTime, new DateTime)

        val edgeColumns3 = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("startDate", DataTypes.string))
        val edgeSchema3 = new EdgeSchema(edgeColumns3, "label3", "srclabel", "destlabel")
        val frame3 = new FrameEntity(1, Some("name"), edgeSchema3, 0L, new DateTime, new DateTime)

        val plugin: ExportToTitanGraphPlugin = new ExportToTitanGraphPlugin
        plugin.validateLabelNames(List(frame1, frame2, frame3), List("label1", "label2", "label3"))
      }
    }
    "no exception thrown if titan naming elements are valid" in {
      val frame1 = new FrameEntity(1, Some("name"), edgeSchema, 0L, new DateTime, new DateTime, graphId = Some(1L))

      val edgeColumns2 = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("startDate", DataTypes.string))
      val edgeSchema2 = new EdgeSchema(edgeColumns2, "label1", "srclabel", "destlabel")
      val frame2 = new FrameEntity(1, Some("name"), edgeSchema2, 0L, new DateTime, new DateTime, graphId = Some(1L))

      val plugin: ExportToTitanGraphPlugin = new ExportToTitanGraphPlugin
      plugin.validateLabelNames(List(frame1, frame2), List("notalabel1", "label2", "label3"))

    }
  }
}
