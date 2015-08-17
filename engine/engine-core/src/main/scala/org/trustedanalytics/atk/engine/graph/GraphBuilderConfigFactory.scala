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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderConfig
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ ConstantValue, ParsedValue, EdgeRule => GBEdgeRule, PropertyRule => GBPropertyRule, Value => GBValue, VertexRule => GBVertexRule }
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, InputSchema }
import org.trustedanalytics.atk.domain.frame.FrameName
import org.trustedanalytics.atk.domain.graph.construction.{ EdgeRule, PropertyRule, ValueRule, VertexRule, _ }
import org.trustedanalytics.atk.domain.graph.{ GraphName, GraphEntity, LoadGraphArgs }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.Schema
import org.trustedanalytics.atk.engine.EngineConfig
import com.typesafe.config.Config

/**
 * Converter that produces the graphbuilder3 consumable
 * org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilderConfig object from a GraphLoad command,
 * the schema of the source dataframe, and the metadata of the graph being written to.
 *
 * @param schema Schema of the source dataframe.
 * @param graphLoad The graph loading command.
 * @param graph Metadata for the graph being written to.
 */
class GraphBuilderConfigFactory(val schema: Schema, val graphLoad: LoadGraphArgs, graph: GraphEntity) {

  // TODO graphbuilder does not yet support taking multiple frames as input
  require(graphLoad.frameRules.size == 1, "only one frame rule per call is supported in this version")

  val theOnlyFrameRule = graphLoad.frameRules.head

  val graphConfig: GraphBuilderConfig = {
    new GraphBuilderConfig(getInputSchema(schema),
      getGBVertexRules(theOnlyFrameRule.vertexRules),
      getGBEdgeRules(theOnlyFrameRule.edgeRules),
      GraphBuilderConfigFactory.getTitanConfiguration(graph.storage),
      append = graphLoad.append,
      // The retainDanglingEdges option doesn't make sense for Python Layer because of how the rules get defined
      retainDanglingEdges = false,
      inferSchema = true,
      broadcastVertexIds = false)
  }

  /**
   * Converts org.trustedanalytics.atk.domain.schema.Schema into org.trustedanalytics.atk.graphbuilder.parser.InputSchema
   * @param schema The dataframe schema to be converted.
   * @return Dataframe schema as a org.trustedanalytics.atk.graphbuilder.parser.InputSchema
   */
  private def getInputSchema(schema: Schema): InputSchema = {

    val columns: List[ColumnDef] = schema.columnTuples map { case (name: String, dataType: DataType) => new ColumnDef(name, dataType.scalaType) }

    new InputSchema(columns)
  }

  /**
   * Converts org.trustedanalytics.atk.domain.graphconstruction.Value into the GraphBuilder3 consumable
   * org.trustedanalytics.atk.graphbuilder.parser.rule.Value
   *
   * @param value A value from a graph load's parsing rules.
   * @return A org.trustedanalytics.atk.graphbuilder.parser.rule.Value
   */
  private def getGBValue(value: ValueRule): GBValue = {
    if (value.source == GBValueSourcing.CONSTANT) {
      new ConstantValue(value.value)
    }
    else {
      new ParsedValue(value.value)
    }
  }

  /**
   * Converts {org.trustedanalytics.atk.domain.graphconstruction.Property} into the GraphBuilder3 consumable
   * org.trustedanalytics.atk.graphbuilder.parser.rule.PropertyRule
   * @param property A property rule from a graph load's parsing rules.
   * @return A org.trustedanalytics.atk.graphbuilder.parser.rule.PropertyRule
   */
  private def getGBPropertyRule(property: PropertyRule): GBPropertyRule = {
    new GBPropertyRule(getGBValue(property.key), getGBValue(property.value))
  }

  /**
   * Converts org.trustedanalytics.atk.domain.graphconstruction.VertexRule to GraphBuilder3 consumable
   * org.trustedanalytics.atk.graphbuilder.parser.rule.VertexRule
   * @param vertexRule A vertex rule from a graph load's parsing rules.
   * @return A org.trustedanalytics.atk.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRule(vertexRule: VertexRule): GBVertexRule = {
    new GBVertexRule(getGBPropertyRule(vertexRule.id), vertexRule.properties.map(getGBPropertyRule))
  }

  /**
   * Converts a list of org.trustedanalytics.atk.domain.graphconstruction.VertexRule's into a list of
   * GraphBuilder3 consumable org.trustedanalytics.atk.graphbuilder.parser.rule.VertexRule's
   * @param vertexRules A list of vertex rules from a graph load's parsing rules.
   * @return A list of org.trustedanalytics.atk.domain.graphconstruction.VertexRule
   */
  private def getGBVertexRules(vertexRules: List[VertexRule]): List[GBVertexRule] = {
    vertexRules map getGBVertexRule
  }

  /**
   * Converts org.trustedanalytics.atk.domain.graphconstruction.EdgeRule to GraphBuilder3 consumable
   * org.trustedanalytics.atk.graphbuilder.parser.rule.EdgeRule
   * @param edgeRule An edge rule from a graph load's parsing rules.
   * @return A org.trustedanalytics.atk.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRule(edgeRule: EdgeRule): GBEdgeRule = {
    new GBEdgeRule(getGBPropertyRule(edgeRule.tail), getGBPropertyRule(edgeRule.head),
      getGBValue(edgeRule.label), edgeRule.properties.map(getGBPropertyRule), biDirectional = edgeRule.bidirectional)
  }

  /**
   * Converts a list of org.trustedanalytics.atk.domain.graphconstruction.EdgeRule's into a list of
   * GraphBuilder3 consumable org.trustedanalytics.atk.graphbuilder.parser.rule.EdgeRule's
   * @param edgeRules A list of edge rules from a graph load's parsing rules.
   * @return A list of org.trustedanalytics.atk.domain.graphconstruction.EdgeRule
   */
  private def getGBEdgeRules(edgeRules: List[EdgeRule]): List[GBEdgeRule] = {
    edgeRules map getGBEdgeRule
  }

}

object GraphBuilderConfigFactory {
  /**
   * Produces graphbuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration from
   * a graph name and a org.trustedanalytics.atk.domain.graphconstruction.outputConfiguration
   * @param backendStorageName Name of the graph to be written to.
   *
   * @return GraphBuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
   */
  def getTitanConfiguration(backendStorageName: String): SerializableBaseConfiguration = {

    // load settings from titan.conf file...
    // ... the configurations are Java objects and the conversion requires jumping through some hoops...
    val titanConfiguration = EngineConfig.titanLoadConfiguration
    val titanGraphNameKey = getTitanGraphNameKey(titanConfiguration)
    titanConfiguration.setProperty(titanGraphNameKey, backendStorageName)
    titanConfiguration
  }

  /**
   * Produces graphbuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration from
   * a graph name and a org.trustedanalytics.atk.domain.graphconstruction.outputConfiguration
   * @param commandConfig Configuration object for command.
   * @param titanPath Dot-separated expressions with Titan config, e.g., titan.load
   * @param backendStorageName Name of the graph to be written to.
   *
   * @return GraphBuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
   */
  def getTitanConfiguration(commandConfig: Config, titanPath: String, backendStorageName: String): SerializableBaseConfiguration = {

    // load settings from titan.conf file...
    // ... the configurations are Java objects and the conversion requires jumping through some hoops...
    val titanConfiguration = EngineConfig.createTitanConfiguration(commandConfig, titanPath)
    val titanGraphNameKey = getTitanGraphNameKey(titanConfiguration)
    titanConfiguration.setProperty(titanGraphNameKey, backendStorageName)
    titanConfiguration
  }

  /**
   * Produces graphbuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration from
   * a graph entity
   * @param graph Graph Entity
   * @return GraphBuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
   */
  def getTitanConfiguration(graph: GraphEntity): SerializableBaseConfiguration = {
    val backendStorageName = graph.storage
    getTitanConfiguration(backendStorageName)
  }

  /**
   * Get graph name from Titan configuration based on the storage backend.
   *
   * @param titanConfig Titan configuration
   * @return Graph name
   */
  def getTitanGraphName(titanConfig: SerializableBaseConfiguration): String = {
    val titanGraphNameKey = getTitanGraphNameKey(titanConfig)
    titanConfig.getString(titanGraphNameKey)
  }

  /**
   * Get graph name configuration key based on the storage backend.
   *
   * Titan uses different options for specifying the graph name based on the backend. For example,
   * "storage.hbase.table" for HBase, and "storage.cassandra.keyspace" for Cassandra.
   *
   * @param titanConfig Titan configuration
   * @return Graph name key for backend (either "storage.hbase.table" or "storage.cassandra.keyspace")
   */
  private def getTitanGraphNameKey(titanConfig: SerializableBaseConfiguration): String = {
    val storageBackend = titanConfig.getString("storage.backend")

    storageBackend.toLowerCase match {
      case "hbase" => "storage.hbase.table"
      case "cassandra" => "storage.cassandra.keyspace"
      case _ => throw new RuntimeException("Unsupported storage backend for Titan. Please set storage.backend to hbase or cassandra")
    }
  }

}
