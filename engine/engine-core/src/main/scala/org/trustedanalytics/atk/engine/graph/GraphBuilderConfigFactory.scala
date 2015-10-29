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


package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.trustedanalytics.atk.engine.EngineConfig
import com.typesafe.config.Config

object GraphBuilderConfigFactory {
  /**
   * Produces graphbuilder3 consumable org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration from
   * a graph name and a org.trustedanalytics.atk.domain.graphconstruction.outputConfiguration
   * @param backendStorageName Name of the graph to be written to.
   *
   * @return GraphBuilder3 consumable org.truststedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
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
