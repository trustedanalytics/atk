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

package org.trustedanalytics.atk.engine.command

import org.trustedanalytics.atk.domain.command.{ CommandDocLoader, CommandDefinition, CommandDoc }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.shared.JsonSchemaExtractor
import org.trustedanalytics.atk.spray.json.{ JsonSchema, ObjectSchema }
import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * Register and store command plugin
 */
class CommandPluginRegistry(loader: CommandLoader) {

  private val registry: CommandPluginRegistryMaps = loader.loadFromConfig()
  private def commandPlugins = registry.commandPlugins
  private def pluginsToArchiveMap = registry.pluginsToArchiveMap

  /**
   * Get command plugin by name
   * @param name command name
   * @return optional value of command plugin
   */
  def getCommandPlugin(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  /**
   * Adds the given command to the registry.
   * @param command the command to add
   * @return the same command that was passed, for convenience
   */
  def registerCommand[A <: Product, R <: Product](command: CommandPlugin[A, R]): CommandPlugin[A, R] = {
    synchronized {
      registry.commandPlugins += (command.name -> command)
    }
    command
  }

  /**
   * Returns all the command definitions registered with this command executor.
   *
   * NOTE: this is a val because the underlying operations are not thread-safe -- Todd 3/10/2015
   */
  lazy val commandDefinitions: Iterable[CommandDefinition] =
    commandPlugins.values.map(p => {
      // It seems that the underlying operations are not thread-safe -- Todd 3/10/2015
      val argSchema = getArgumentsSchema(p)
      val retSchema = getReturnSchema(p)
      val doc = getCommandDoc(p)
      CommandDefinition(p.name, argSchema, retSchema, doc, p.apiMaturityTag)
    })

  def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  // Get archive name for the plugin. If it does not exist in the map, check if it is in commandPlugins as a valid plugin
  def getArchiveNameFromPlugin(name: String): Option[String] = {
    pluginsToArchiveMap.get(name)
  }

  private def getArgumentsSchema(p: CommandPlugin[_, _]): ObjectSchema = {
    JsonSchemaExtractor.getProductSchema(p.argumentManifest, includeDefaultValues = true)
  }

  private def getReturnSchema(p: CommandPlugin[_, _]): JsonSchema = {

    // if plugin annotation has a returns description, add it to the schema
    val description = JsonSchemaExtractor.getPluginDocAnnotation(p.thisManifest) match {
      case None => None
      case Some(pluginDoc) => pluginDoc.getReturnsDescription
    }

    JsonSchemaExtractor.getReturnSchema(p.returnManifest, description = description)
  }

  private def getCommandDoc(p: CommandPlugin[_, _]): Option[CommandDoc] = {
    val examples: Option[Map[String, String]] = CommandDocLoader.getCommandDocExamples(p.name)
    JsonSchemaExtractor.getPluginDocAnnotation(p.thisManifest) match {
      case Some(pluginDoc) => Some(CommandDoc(pluginDoc.oneLine, Some(pluginDoc.extended), examples))
      case None => None
    }
  }
}

case class CommandPluginRegistryMaps(var commandPlugins: Map[String, CommandPlugin[_, _]], var pluginsToArchiveMap: Map[String, String])
