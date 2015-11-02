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

  private lazy val registry = loader.loadFromConfig()

  /** keys are plugin names, values are the plugins */
  private lazy val commandPlugins: Map[String, CommandPlugin[_, _]] = registry.map { case (module, plugin) => plugin.name -> plugin }.toMap

  /** keys are plugin names, values are the module names */
  private lazy val pluginsToModuleMap: Map[String, String] = registry.filter { case (module, plugin) => module.isDefined }.map { case (module, plugin) => plugin.name -> module.get.name }.toMap

  /**
   * Get command plugin by name
   * @param name command name
   * @return optional value of command plugin
   */
  def getCommandPlugin(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  /**
   * Returns all the command definitions registered with this command executor.
   *
   * NOTE: this is a val because the underlying operations are not thread-safe -- Todd 3/10/2015
   */
  lazy val commandDefinitions: Iterable[CommandDefinition] = {
    commandPlugins.values.map(p => {
      // It seems that the underlying operations are not thread-safe -- Todd 3/10/2015
      val argSchema = getArgumentsSchema(p)
      val retSchema = getReturnSchema(p)
      val doc = getCommandDoc(p)
      CommandDefinition(p.name, argSchema, retSchema, doc, p.apiMaturityTag)
    })
  }

  def getCommandDefinition(name: String): Option[CommandPlugin[_, _]] = {
    commandPlugins.get(name)
  }

  def moduleNameForPlugin(pluginName: String): String = {
    pluginsToModuleMap.getOrElse(pluginName, throw new RuntimeException(s"No module found for plugin named $pluginName"))
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
