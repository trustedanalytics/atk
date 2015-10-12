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

package org.trustedanalytics.atk.moduleloader.internal

import java.net.{ URL, URLClassLoader }
import org.trustedanalytics.atk.moduleloader.Module
import scala.collection.mutable

/**
 * Search the searchPath, find all modules, load their config, and initializes them with ClassLoaders setup appropriately
 */
private[moduleloader] class ModuleLoader(searchPath: SearchPath) {

  /**
   * Search the searchPath, find all modules, load their config, and initializes them with ClassLoaders setup appropriately
   * @return map with module names as keys and the modules as values
   */
  private[moduleloader] def load(): Map[String, Module] = {
    val configs = loadModuleConfigs()
    configToModules(configs)
  }

  /**
   * Find modules using the searchPath and then load the ModuleConfigs while performing some initial validation
   * @return all of the module configs that were found
   */
  private[internal] def loadModuleConfigs(): Seq[ModuleConfig] = {
    val moduleFiles = searchPath.findModules()
    val moduleConfigs = moduleFiles.map(ModuleConfig.loadModuleConfig)
    validate(moduleConfigs)
    combineMemberOfModules(moduleConfigs)
  }

  /**
   * Perform some validation on the supplied list of ModuleConfigs
   */
  private[internal] def validate(configs: Seq[ModuleConfig]): Unit = {
    val moduleConfigs = toMap(configs)
    val moduleNames = moduleConfigs.keys.mkString(", ")

    // TODO: check for duplicate names (what about exploded jars)

    // Validate values in parent and memberOf
    moduleConfigs.values.foreach(module => {
      if (module.parentName.isDefined) {
        moduleConfigs.getOrElse(module.parentName.get, throw new IllegalArgumentException(s"$module had invalid parent name, choose from: $moduleNames"))
      }
      if (module.memberOf.isDefined) {
        moduleConfigs.getOrElse(module.memberOf.get, throw new IllegalArgumentException(s"$module had invalid memberOf, choose from: $moduleNames"))
      }
    })
  }

  private[internal] def combineMemberOfModules(configs: Seq[ModuleConfig]): Seq[ModuleConfig] = {
    var topLevelConfigs = toMap(configs.filter(_.memberOf.isEmpty))

    val memberOfConfigs = configs.filter(_.memberOf.isDefined)
    memberOfConfigs.foreach(config => {
      val topLevel = topLevelConfigs.getOrElse(config.memberOf.get, throw new IllegalArgumentException(s"Error: '${config.memberOf.get}' is not allowed to be a member-of modules that define member-of"))
      topLevelConfigs = topLevelConfigs.+((topLevel.name, topLevel.addMember(config)))
    })

    topLevelConfigs.values.toSeq
  }

  private[internal] def configToModules(configs: Seq[ModuleConfig]): Map[String, Module] = {

    val configsMap = toMap(configs)
    val classLoaders = new mutable.HashMap[String, URLClassLoader]()

    // 'system' is a special name reserved for the module-loader itself
    classLoaders.put(Module.SystemName, this.getClass.getClassLoader.asInstanceOf[URLClassLoader])

    def resolveClassLoader(name: String): URLClassLoader = {

      if (classLoaders.contains(name)) {
        val classLoader = classLoaders(name)
        if (classLoader == null) {
          throw new IllegalArgumentException(s"Module $name is part of a circular chain of parent relationships: " + configsMap.values.mkString(", "))
        }
        classLoader
      }
      else {
        // null used to make sure there isn't a circular chain of parent relationships
        classLoaders.put(name, null)

        val moduleConfig = configsMap(name)
        val urls = jarUrls(moduleConfig)
        val classLoader = moduleConfig.parentName match {
          case Some(parentName) => new URLClassLoader(urls, resolveClassLoader(parentName))
          case None => new URLClassLoader(urls)
        }

        classLoaders.put(name, classLoader)
        classLoader
      }
    }

    configsMap.values.map(moduleConfig => {
      val name = moduleConfig.name
      name -> moduleConfig.toModule(resolveClassLoader(name))
    }).toMap
  }

  private[internal] def jarUrls(moduleConfig: ModuleConfig): Array[URL] = {
    try {
      searchPath.findJars(moduleConfig.jarNames)
    }
    catch {
      case e: Exception =>
        try {
          val developerSearchPath = new SearchPath(moduleConfig.buildClasspath.mkString(":"))
          developerSearchPath.findJars(moduleConfig.jarNames)
        }
        catch {
          case e2: Exception => throw e
        }
    }
  }

  /**
   * Convert a list of configs to a Map where the keys are the config names
   */
  private def toMap(configs: Seq[ModuleConfig]): Map[String, ModuleConfig] = {
    configs.map(config => config.name -> config).toMap
  }
}
