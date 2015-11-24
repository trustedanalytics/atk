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

import com.typesafe.config.{ Config, ConfigFactory }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.moduleloader.Module
import scala.collection.JavaConverters._

/**
 * Load command plugin
 *
 * @param loadFromModules true to use the Module loader to search for plugins, false to use the current classloader
 */
class CommandLoader(loadFromModules: Boolean) {

  /**
   * Load plugins from the config
   */
  def loadFromConfig(): Iterable[(Option[Module], CommandPlugin[_, _])] = {
    if (loadFromModules) {
      // This is how the engine finds plugins
      Module.modules.flatMap(module => {
        val configs = module.getResources("atk-plugin.conf").map(url => ConfigFactory.parseURL(url))
        val classNames = getClassNames(configs)
        classNames.map(className => (Some(module), module.load(className)))
      })
    }
    else {
      // This is how plugins are loaded in SparkCommandJob (running in Yarn)
      val configs = this.getClass.getClassLoader.getResources("atk-plugin.conf").asScala.map(url => ConfigFactory.parseURL(url))
      val classNames = getClassNames(configs.toList)
      classNames.map(className => {
        (Option.empty[Module], this.getClass.getClassLoader.loadClass(className).newInstance().asInstanceOf[CommandPlugin[Product, Product]])
      }).toIterable
    }
  }

  private def getClassNames(configs: List[Config]): List[String] = {
    configs.flatMap(config => {
      if (config.hasPath("atk.plugin.command-plugins")) {
        config.getStringList("atk.plugin.command-plugins").asScala
      }
      else {
        Nil
      }
    })
  }
}