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

import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.component.{ Archive, Boot }

/**
 * Load command plugin
 */
class CommandLoader {
  /**
   * Load plugins from the config
   * @return mapping between name and plugin, mapping between name and archive's name the plugin was loaded from
   */
  def loadFromConfig(): CommandPluginRegistryMaps = {
    val commandPluginsWithArchiveName = EngineConfig.archives.flatMap {
      archive =>
        Archive.getArchive(archive)
          .getAll[CommandPlugin[_ <: Product, _ <: Product]]("command")
          .map(p => (p.name, p, archive))
    }
    CommandPluginRegistryMaps(
      commandPluginsWithArchiveName.map { case (pluginName, plugin, archive) => pluginName -> plugin }.toMap,
      commandPluginsWithArchiveName.map { case (pluginName, plugin, archive) => pluginName -> archive }.toMap
    )
  }
}
