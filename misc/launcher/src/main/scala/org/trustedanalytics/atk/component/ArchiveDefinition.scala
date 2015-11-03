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


package org.trustedanalytics.atk.component

import com.typesafe.config.{ ConfigFactory, Config }
import org.apache.commons.lang.StringUtils

import scala.util.Try

/**
 * Encapsulates registration data for `Archive` instances.
 *
 * @param name the name of the Archive (e.g. the name of the jar, without the extension)
 * @param parent the name of the Archive whose class loader should be the parent for this
 *               archive's class loader
 * @param className the name of the class that will manage the archive
 * @param configPath the location of configuration information (in the config tree)
 *                   that applies to the archive
 */
case class ArchiveDefinition(name: String,
                             parent: String,
                             className: String,
                             configPath: String)

/**
 * Companion object, provides methods for constructing ArchiveDefinitions
 */
object ArchiveDefinition {
  /**
   * Constructs an ArchiveDefinition by reading it from a Config object
   * @param archiveName the name of the ArchiveDefinition to load
   * @param config the {Config} object that presumably has the information to load
   * @param configKeyBase the prefix string used to locate the archive definition in the Config object.
   *                      The final key is determined by the configKeyBase + archiveName concatenated.
   */
  def apply(archiveName: String,
            config: Config,
            configKeyBase: String = "trustedanalytics.atk.component.archives"): ArchiveDefinition = {
    val configKey = configKeyBase + "." + archiveName
    val restricted = Try {
      config.getConfig(configKey)
    }.getOrElse(
      {
        Archive.logger(s"No config found for '$configKey', using empty")
        ConfigFactory.empty()
      })
    val parent = Try {
      restricted.getString("parent")
    }.getOrElse({
      Archive.logger(s"Missing archive parent name for: " + archiveName + " using empty")
      StringUtils.EMPTY
    })
    val className = Try {
      restricted.getString("class")
    }.getOrElse({
      Archive.logger("No class entry found, using standard DefaultArchive class")
      "org.trustedanalytics.atk.component.DefaultArchive"
    })

    val configPath = Try {
      restricted.getString("config-path")
    }.getOrElse(
      {
        Archive.logger("No config-path found, using default")
        configKey
      })
    ArchiveDefinition(archiveName, parent, className, configPath)
  }

}
