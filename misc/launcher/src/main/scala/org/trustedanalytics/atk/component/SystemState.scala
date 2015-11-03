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

/**
 * The system state for an application environment, typically managed globally by `Archive`
 */
class SystemState(val systemConfig: SystemConfig = new SystemConfig(),
                  archives: Map[String, Archive] = Map.empty) {
  /**
   * Creates a copy of this system configuration with the given root Config.
   */
  def withConfiguration(config: SystemConfig): SystemState = new SystemState(config, archives)

  /**
   * Look up the class loader for a given archive. Convenience method.
   */
  def loader(archiveName: String) = {
    archive(archiveName).map(_.classLoader)
  }

  /**
   * Look up an archive by name
   */
  def archive(archiveName: String) = {
    require(archiveName != null, "archiveName cannot be null")
    archives.get(archiveName)
  }

  /**
   * Generate a new system configuration with an additional archive included
   */
  def addArchive(archive: Archive) = {
    require(archive != null, "archive cannot be null")

    new SystemState(systemConfig, archives + (archive.definition.name -> archive))
  }

  def lookupArchiveNameByLoader(loader: ClassLoader): String = {
    val archiveName = for {
      archive <- archives
      if loader == archive._2.classLoader
    } yield archive._1
    archiveName.head
  }
}
