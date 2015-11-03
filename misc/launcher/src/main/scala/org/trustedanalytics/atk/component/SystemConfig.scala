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

import java.io.File

import com.typesafe.config.{ ConfigResolveOptions, ConfigFactory, Config }

import scala.collection.JavaConverters._

/**
 * The configuration for an application environment, typically managed globally by {Archive}
 */
class SystemConfig(val rootConfiguration: Config = ConfigFactory.load(SystemConfig.getClass.getClassLoader,
                     //Allow unresolved subs because user may have specified subs on the command line
                     //that can't be resolved yet until other archives are loaded
                     ConfigResolveOptions.defaults().setAllowUnresolved(true))) {

  def extraClassPath(archivePath: String): Array[String] = {
    Archive.logger(s"Checking archive path $archivePath for extra classpath")
    val path = archivePath + ".extra-classpath"
    val result = getStrings(path)
    Archive.logger(s"Checking archive path $archivePath for extra classpath - result: [${result.mkString(", ")}]")
    result
  }

  def getStrings(path: String): Array[String] = {
    if (rootConfiguration.hasPath(path)) {
      rootConfiguration.getStringList(path).asScala.toArray
    }
    else {
      Array.empty
    }
  }

  val debugConfig = rootConfiguration.getBoolean(SystemConfig.debugConfigKey)

  val debugConfigFolder = rootConfiguration.getString(SystemConfig.debugConfigPrefix) + java.util.UUID.randomUUID.toString + File.separator

  val jarFolders = rootConfiguration.getStringList(SystemConfig.jarFolders).asScala.toArray

  val sourceRoots = rootConfiguration.getStringList(SystemConfig.sourceRoots).asScala.toArray

}

object SystemConfig {

  private[component] val debugConfigKey: String = "trustedanalytics.atk.launcher.debug-config.enabled"

  private[component] val debugConfigPrefix: String = "trustedanalytics.atk.launcher.debug-config.prefix"

  private[component] val jarFolders = "trustedanalytics.atk.launcher.jar-folders"

  private[component] val sourceRoots = "trustedanalytics.atk.launcher.source-roots"

}
