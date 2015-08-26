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

import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import org.trustedanalytics.atk.moduleloader.Module
import scala.collection.JavaConversions._

/**
 * Config for a single Module.
 *
 * This is basically a wrapper for a Typesafe Config initialized with atk-module.conf, atk-module-*.conf
 *
 * @param moduleLocation jar or directory where config was loaded from
 * @param config the raw typesafe config
 */
private[internal] case class ModuleConfig(moduleLocation: String, config: Config, members: Seq[ModuleConfig] = Nil) {

  // validation
  require(name != null, s"$toString Module name cannot be null")
  require(name.nonEmpty, s"$toString Module name cannot be empty")
  require(name.replaceAll(" ", "").nonEmpty, s"$toString Module name cannot be blank spaces")
  require(parentName.isEmpty || memberOf.isEmpty, s"$toString A module cannot define both a member-of and a parent, only one or the other is allowed")
  require(Option(name) != parentName, s"$toString A module cannot be its own parent")
  require(Option(name) != memberOf, s"$toString A module cannot be a member-of itself")
  if (memberOf.nonEmpty) {
    require(members.isEmpty, s"$toString A module that is a member-of another module cannot accept members")
  }
  // 'system' is a special name reserved for the module-loader itself
  if (name == Module.SystemName) {
    require(parentName.isEmpty, s"$toString The 'system' module cannot have a parent because the ClassLoader has already been initialized")
    require(memberOf.isEmpty, s"$toString The 'system' module cannot be a member-of because the ClassLoader has already been initialized")
    require(members.isEmpty, s"$toString The 'system' module cannot have members: $members because the ClassLoader has already been initialized")
  }

  /** Name of the module */
  lazy val name = config.getString("atk.module.name")

  /** Name of this modules parent */
  lazy val parentName: Option[String] = optionString("atk.module.parent")

  /** Name of the module this module is a member of */
  lazy val memberOf: Option[String] = optionString("atk.module.member-of")

  /**
   * The names of the jars this module will need available in its ClassLoader
   */
  lazy val jarNames: Seq[String] = {
    // TODO: add moduleLocation to list?
    val myJarNames = if (config.hasPath("atk.module.jar-names")) {
      config.getStringList("atk.module.jar-names").toSeq
    }
    else {
      Nil
    }
    val memberJarNames = members.flatMap(_.jarNames)
    (myJarNames ++ memberJarNames).distinct
  }

  lazy val commandPlugins: Seq[String] = {
    val myPlugins = if (config.hasPath("atk.module.command-plugins")) {
      config.getStringList("atk.module.command-plugins").toSeq
    }
    else {
      Nil
    }
    val memberPlugins = members.flatMap(_.commandPlugins)
    myPlugins ++ memberPlugins
  }

  override def toString: String = {
    s"ModuleConfig(name:$name, fileLocation:$moduleLocation, parentName:$parentName, memberOf:$memberOf)"
  }

  /**
   * Add a members moduleConfig to this module
   * @param moduleConfig the moduleConfig to add
   * @return the updated version of this module
   */
  def addMember(moduleConfig: ModuleConfig): ModuleConfig = {
    require(moduleConfig.memberOf.isDefined, "trying to add a module that doesn't have a member-of defined")
    require(moduleConfig.memberOf.get == name, s"trying to add a module who has a different member-of defined $name != ${moduleConfig.memberOf.get}")
    copy(members = members :+ moduleConfig)
  }

  /**
   * Use this config and the supplied classLoader to instantiate a Module
   */
  def toModule(classLoader: ClassLoader): Module = {
    new Module(name, parentName, jarNames, commandPlugins, classLoader)
  }

  /**
   * Helper method for getting a String out of config, if it exists
   */
  private def optionString(key: String): Option[String] = {
    if (config.hasPath(key)) {
      Some(config.getString(key))
    }
    else {
      None
    }
  }

}

private[internal] object ModuleConfig {

  /**
   * Load a ModuleConfig from
   * @param moduleFile jar or directory containing an atk-module.conf
   * @return the loaded configuration for a Module
   */
  def loadModuleConfig(moduleFile: File): ModuleConfig = {
    val fileContents = FileUtils.readFiles(moduleFile, Module.moduleConfigFileNames)
    val configs = fileContents.map(fileContent => ConfigFactory.parseString(fileContent))
    val config = combineConfigs(configs)
    new ModuleConfig(moduleFile.getAbsolutePath, config)
  }

  /**
   * Combine a list of configs into a single config.
   */
  def combineConfigs(configs: Seq[Config]): Config = {
    configs.foldLeft(ConfigFactory.empty())((config1, config2) => config1.withFallback(config2))
  }

}