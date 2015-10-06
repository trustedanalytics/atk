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

package org.trustedanalytics.atk.moduleloader

import java.io.File
import java.net.{ URLClassLoader, URL }

import com.sun.tracing.dtrace.ModuleName
import org.trustedanalytics.atk.moduleloader.internal.{ ModuleLoader, SearchPath }
import scala.collection.JavaConversions._

/**
 * Modules provide ClassLoader isolation in Atk.
 *
 * @param name name of module
 * @param parentName name of parent module
 * @param classLoader the classloader for this module
 */
class Module private[moduleloader] (val name: String,
                                    val parentName: Option[String],
                                    private[moduleloader] val classLoader: URLClassLoader) extends ClassLoaderAware {

  /**
   * jars this module needs in its ClassLoader
   */
  private[moduleloader] lazy val jarNames: Seq[String] = {
    classLoaderUrls.map(url => {
      val file = url.getFile
      file.substring(file.lastIndexOf("/") + 1)
    })
  }

  lazy val classLoaderUrls: Seq[URL] = classLoader.getURLs

  /**
   * Load a class definition from this module
   * @param className fully qualified className to load
   * @return the class definition
   */
  def loadClass[T](className: String): Class[T] = {
    classLoader.loadClass(className).asInstanceOf[Class[T]]
  }

  /**
   * Load a class from this module and instantiate it with its empty constructor
   * @param className fully qualified className to load
   * @tparam T the common interface for className shared between the called Module and calling Module
   * @return newly instantiated instance
   */
  def load[T](className: String): T = {
    loadClass(className).newInstance().asInstanceOf[T]
  }

  /**
   * Load resources from this modules ClassLoader
   * @return the resources or empty list if not found
   */
  def getResources(name: String): List[URL] = {
    classLoader.getResources(name).toList
  }

  override def toString: String = {
    s"Module(name:$name, parent:$parentName, classLoader:$classLoader})"
  }
}

/**
 * Main entry point for interacting with the Atk Modules
 */
object Module {

  /** 'system' is a special name reserved for the module-loader itself */
  val SystemName = "system"

  private[moduleloader] val primaryModuleConfigFileName = "atk-module.conf"

  private[moduleloader] val moduleConfigFileNames = Seq(primaryModuleConfigFileName, "atk-module-generated.conf")

  private lazy val moduleMap = new ModuleLoader(new SearchPath()).load()

  /**
   * Setup modules and start a Component
   *
   * The Module system was designed so that this main() would be the entry point to the system.
   *
   * @param args requires two arguments
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      printUsage()
      System.exit(1)
    }
    else {
      val component: Component = load(args(0), args(1))
      component.start()
    }
  }

  /**
   * Load a class definition from a module
   * @param moduleName name of module to load from
   * @param className fully qualified className to load
   * @return the class definition
   */
  def loadClass[T](moduleName: String, className: String): Class[T] = {
    get(moduleName).loadClass(className)
  }

  /**
   * Load a class from a module and instantiate it with its empty constructor
   * @param moduleName name of module to load from
   * @param className fully qualified className to load
   * @tparam T the common interface for className shared between the called Module and calling Module
   * @return newly instantiated instance
   */
  def load[T](moduleName: String, className: String): T = {
    get(moduleName).load(className)
  }

  /**
   * Get a module throwing an error if Module does not exist
   */
  def apply(moduleName: String): Module = {
    get(moduleName)
  }

  /**
   * Get a module throwing an error if Module does not exist
   */
  def get(moduleName: String): Module = {
    moduleMap.getOrElse(moduleName, throw new IllegalArgumentException(s"No module with name $moduleName, please choose from: " + moduleNames.mkString(", ")))
  }

  /**
   * The list of module names available
   */
  def moduleNames: Seq[String] = {
    moduleMap.keys.toSeq
  }

  def modules: Iterable[Module] = {
    moduleMap.values
  }

  def libs: Iterable[URL] = {
    modules.flatMap(_.classLoader.getURLs)
  }

  def allJarNames(moduleName: String): Seq[String] = {
    val module = get(moduleName)
    module.parentName match {
      case Some(parentName) => module.jarNames ++ allJarNames(parentName)
      case None => module.jarNames
    }
  }

  def allLibs(moduleName: String): Iterable[URL] = {
    val module = get(moduleName)
    module.parentName match {
      case Some(parentName) => module.classLoader.getURLs ++ allLibs(parentName)
      case None => module.classLoader.getURLs
    }
  }

  /**
   * Print usage of main()
   */
  private def printUsage(): Unit = {
    println("USAGE: ModuleLoader requires two args: the module name and class name")
  }

}
