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

import java.io.{ IOException, File, FileInputStream }
import java.net.URL
import java.util.regex.Pattern
import java.util.zip.ZipInputStream
import org.trustedanalytics.atk.moduleloader.Module
import scala.collection.mutable
import com.typesafe.config.{ ConfigResolveOptions, ConfigFactory }

/**
 * The SearchPath is used to find Modules and Jars
 *
 * Modules are expected jar's containing an atk-module.conf file.
 *
 * @param path list of directories delimited by colons
 */
private[moduleloader] class SearchPath(path: String = SearchPath.defaultSearchPath) {

  private lazy val searchPath: List[File] = path.split(":").toList.map(file => new File(file))
  println("searchPath: " + searchPath.mkString(":"))

  private lazy val jarsInSearchPath: Map[String, File] = {
    val startTime = System.currentTimeMillis()
    val files = searchPath.flatMap(recursiveListOfJars)
    val results = mutable.Map[String, File]()
    for (file <- files) {
      // only take the first jar with a given name on the search path
      if (!results.contains(file.getName)) {
        results += (file.getName -> file)
      }
    }
    // debug to make sure we're not taking forever when someone adds some huge Maven repo to search path
    println(s"searchPath found ${files.size} jars (${results.size} of them unique) in ${System.currentTimeMillis() - startTime} milliseconds")
    results.toMap
  }

  /**
   * Recursively find jars under a directory
   */
  private def recursiveListOfJars(dir: File): Array[File] = {
    if (dir.exists()) {
      require(dir.isDirectory, s"Only directories are allowed in the search path: '${dir.getAbsolutePath}' was not a directory")
      val files = dir.listFiles()
      val jars = files.filter(f => f.exists() && f.getName.endsWith(".jar"))
      jars ++ files.filter(_.isDirectory).flatMap(recursiveListOfJars)
    }
    else {
      Array.empty
    }
  }

  /**
   * Search the searchPath and find all jars whose name matches a regex and that contain "atk-module.conf"
   *
   * @return list of jars and directories that include atk-module.conf
   */
  private[internal] def findModules(): Seq[File] = {
    jarsInSearchPath.values.filter(isModule).toSeq
  }

  /**
   * Search the searchPath for the jar file names supplied
   *
   * Prints error if jar is not found in searchPath
   *
   * @param jarNames the names of the jars to search for
   * @return jars with fully qualified paths
   */
  private[internal] def findJars(jarNames: Seq[String]): Array[URL] = {
    jarNames.flatMap(jarName => {
      jarsInSearchPath.get(jarName) match {
        case Some(file) => Some(file.toURI.toURL)
        case None =>
          // not throwing an Exception here because sometimes it doesn't matter if a jar isn't found
          System.err.println(s"$jarName not found in search path.  Please exclude jar or add it to the search path: " + searchPath.mkString(":"))
          None
      }
    }).toArray
  }

  /**
   * Check if the supplied jar has a name that matches the module regex and that the jar contains a atk-module.conf file
   * @param jar the jar to check
   * @return true if jar file is a module
   */
  private def isModule(jar: File): Boolean = {
    if (!SearchPath.moduleNamePattern.matcher(jar.getName).matches()) {
      // bail early so that we don't have to open up every single jar on the path
      return false
    }
    val zipInputStream = new ZipInputStream(new FileInputStream(jar))
    try {
      while (true) {
        val entry = zipInputStream.getNextEntry
        if (entry == null) {
          // not found
          return false
        }
        else if (entry.getName == Module.primaryModuleConfigFileName) {
          // was found
          return true
        }
      }
      false
    }
    catch {
      case e: IOException =>
        System.err.print(s"Jar ${jar.getAbsolutePath} caused exception:")
        e.printStackTrace()
        false
    }
    finally {
      zipInputStream.close()
    }
  }
}

object SearchPath {

  private val config = ConfigFactory.load(this.getClass.getClassLoader, ConfigResolveOptions.defaults().setAllowUnresolved(true))

  /** By default we load the search path from config */
  val defaultSearchPath: String = config.getString("atk.module-loader.search-path")

  /**
   * Regular expression for what jars names to consider being a "module".
   * Otherwise we'd have to open every jar or have a hard-coded list.
   * Opening every jar takes too long with local development where you might add ~/.m2 to your search path.
   * A hard-coded list didn't seem nice because you'd need config change every time a 3rd party wants to add plugins.
   */
  val moduleNamePattern: Pattern = Pattern.compile(config.getString("atk.module-loader.module-name-pattern"))

}
