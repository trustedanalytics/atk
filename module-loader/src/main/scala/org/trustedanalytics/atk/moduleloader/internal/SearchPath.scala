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

import java.io.{ IOException, File, FileInputStream, FilenameFilter }
import java.net.URL
import java.util.zip.ZipInputStream

import org.trustedanalytics.atk.moduleloader.Module

import scala.collection.JavaConversions._

import com.typesafe.config.{ ConfigFactory, Config }

/**
 * The SearchPath is used to find Modules and Jars
 *
 * Modules are expected to either be in a directory or in a jar.
 *
 * @param path list of jars and directories delimited by colons
 */
private[moduleloader] class SearchPath(path: String = SearchPath.defaultSearchPath) {

  private lazy val searchPath: List[File] = path.split(":").toList.map(file => new File(file))

  println("searchPath: " + searchPath.mkString(":"))

  /**
   * Search the searchPath and find all jars and directories that contain "atk-module.conf"
   *
   * @return list of jars and directories that include atk-module.conf
   */
  private[internal] def findModules(): Seq[File] = {
    expandSearchPath(searchPath).filter(path => {
      if (path.isDirectory && path.listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          Module.primaryModuleConfigFileName == name
        }
      }).nonEmpty) {
        true
      }
      else if (path.getName.endsWith(".jar")) {
        jarContainsModuleConf(path)
      }
      else {
        false
      }
    })
  }

  /**
   * Search the searchPath for the jar file names supplied
   *
   * Throws error if jar is not found in searchPath
   *
   * @param jarNames the names of the jars to search for
   * @return jars with fully qualified paths
   */
  private[internal] def findJars(jarNames: Seq[String]): Array[URL] = {
    val files = jarNames.map(jar => {
      val jarsListedExplicitlyInPath = searchPath.filter(file => file.getName.equals(jar) && file.exists())
      if (jarsListedExplicitlyInPath.isEmpty) {
        val locations = searchPath.map(new File(_, jar)).filter(_.exists())
        if (locations.isEmpty) {
          throw new RuntimeException(s"jar $jar not found in search path: " + searchPath.mkString(", "))
        }
        else {
          // only return the first jar found
          locations.head
        }
      }
      else {
        jarsListedExplicitlyInPath.head
      }
    })
    files.map(_.toURI.toURL).toArray
  }

  /**
   * Takes a searchPath and expands it to include files and directories one level deep.
   *
   * For example, a "/lib" might expand to "/lib/some.jar, /list/another.jar".
   *
   * Also, drop files and directories that do NOT exist.
   *
   * @param searchPath the searchPath to expand
   * @return the expanded path with
   */
  private def expandSearchPath(searchPath: Seq[File]): Seq[File] = {
    searchPath.flatMap(path => {
      if (!path.exists()) {
        // don't include files or directories that don't exist
        None
      }
      else if (path.isDirectory) {
        // go one more level deep for directories but also keep the directory in the list
        path.listFiles().toSeq :+ path
      }
      else {
        // files are kept but have nothing to expand to
        Some(path)
      }
    })
  }

  /**
   * Check if the supplied jar contains a atk-module.conf file
   * @param jar the jar to check
   * @return true if jar file contained atk-module.conf file
   */
  private def jarContainsModuleConf(jar: File): Boolean = {
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

  val defaultSearchPath: String = {
    val config = ConfigFactory.load(this.getClass.getClassLoader)
    config.getString("atk.module-loader.search-path")
  }

}
