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
 * Entry point for all Trusted Analytics Toolkit applications.
 *
 * Manages a registry of plugins.
 *
 */
object Boot {

  /**
   * Returns the requested archive, loading it if needed.
   * @param archiveName the name of the archive
   * @param className the name of the class managing the archive
   *
   * @return the requested archive
   */
  def getArchive(archiveName: String, className: Option[String] = None): Archive = {
    Archive.getArchive(archiveName, className)
  }

  /**
   * Returns the class loader for the given archive
   */
  def getClassLoader(archive: String): ClassLoader = {
    Archive.getClassLoader(archive)
  }

  private def usage(): Unit = println("Usage: java -jar launcher.jar <archive> <application>")

  def main(args: Array[String]) = {
    if (args.length < 1 || args.length > 2) {
      usage()
    }
    else {
      try {
        val archiveName: String = args(0)
        val applicationName: Option[String] = { if (args.length == 2) Some(args(1)) else None }
        println("Starting application")
        Archive.logger(s"Starting $archiveName")
        val instance = getArchive(archiveName, applicationName)
        Archive.logger(s"Started $archiveName with ${instance.definition}")
      }
      catch {
        case e: Throwable =>
          var current = e
          while (current != null) {
            Archive.logger(current.toString)
            println(current)
            current.printStackTrace()
            current = current.getCause
          }
      }
    }
  }
}
