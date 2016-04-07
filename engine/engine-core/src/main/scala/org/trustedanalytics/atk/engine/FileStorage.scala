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

package org.trustedanalytics.atk.engine

import java.io.{ InputStream, OutputStream }
import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.engine.util.KerberosAuthenticator
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.trustedanalytics.atk.moduleloader.Module

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import EngineExecutionContext._

/**
 * HDFS Access
 *
 * IMPORTANT! Make sure you aren't breaking wild card support - it is easy to forget about
 */
class FileStorage extends EventLogging {

  implicit val eventContext = EventContext.enter("FileStorage")

  private val securedConfiguration = withContext("HDFSFileStorage.configuration") {
    info("fsRoot: " + EngineConfig.fsRoot)
    KerberosAuthenticator.loginAsAuthenticatedUser()

    val configuration = KerberosAuthenticator.loginConfigurationWithClassLoader()
    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
    configuration.set("fs.defaultFS", EngineConfig.fsRoot)

    configuration
  }(null)

  def configuration: Configuration = {
    securedConfiguration
  }

  val localFileSystem = FileSystem.getLocal(configuration)
  private val fileSystem = {
    try {
      import org.trustedanalytics.hadoop.config.client.helper.Hdfs
      Hdfs.newInstance().createFileSystem()
    }
    catch {
      case _ =>
        info("Failed to create HDFS instance using hadoop-library. Default to FileSystem")
        FileSystem.get(configuration)
    }
  }

  /**
   * Verifies that the Kerberos Ticket is still valid and if not relogins before returning fileSystem object
   * @return Hadoop FileSystem
   */
  def hdfs: FileSystem = {
    fileSystem
  }

  private val absolutePathPattern = """^\w+\:/.+""".r

  /**
   * Path from a path
   * @param path a path relative to the root or that includes the root
   */
  def absolutePath(path: String): Path = {
    if (absolutePathPattern.findFirstIn(path).isDefined) {
      new Path(path)
    }
    else {
      new Path(concatPaths(EngineConfig.fsRoot, path))
    }
  }

  def write(sink: Path, append: Boolean): OutputStream = withContext("file.write") {
    val path: Path = absolutePath(sink.toString)
    if (append) {
      hdfs.append(path)
    }
    else {
      hdfs.create(path, true)
    }
  }

  def list(source: Path): Seq[Path] = withContext("file.list") {
    hdfs.listStatus(absolutePath(source.toString)).map(fs => fs.getPath)
  }

  /**
   * Return a sequence of Path objects in a given directory that match a user supplied path filter
   * @param source parent directory
   * @param filter path filter
   * @return Sequence of Path objects
   */
  def globList(source: Path, filter: String): Seq[Path] = withContext("file.globList") {
    hdfs.globStatus(new Path(source, filter)).map(fs => fs.getPath)
  }

  def read(source: Path): InputStream = withContext("file.read") {
    val path: Path = absolutePath(source.toString)
    hdfs.open(path)
  }

  def exists(path: Path): Boolean = withContext("file.exists") {
    val p: Path = absolutePath(path.toString)
    hdfs.exists(p)
  }

  /**
   * Delete
   * @param recursive true to delete subdirectories and files
   */
  def delete(path: Path, recursive: Boolean = true): Unit = withContext("file.delete") {
    val fullPath = absolutePath(path.toString)
    if (hdfs.exists(fullPath)) {
      val success = hdfs.delete(fullPath, recursive)
      if (!success) {
        error("Could not delete path: " + fullPath.toUri.toString)
      }
    }
  }

  def create(file: Path): Unit = withContext("file.create") {
    hdfs.create(absolutePath(file.toString))
  }

  def createDirectory(directory: Path): Unit = withContext("file.createDirectory") {
    val adjusted = absolutePath(directory.toString)
    hdfs.mkdirs(adjusted)
  }

  /**
   * File size (supports wildcards)
   * @param path relative path
   */
  def size(path: String): Long = {
    val abPath: Path = absolutePath(path)
    // globStatus() was returning zero if File was directory
    val fileStatuses = if (hdfs.isDirectory(abPath)) hdfs.listStatus(abPath) else hdfs.globStatus(abPath)
    if (ArrayUtils.isEmpty(fileStatuses.asInstanceOf[Array[AnyRef]])) {
      throw new RuntimeException("No file found at path " + abPath)
    }
    fileStatuses.map(fileStatus => fileStatus.getLen).sum
  }

  /**
   * Determine if the file path is a directory
   * @param path path to examine
   * @return true if the path is a directory false if it is not
   */
  def isDirectory(path: Path): Boolean = withContext("file.isDirectory") {
    hdfs.isDirectory(path)
  }

  /**
   * Given a list of jarNames return the paths in HDFS
   */
  def hdfsLibs(jarNames: Seq[String]): Seq[String] = {
    val hdfsLib = absolutePath(EngineConfig.hdfsLib)
    jarNames.map(jarName => concatPaths(hdfsLib.toString, jarName))
  }

  /**
   * Concat two paths
   */
  private def concatPaths(first: String, second: String): String = {
    if (first.endsWith("/") || second.startsWith("/")) {
      first + second
    }
    else {
      first + "/" + second
    }
  }

}
