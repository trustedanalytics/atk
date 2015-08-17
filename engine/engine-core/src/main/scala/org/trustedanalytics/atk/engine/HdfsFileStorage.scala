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

package org.trustedanalytics.atk.engine

import java.io.{ InputStream, OutputStream }

import org.trustedanalytics.atk.engine.util.KerberosAuthenticator
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, LocalFileSystem, Path }
import org.apache.hadoop.hdfs.DistributedFileSystem

/**
 * HDFS Access
 *
 * IMPORTANT! Make sure you aren't breaking wild card support - it is easy to forget about
 *
 * @param fsRoot the root directory for TrustedAnalytics e.g. "/user/atkuser"
 */
class HdfsFileStorage(fsRoot: String) extends EventLogging {
  implicit val eventContext = EventContext.enter("HDFSFileStorage")

  private val securedConfiguration = withContext("HDFSFileStorage.configuration") {

    info("fsRoot: " + fsRoot)

    val hadoopConfig = new Configuration()
    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[LocalFileSystem].getName)

    if (fsRoot.startsWith("hdfs")) {
      info("fsRoot starts with HDFS")
    }

    hadoopConfig.set("fs.defaultFS", fsRoot)

    require(hadoopConfig.getClassByNameOrNull(classOf[LocalFileSystem].getName) != null,
      "Could not load local filesystem for Hadoop")

    KerberosAuthenticator.loginConfigurationWithKeyTab(hadoopConfig)
    hadoopConfig
  }(null)

  def configuration: Configuration = {
    if (EngineConfig.enableKerberos) {
      KerberosAuthenticator.loginConfigurationWithKeyTab(securedConfiguration)
    }
    securedConfiguration
  }

  private val fileSystem = FileSystem.get(configuration)

  /**
   * Verifies that the Kerberos Ticket is still valid and if not relogins before returning fileSystem object
   * @return Hadoop FileSystem
   */
  def fs: FileSystem = {
    if (EngineConfig.enableKerberos) {
      KerberosAuthenticator.loginConfigurationWithKeyTab(securedConfiguration)
    }
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
      new Path(concatPaths(fsRoot, path))
    }
  }

  def write(sink: Path, append: Boolean): OutputStream = withContext("file.write") {
    val path: Path = absolutePath(sink.toString)
    if (append) {
      fs.append(path)
    }
    else {
      fs.create(path, true)
    }
  }

  def list(source: Path): Seq[Path] = withContext("file.list") {
    fs.listStatus(absolutePath(source.toString)).map(fs => fs.getPath)
  }

  /**
   * Return a sequence of Path objects in a given directory that match a user supplied path filter
   * @param source parent directory
   * @param filter path filter
   * @return Sequence of Path objects
   */
  def globList(source: Path, filter: String): Seq[Path] = withContext("file.globList") {
    fs.globStatus(new Path(source, filter)).map(fs => fs.getPath)
  }

  def read(source: Path): InputStream = withContext("file.read") {
    val path: Path = absolutePath(source.toString)
    fs.open(path)
  }

  def exists(path: Path): Boolean = withContext("file.exists") {
    val p: Path = absolutePath(path.toString)
    fs.exists(p)
  }

  /**
   * Delete
   * @param recursive true to delete subdirectories and files
   */
  def delete(path: Path, recursive: Boolean = true): Unit = withContext("file.delete") {
    val fullPath = absolutePath(path.toString)
    if (fs.exists(fullPath)) {
      val success = fs.delete(fullPath, recursive)
      if (!success) {
        error("Could not delete path: " + fullPath.toUri.toString)
      }
    }
  }

  def create(file: Path): Unit = withContext("file.create") {
    fs.create(absolutePath(file.toString))
  }

  def createDirectory(directory: Path): Unit = withContext("file.createDirectory") {
    val adjusted = absolutePath(directory.toString)
    fs.mkdirs(adjusted)
  }

  /**
   * File size (supports wildcards)
   * @param path relative path
   */
  def size(path: String): Long = {
    val abPath: Path = absolutePath(path)
    // globStatus() was returning zero if File was directory
    val fileStatuses = if (fs.isDirectory(abPath)) fs.listStatus(abPath) else fs.globStatus(abPath)
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
    fs.isDirectory(path)
  }

  private def concatPaths(first: String, second: String) = {
    if (first.endsWith("/") || second.startsWith("/")) {
      first + second
    }
    else {
      first + "/" + second
    }
  }

}
