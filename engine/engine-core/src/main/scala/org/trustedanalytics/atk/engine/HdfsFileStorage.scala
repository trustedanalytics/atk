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
 */
class HdfsFileStorage extends EventLogging {
  implicit val eventContext = EventContext.enter("HDFSFileStorage")

  private val securedConfiguration = withContext("HDFSFileStorage.configuration") {

    info("fsRoot: " + EngineConfig.fsRoot)

    val hadoopConfig = new Configuration()
    //http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    hadoopConfig.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[LocalFileSystem].getName)
    hadoopConfig.set("fs.defaultFS", EngineConfig.fsRoot)

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

  private val localFileSystem = FileSystem.getLocal(configuration)
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
      new Path(concatPaths(EngineConfig.fsRoot, path))
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

  /**
   * Synchronize local-lib folders to hdfs-lib by copying jars to HDFS.
   */
  def syncLibs(): Unit = withContext("synclibs") {
    val destDir = absolutePath(EngineConfig.hdfsLib)
    val srcDirs = EngineConfig.localLibs.map(path => new Path(path))
    srcDirs.foreach(srcDir => info(s"local-lib: $srcDir"))
    info(s"hdfs-lib: $destDir")
    if (!fs.exists(destDir)) {
      info(s"Creating $destDir")
      fs.mkdirs(destDir)
    }
    require(fs.isDirectory(destDir), s"Not a directory $destDir, please configure hdfs-lib")
    for (srcDir <- srcDirs) {
      syncDir(srcDir, destDir)
    }
  }

  /**
   * Synchronize a local-lib folder to hdfs-lib by copying jars to HDFS
   */
  private def syncDir(srcDir: Path, destDir: Path): Unit = {
    require(localFileSystem.exists(srcDir), s"Directory does not exist $srcDir, please configure local-libs")
    require(localFileSystem.isDirectory(srcDir), s"local-libs had a path that was NOT a directory: $srcDir")

    val localJars = localFileSystem.listFiles(srcDir, false)
    while (localJars.hasNext) {
      val localJarStatus = localJars.next()
      val localJarPath = localJarStatus.getPath
      if (localJarStatus.isFile && localJarPath.getName.endsWith(".jar")) {
        val destJarPath = new Path(destDir, localJarPath.getName)
        if (fs.exists(destJarPath)) {
          val destJarStatus = fs.getFileStatus(destJarPath)
          if (localJarStatus.getModificationTime > destJarStatus.getModificationTime) {
            info(s"jar is out of date, copying $localJarPath to $destJarPath")
            fs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
          }
          else if (localJarStatus.getLen != destJarStatus.getLen) {
            // this is a slight fail-safe in case timestamps aren't correct
            info(s"jars were different, copying $localJarPath to $destJarPath")
            fs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
          }
          else {
            info(s"jar is up to date, $localJarPath matches $destJarPath")
          }
        }
        else {
          info(s"jar does not exist, copying $localJarPath to $destJarPath")
          fs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
        }
      }
    }
  }

  private def concatPaths(first: String, second: String): String = {
    if (first.endsWith("/") || second.startsWith("/")) {
      first + second
    }
    else {
      first + "/" + second
    }
  }

}
