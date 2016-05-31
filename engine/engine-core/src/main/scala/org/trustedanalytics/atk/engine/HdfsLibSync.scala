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

import org.apache.hadoop.fs.permission.{ FsPermission, FsAction }
import org.apache.hadoop.fs.{ FileSystem, FileStatus, Path }
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.trustedanalytics.atk.moduleloader.Module

/**
 * Copy jars from local fileSystem to HDFS
 */
class HdfsLibSync(fs: FileStorage) extends EventLogging {

  implicit val eventContext = EventContext.enter("HdfsLibSync")

  def create_base_dirs(): Unit = withContext("create_base_dirs") {

    val fsRootPath = fs.absolutePath(EngineConfig.fsRoot)
    if (fs.exists(fsRootPath) == false) {
      FileSystem.mkdirs(fs.hdfs, fsRootPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
    }
    val trustedAnalyticsSubDirectory = fs.absolutePath(s"${EngineConfig.fsRoot}/trustedanalytics")
    if (fs.exists(trustedAnalyticsSubDirectory) == false) {
      FileSystem.mkdirs(fs.hdfs, trustedAnalyticsSubDirectory, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
    }
  }

  /**
   * Synchronize local-lib folders to hdfs-lib by copying jars to HDFS.
   */
  def syncLibs(): Unit = {
    create_base_dirs()
    val destDir = fs.absolutePath(EngineConfig.hdfsLib)
    val localLibs = Module.libs.map(url => new Path(url.toURI))
    info(s"hdfs-lib: $destDir")
    if (!fs.hdfs.exists(destDir)) {
      info(s"Creating $destDir")
      fs.hdfs.mkdirs(destDir)
      fs.hdfs.setPermission(destDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
    }
    require(fs.hdfs.isDirectory(destDir), s"Not a directory $destDir, please configure hdfs-lib")
    localLibs.foreach(localLib => {
      sync(localLib, destDir)
    })
  }

  /**
   * Synchronize a local-lib folder to hdfs-lib by copying jars to HDFS
   */
  private def sync(localLib: Path, destDir: Path): Unit = {
    if (!fs.localFileSystem.exists(localLib)) {
      warn(s"Does not exist $localLib")
    }
    else if (fs.localFileSystem.isDirectory(localLib)) {
      val localJars = fs.localFileSystem.listFiles(localLib, false)
      while (localJars.hasNext) {
        val localJarStatus = localJars.next()
        syncJar(localJarStatus, destDir)
      }
    }
    else {
      syncJar(fs.localFileSystem.getFileStatus(localLib), destDir)
    }
  }

  /**
   * Sync a local jar to a location in HDFS, if needed.
   *
   * Sync is performed if local jar has a newer timestamp or a different
   * file size than jar in HDFS.
   */
  private def syncJar(localJarStatus: FileStatus, destDir: Path): Unit = {
    val localJarPath = localJarStatus.getPath
    if (localJarStatus.isFile && localJarPath.getName.endsWith(".jar")) {
      val destJarPath = new Path(destDir, localJarPath.getName)
      if (fs.exists(destJarPath)) {
        val destJarStatus = fs.hdfs.getFileStatus(destJarPath)
        if (localJarStatus.getModificationTime > destJarStatus.getModificationTime) {
          info(s"jar is out of date, copying $localJarPath to $destJarPath")
          fs.hdfs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
        }
        else if (localJarStatus.getLen != destJarStatus.getLen) {
          // this is a slight fail-safe in case timestamps aren't correct
          info(s"jars were different, copying $localJarPath to $destJarPath")
          fs.hdfs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
        }
        else {
          info(s"jar is up to date, $localJarPath matches $destJarPath")
        }
      }
      else {
        info(s"jar does not exist, copying $localJarPath to $destJarPath")
        fs.hdfs.copyFromLocalFile(false, true, localJarStatus.getPath, destJarPath)
      }
    }
  }

}
