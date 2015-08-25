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

package org.trustedanalytics.atk.engine.frame

import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.HdfsFileStorage
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.event.EventLogging

/**
 * Frame storage in HDFS.
 *
 * @param fsRoot root for our application, e.g. "hdfs://hostname/user/atkuser"
 * @param hdfs methods for interacting with underlying storage (e.g. HDFS)
 */
class FrameFileStorage(fsRoot: String,
                       val hdfs: HdfsFileStorage)(implicit startupInvocation: Invocation)
    extends EventLogging with EventLoggingImplicits {

  private val framesBaseDirectory = new Path(fsRoot + "/trustedanalytics/frames")

  withContext("FrameFileStorage") {
    info("fsRoot: " + fsRoot)
    info("data frames base directory: " + framesBaseDirectory)
  }

  /**
   * Determines what the storage path should be based on information in the Entity and current configuration
   * @param frame the data frame to act on
   * @return
   */
  def calculateFramePath(frame: FrameEntity): Path = {
    new Path(framesBaseDirectory, frame.id.toString)
  }

  /**
   * Remove the directory and underlying data for a particular frame
   * @param frame the data frame to act on
   */
  def delete(frame: FrameEntity): Unit = {
    if (frame.storageLocation.isDefined) {
      deletePath(getStoredRevisionPath(frame).getParent)
    }
  }

  /**
   * Remove the directory and underlying data for a particular frame
   * @param path the path of the dir to remove
   */
  def deletePath(path: Path): Unit = {
    hdfs.delete(path, recursive = true)
  }

  /**
   * Determine if a dataFrame is saved as parquet
   * @param dataFrame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  private[frame] def isParquet(dataFrame: FrameEntity): Boolean = {
    val path = getStoredRevisionPath(dataFrame)
    hdfs.globList(path, "*.parquet").nonEmpty
  }

  /**
   * Helper method to get the storage path that is stored in the Entity
   * @param frame the frame to interrogate
   * @return
   */
  private[frame] def getStoredRevisionPath(frame: FrameEntity): Path = {
    frame.storageLocation match {
      case Some(path) => new Path(path)
      case None => throw new RuntimeException(s"Frame ${frame.id} does not have a storage location set")
    }
  }
}
