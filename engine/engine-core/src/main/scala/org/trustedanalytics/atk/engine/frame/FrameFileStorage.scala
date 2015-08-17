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

  private val framesBaseDirectory = new Path(fsRoot + "/trustedanalytics/dataframes")

  withContext("FrameFileStorage") {
    info("fsRoot: " + fsRoot)
    info("data frames base directory: " + framesBaseDirectory)
  }

  def frameBaseDirectoryExists(dataFrame: FrameEntity) = {
    val path = frameBaseDirectory(dataFrame.id)
    hdfs.exists(path)
  }

  def createFrame(dataFrame: FrameEntity): Path = withContext("createFrame") {

    if (frameBaseDirectoryExists(dataFrame)) {
      throw new IllegalArgumentException(s"Frame already exists at ${frameBaseDirectory(dataFrame.id)}")
    }
    //TODO: actually create the file?
    frameBaseDirectory(dataFrame.id)
  }

  /**
   * Remove the directory and underlying data for a particular revision of a data frame
   * @param dataFrame the data frame to act on
   */
  def delete(dataFrame: FrameEntity): Unit = {
    hdfs.delete(frameBaseDirectory(dataFrame.id), recursive = true)
  }

  /** Base dir for a frame */
  private[frame] def frameBaseDirectory(frameId: Long): Path = {
    new Path(framesBaseDirectory + "/" + frameId)
  }

  /**
   * Determine if a dataFrame is saved as parquet
   * @param dataFrame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  private[frame] def isParquet(dataFrame: FrameEntity): Boolean = {
    val path = frameBaseDirectory(dataFrame.id)
    hdfs.globList(path, "*.parquet").nonEmpty
  }

}
