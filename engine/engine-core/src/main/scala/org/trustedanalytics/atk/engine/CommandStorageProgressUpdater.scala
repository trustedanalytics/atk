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

/**
 * Update progress for spark command.
 * @param commandStorage command storage
 */
class CommandStorageProgressUpdater(commandStorage: CommandStorage) extends CommandProgressUpdater {
  val INTERVAL = 1000
  var lastUpdateTime = System.currentTimeMillis() - 2 * INTERVAL
  /**
   * save the progress update
   * @param commandId id of the command
   * @param progressInfo list of progress for jobs initiated by the command
   */
  override def updateProgress(commandId: Long, progressInfo: List[ProgressInfo]): Unit = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastUpdateTime >= INTERVAL) {
      lastUpdateTime = currentTime
      commandStorage.updateProgress(commandId, progressInfo)
    }
  }

  /**
   * save the progress update
   * @param commandId id of the command
   */
  override def updateProgress(commandId: Long, progress: Float): Unit = {
    updateProgress(commandId, List(ProgressInfo(progress, None)))
  }
}
