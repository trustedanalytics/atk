/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.command.mgmt

/**
 * Keep track of last activity and if timeout should occur
 */
class ActivityTracker(timeoutInSeconds: Long) {

  private val timeoutInMillis = timeoutInSeconds * 1000
  @volatile private var lastActivityTime = System.currentTimeMillis()

  /** log that activity has happened to avoid timing out */
  def logActivity(): Unit = {
    lastActivityTime = System.currentTimeMillis()
  }

  /** True if timeout since last activity */
  def isTimedout: Boolean = {
    System.currentTimeMillis() - lastActivityTime > timeoutInMillis
  }

}