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

package org.trustedanalytics.atk.engine.command.mgmt

class JobManager(timeoutInMinutes: Long) {

  private val activityTracker = new ActivityTracker(timeoutInMinutes)

  private var shutdownRequested = false
  private var commandReceived = true

  def accept(message: String): Unit = {
    this.synchronized {
      if (message == YarnWebProtocol.NextMsg) {
        if (shutdownRequested) {
          throw new RuntimeException("Job in process of shutting down, not accepting new commands")
        }
        else {
          commandReceived = true
          activityTracker.logActivity()
        }
      }
      else if (message == YarnWebProtocol.ShutdownMsg) {
        shutdownRequested = true
      }
      else {
        throw new IllegalArgumentException(s"Unexpected message received: $message")
      }
    }
  }

  def shouldDoWork(): Boolean = {
    this.synchronized {
      val hasNext = commandReceived
      commandReceived = false
      hasNext
    }
  }

  def isKeepRunning: Boolean = {
    this.synchronized {
      if (activityTracker.isTimedout) {
        shutdownRequested = true
      }
      !shutdownRequested
    }
  }

  /** log that activity has happened to avoid timing out */
  def logActivity(): Unit = {
    this.synchronized {
      activityTracker.logActivity()
    }
  }

}
