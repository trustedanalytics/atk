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

package org.apache.spark.engine

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.apache.spark.Success
import org.apache.spark.scheduler._

/**
 * Logs progress from SparkProgressListener
 */
class ProgressPrinter(implicit invocation: Invocation) extends SparkListener
    with EventLogging
    with EventLoggingImplicits {

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = withContext("stageCompleted") {
    stageCompleted.stageInfo.failureReason match {
      case Some(failureReason) => error(buildStageInfoMessage(stageCompleted.stageInfo))
      case _ => //ignore
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = withContext("taskEnd") {
    taskEnd.reason match {
      case Success => //ignore
      case _ => warn(taskEnd.reason.toString)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = withContext("jobEnd") {
    jobEnd.jobResult match {
      case JobSucceeded => //ignore
      case _ => warn(jobEnd.jobResult.toString)
    }
  }

  private def buildStageInfoMessage(stageInfo: StageInfo): String = {
      " stageId:" + stageInfo.stageId +
      " stageInfoName:" + stageInfo.name +
      " numTasks:" + stageInfo.numTasks +
      //      " emittedTaskSizeWarning:" + stageInfo.emittedTaskSizeWarning +
      " failureReason:" + stageInfo.failureReason.getOrElse("")
  }

}
