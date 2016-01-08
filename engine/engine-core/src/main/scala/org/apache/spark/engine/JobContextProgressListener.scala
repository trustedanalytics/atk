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

package org.apache.spark.engine

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{ SparkListenerJobStart, SparkListenerTaskEnd }
import org.apache.spark.ui.jobs.JobProgressListener
import org.trustedanalytics.atk.engine.jobcontext.JobContextStorageImpl
import org.trustedanalytics.atk.engine.plugin.{ SparkInvocation, Invocation }

class JobContextProgressListener(jobContextStorage: JobContextStorageImpl, invocation: SparkInvocation) extends JobProgressListener(new SparkConf(true)) {

  var lastUpdate = System.currentTimeMillis() - 1000

  val jobContext = jobContextStorage.lookupByClientId(invocation.user.user, invocation.clientId).getOrElse(throw new IllegalArgumentException(s"couldn't find jobContext id for invocation $invocation"))

  override def onJobStart(jobStart: SparkListenerJobStart) {
    super.onJobStart(jobStart)
    updateProgress()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    super.onTaskEnd(taskEnd)
    updateProgress()
  }

  private def updateProgress(): Unit = {
    if (System.currentTimeMillis() - 1000 > lastUpdate) {

      //      for (j <- activeJobs.valuesIterator) {
      //        System.out.println(s"Active jobid:${j.jobId} activestages:${j.numActiveStages} numTasks:${j.numTasks} failedTasks:${j.numFailedTasks} completedTasks:${j.numCompletedTasks} ${j.stageIds.mkString(",")}")
      //      }
      //      for (j <- activeStages.valuesIterator) {
      //        System.out.println(s"Active stageid:${j.stageId} status: ${j.getStatusString}  numTasks: ${j.numTasks} ${j.failureReason}")
      //      }

      val progress = s"Completed Jobs:$numCompletedJobs Stages:$numCompletedStages" +
        s" Failed Jobs:$numFailedJobs Stages:$numFailedStages"

      System.out.println(progress)

      jobContextStorage.updateProgress(jobContext.id, progress)
    }

  }
}

