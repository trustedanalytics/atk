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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.scheduler.{ SparkListenerJobStart, SparkListenerTaskEnd }
import org.apache.spark.ui.jobs.JobProgressListener
import org.trustedanalytics.atk.engine.jobcontext.JobContextStorageImpl
import org.trustedanalytics.atk.engine.plugin.SparkInvocation

class JobContextProgressListener(jobContextStorage: JobContextStorageImpl, invocation: SparkInvocation) extends JobProgressListener(new SparkConf(true)) {

  var lastUpdate = System.currentTimeMillis() - 1000

  val jobContext = jobContextStorage.lookupByClientId(invocation.user.user, invocation.clientId).getOrElse(throw new IllegalArgumentException(s"couldn't find jobContext id for invocation $invocation"))

  // Store the Current Job Group ID
  var jobGroupId = StringUtils.EMPTY

  override def onJobStart(jobStart: SparkListenerJobStart) {
    super.onJobStart(jobStart)
    jobGroupId = jobStart.properties.getProperty(SparkContext.SPARK_JOB_GROUP_ID)
    updateProgress()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    super.onTaskEnd(taskEnd)
    updateProgress()
  }

  private def updateProgress(): Unit = {
    if (System.currentTimeMillis() - 1000 > lastUpdate) {
      lastUpdate = System.currentTimeMillis()

      val completedJobs = numCompletedJobs
      val numTasks = activeJobs.map { case (jobId, jobsUIData) => jobsUIData.numTasks }.sum
      val completedTasks = activeJobs.map { case (jobId, jobsUIData) => jobsUIData.numCompletedTasks }.sum
      val retries = activeJobs.map { case (jobId, jobsUIData) => jobsUIData.numFailedTasks }.sum
      val showTaskRetries = if (retries > 0) s" Task retries:$retries" else ""
      val percentComplete = if (numTasks > 0) completedTasks * 100.0 / numTasks else 0.0
      val percentIncomplete = 100 - percentComplete

      val progressBar = s"[${"=" * Math.floor(percentComplete / 4).toInt}${"." * Math.ceil(percentIncomplete / 4).toInt}]"

      val progress = f"Job:$completedJobs $progressBar $percentComplete%6.2f%%$showTaskRetries"

      jobContextStorage.updateProgress(jobContext.id, progress)
    }

  }
}

