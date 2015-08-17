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

import org.trustedanalytics.atk.domain.command.Command
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.jobs.JobProgressListener
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.trustedanalytics.atk.engine.CommandProgressUpdater
import org.trustedanalytics.atk.engine.{ ProgressInfo, TaskProgressInfo }

/**
 * Listens to progress on Spark Jobs.
 *
 * Requires access to classes private to org.apache.spark.engine
 */
object SparkProgressListener {
  var progressUpdater: CommandProgressUpdater = null
}

class SparkProgressListener(val progressUpdater: CommandProgressUpdater, val command: Command, val jobCount: Int) extends JobProgressListener(new SparkConf(true)) {

  val jobIdToStagesIds = new mutable.HashMap[Int, Array[Int]]

  val jobs = new ListBuffer[Int]

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val stageIds = jobStart.stageIds
    val jobId = jobStart.jobId

    jobIdToStagesIds(jobId) = stageIds.toArray
    jobs += jobId
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    super.onTaskEnd(taskEnd)
    updateProgress()
  }

  /* Calculate progress for the job */
  private def getProgress(jobId: Int): Float = {
    val totalStageIds = jobIdToStagesIds(jobId)
    val completedStageIds = completedStages.map(stageInfo => stageInfo.stageId).toSet

    val finishedCount = totalStageIds.count(i => completedStageIds.contains(i))
    val runningStages = activeStages.filter { case (id, _) => totalStageIds.contains(id) }.map { case (id, stage) => stage }

    val totalStageCount: Int = totalStageIds.length
    var progress: Float = if (totalStageCount == 0) 100 else (100 * finishedCount.toFloat) / totalStageCount.toFloat

    runningStages.foreach(stage => {
      val totalTasksCount = stage.numTasks
      val successCount = {
        stageIdToData.get((stage.stageId, stage.attemptId)) match {
          case None => 0
          case taskInfo => taskInfo.get.numCompleteTasks
        }
      }
      progress += (100 * successCount.toFloat / (totalTasksCount * totalStageCount).toFloat)
    })

    BigDecimal(progress).setScale(2, BigDecimal.RoundingMode.DOWN).toFloat
  }

  /**
   * Return a detailed progress info about current job.
   */
  private def getDetailedProgress(jobId: Int): TaskProgressInfo = {
    val stageIds = jobIdToStagesIds(jobId)
    var totalFailed = 0
    val runningStages = activeStages.filter { case (id, _) => stageIds.contains(id) }.map { case (id, stage) => stage }

    runningStages.foreach(stage => {
      totalFailed += {
        stageIdToData.get((stage.stageId, stage.attemptId)) match {
          case None => 0
          case taskInfo => taskInfo.get.numFailedTasks
        }
      }
    })

    TaskProgressInfo(totalFailed)
  }

  /**
   * Calculate progress for the command
   */
  def getCommandProgress(): List[ProgressInfo] = {
    var progress = 0f
    var retriedCounts = 0

    jobs.zip(1 to jobCount).foreach {
      case (jobId, _) =>
        progress += getProgress(jobId)
        retriedCounts += getDetailedProgress(jobId).retries
    }

    val result = new ListBuffer[ProgressInfo]()
    result += ProgressInfo(progress / jobCount.toFloat, Some(TaskProgressInfo(retriedCounts)))

    val unexpected = for {
      i <- jobCount to (jobs.length - 1)
      jobId = jobs(i)
      progress = getProgress(jobId)
      taskInfo = getDetailedProgress(jobId)
    } yield ProgressInfo(progress, Some(taskInfo))

    result ++= unexpected
    val allProgress: List[ProgressInfo] = result.toList

    /**
     * If there are multiple progress, mark every one to 100 except the last one
     */
    if (allProgress.length >= 2) {
      allProgress.zipWithIndex.map {
        case (value, index) =>
          if (index == result.length - 1) {
            ProgressInfo(value.progress, value.tasksInfo)
          }
          else {
            ProgressInfo(100, value.tasksInfo)
          }
      }
    }
    else {
      allProgress
    }
  }

  /**
   * Update the progress information and send it to progress updater
   */
  private def updateProgress() {
    val progressInfo = getCommandProgress()
    progressUpdater.updateProgress(command.id, progressInfo)
  }
}
