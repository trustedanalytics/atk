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
package org.apache.spark.engine

import org.apache.spark.TaskContext
import org.apache.spark.scheduler._
import org.joda.time.DateTime
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.engine.{ CommandProgressUpdater, ProgressInfo, TaskProgressInfo }

class ProgressListenerTest extends WordSpec with Matchers with MockitoSugar {
  /*
  class TestProgressUpdater extends CommandProgressUpdater {

    val commandProgress = scala.collection.mutable.Map[Long, List[Float]]()

    override def updateProgress(commandId: Long, progress: List[ProgressInfo]): Unit = {
      commandProgress(commandId) = progress.map(info => info.progress)
    }

    override def updateProgress(commandId: Long, progress: Float): Unit = ???
  }

  def createListener_one_job(commandId: Long): SparkProgressListener = {
    val command = new Command(commandId, "mock", createdOn = new DateTime, modifiedOn = new DateTime)
    val listener = new SparkProgressListener(new TestProgressUpdater(), command, 1)

    val stageOne = new StageInfo(1, 1, "one", 1, Seq(), Seq(), "one")
    val stageTwo = new StageInfo(2, 2, "two", 2, Seq(), Seq(), "two")
    val stageThree = new StageInfo(3, 3, "three", 3, Seq(), Seq(), "three")

    val stageIds = Seq(stageOne, stageTwo, stageThree)

    val job = mock[ActiveJob]
    when(job.jobId).thenReturn(1)
    val finalStage1 = mock[ResultStage]
    when(finalStage1.id).thenReturn(3)
    val parent1 = mock[Stage]
    when(parent1.id).thenReturn(1)
    val parent2 = mock[Stage]
    when(parent2.id).thenReturn(2)
    when(parent2.parents).thenReturn(List(parent1))

    when(finalStage1.parents).thenReturn(List(parent1, parent2))

    when(job.finalStage).thenReturn(finalStage1)

    val jobStart = SparkListenerJobStart(job.jobId, 1, stageIds)

    listener onJobStart jobStart
    listener
  }

  def createListener_two_jobs(commandId: Long, expectedJobs: Int = 2): SparkProgressListener = {
    val command = new Command(commandId, "mock", createdOn = new DateTime, modifiedOn = new DateTime)
    val listener = new SparkProgressListener(new TestProgressUpdater(), command, expectedJobs)
    val stageOne = new StageInfo(1, 1, "one", 1, Seq(), Seq(), "one")
    val stageTwo = new StageInfo(2, 2, "two", 2, Seq(), Seq(), "two")
    val stageThree = new StageInfo(3, 3, "three", 3, Seq(), Seq(), "three")
    val stagefour = new StageInfo(4, 4, "four", 4, Seq(), Seq(), "four")
    val stagefive = new StageInfo(5, 5, "five", 5, Seq(), Seq(), "five")
    val stagesix = new StageInfo(6, 6, "six", 6, Seq(), Seq(), "six")
    val stageseven = new StageInfo(7, 7, "seven", 7, Seq(), Seq(), "seven")

    val stageIds = Seq(stageOne, stageTwo, stageThree)

    val job1 = mock[ActiveJob]
    when(job1.jobId).thenReturn(1)

    val jobStart1 = SparkListenerJobStart(job1.jobId, 1, stageIds)
    listener onJobStart jobStart1

    val job2 = mock[ActiveJob]
    when(job2.jobId).thenReturn(2)

    val jobStart2 = SparkListenerJobStart(job2.jobId, 2, Array(stagefour, stagefive, stagesix, stageseven))
    listener onJobStart jobStart2

    listener
  }

  private def sendStageSubmittedToListener(listener: SparkProgressListener, stageId: Int, numTasks: Int) {
    val stageInfo = mock[StageInfo]
    when(stageInfo.numTasks).thenReturn(numTasks)
    when(stageInfo.stageId).thenReturn(stageId)
    when(stageInfo.attemptId).thenReturn(1)

    val submitted = SparkListenerStageSubmitted(stageInfo, null)
    listener.onStageSubmitted(submitted)
  }

  private def sendStageCompletedToListener(listener: SparkProgressListener, stageId: Int) {
    val stageInfo = mock[StageInfo]
    when(stageInfo.stageId).thenReturn(stageId)
    when(stageInfo.attemptId).thenReturn(1)
    when(stageInfo.failureReason).thenReturn(None)
    when(stageInfo.accumulables).thenReturn(new scala.collection.mutable.HashMap[Long, AccumulableInfo])
    listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))
  }

  "get all stages" in {
    val command = new Command(1, "mock", createdOn = new DateTime, modifiedOn = new DateTime)
    val listener = new SparkProgressListener(new TestProgressUpdater(), command, 1)
    val stageOne = new StageInfo(1, 1, "one", 1, Seq(), Seq(), "one")
    val stageTwo = new StageInfo(2, 2, "two", 2, Seq(), Seq(), "two")
    val stageThree = new StageInfo(3, 3, "three", 3, Seq(), Seq(), "three")
    val stageFour = new StageInfo(4, 4, "four", 4, Seq(), Seq(), "four")
    val stageFive = new StageInfo(5, 5, "five", 5, Seq(), Seq(), "five")
    val job = mock[ActiveJob]
    when(job.jobId).thenReturn(1)

    val jobStart = SparkListenerJobStart(job.jobId, 1, Seq(stageOne, stageTwo, stageThree, stageFour, stageFive), null)
    listener onJobStart jobStart

    listener.jobIdToStagesIds(1).toList.sorted shouldEqual List(1, 2, 3, 4, 5)
  }

  "initialize stages count" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(0, Some(TaskProgressInfo(0))))
  }

  "finish first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(33.33f)
  }

  "finish second stage" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(33.33f)
    sendStageSubmittedToListener(listener, 2, 10)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  "finish all stages" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(100f)

    val jobEnd = mock[SparkListenerJobEnd]
    when(jobEnd.jobResult).thenReturn(JobSucceeded)
    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(100)
  }

  "finish first task in first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    listener.stageIdToData((1, 1)).numCompleteTasks = listener.stageIdToData((1, 1)).numCompleteTasks + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(3.33f, Some(TaskProgressInfo(0))))
  }

  "finish second task in second stage" in {
    val listener = createListener_one_job(1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 2
    listener.getCommandProgress() shouldEqual List(ProgressInfo(40f, Some(TaskProgressInfo(0))))
  }

  "finish second task in second stage, second task in third stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)
    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 2

    sendStageSubmittedToListener(listener, 3, 10)
    listener.stageIdToData((3, 1)).numCompleteTasks = listener.stageIdToData((3, 1)).numCompleteTasks + 2

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(46.66f)
  }

  "finish all tasks in second stage" in {
    val listener: SparkProgressListener = finishAllTasksInSecondStage
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  private def finishAllTasksInSecondStage: SparkProgressListener = {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 3)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)
    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 3
    listener
  }

  "finish all tasks in second stage-2" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 3)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 3)

    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 3
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
    sendStageCompletedToListener(listener, 2)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(66.66f)
  }

  "failed at first stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)

    val jobFailed = mock[JobFailed]
    val jobEnd = mock[SparkListenerJobEnd]
    when(jobEnd.jobResult).thenReturn(jobFailed)

    val stage = mock[Stage]

    when(stage.id).thenReturn(1)

    listener.onJobEnd(jobEnd)
    listener.getCommandProgress().map(info => info.progress) shouldEqual List(0)
  }

  "failed at middle of stage" in {
    val listener = createListener_one_job(1)
    sendStageSubmittedToListener(listener, 1, 10)
    listener.stageIdToData((1, 1)).numCompleteTasks = listener.stageIdToData((1, 1)).numCompleteTasks + 6

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)
    val stage = mock[Stage]
    when(stage.id).thenReturn(1)

    listener.getCommandProgress().map(info => info.progress) shouldEqual List(20)
  }

  "get two progress info for a single command" in {
    val listener: SparkProgressListener = createListener_two_jobs(1)
    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 2

    listener.stageIdToData((2, 1)).numFailedTasks = listener.stageIdToData((2, 1)).numFailedTasks + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(1))))

    sendStageSubmittedToListener(listener, 4, 10)
    listener.stageIdToData((4, 1)).numFailedTasks = listener.stageIdToData((4, 1)).numFailedTasks + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(20f, Some(TaskProgressInfo(2))))

    listener.stageIdToData((4, 1)).numCompleteTasks = listener.stageIdToData((4, 1)).numCompleteTasks + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(21.25f, Some(TaskProgressInfo(2))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    listener.stageIdToData((3, 1)).numCompleteTasks = listener.stageIdToData((3, 1)).numCompleteTasks + 10
    sendStageCompletedToListener(listener, 3)
    listener.getCommandProgress().map(info => info.tasksInfo) shouldEqual List(Some(TaskProgressInfo(1)))
  }

  "get two progress info for a single command, expected number of jobs is 1 less than actual" in {
    val listener: SparkProgressListener = createListener_two_jobs(1, 1)

    sendStageSubmittedToListener(listener, 1, 10)
    sendStageCompletedToListener(listener, 1)
    sendStageSubmittedToListener(listener, 2, 10)

    listener.stageIdToData((2, 1)).numCompleteTasks = listener.stageIdToData((2, 1)).numCompleteTasks + 2
    listener.stageIdToData((2, 1)).numFailedTasks = listener.stageIdToData((2, 1)).numFailedTasks + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(0))))

    sendStageSubmittedToListener(listener, 4, 10)

    listener.stageIdToData((4, 1)).numFailedTasks = listener.stageIdToData((4, 1)).numFailedTasks + 1

    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(0f, Some(TaskProgressInfo(1))))
    listener.stageIdToData((4, 1)).numCompleteTasks = listener.stageIdToData((4, 1)).numCompleteTasks + 1
    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(1))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))

    sendStageCompletedToListener(listener, 2)
    sendStageSubmittedToListener(listener, 3, 10)

    listener.stageIdToData((3, 1)).numCompleteTasks = listener.stageIdToData((3, 1)).numCompleteTasks + 10
    sendStageCompletedToListener(listener, 3)

    listener.getCommandProgress() shouldEqual List(ProgressInfo(100f, Some(TaskProgressInfo(0))), ProgressInfo(2.5f, Some(TaskProgressInfo(1))))
  }  */
}
