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

package org.apache.spark.sql.parquet.atk.giraph.frame

import org.apache.hadoop.mapreduce.JobStatus.State
import org.apache.hadoop.mapreduce.{ JobContext, TaskAttemptContext, OutputCommitter }

/**
 * A Committer that wraps multiple Committers.
 *
 * This is helpful when output is more than one file.
 */
class MultiOutputCommitter(val committers: List[OutputCommitter]) extends OutputCommitter {

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = {
    committers.exists(committer => committer.needsTaskCommit(taskAttemptContext))
  }

  override def setupJob(jobContext: JobContext): Unit = {
    committers.foreach(_.setupJob(jobContext))
  }

  override def setupTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.setupTask(taskAttemptContext))
  }

  override def commitTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.commitTask(taskAttemptContext))
  }

  override def abortTask(taskAttemptContext: TaskAttemptContext): Unit = {
    committers.foreach(_.abortTask(taskAttemptContext))
  }

  override def isRecoverySupported(jobContext: JobContext): Boolean = super.isRecoverySupported(jobContext)

  override def abortJob(jobContext: JobContext, state: State): Unit = {
    super.abortJob(jobContext, state)
    committers.foreach(_.abortJob(jobContext, state))
  }

  override def commitJob(jobContext: JobContext): Unit = {
    super.commitJob(jobContext)
    committers.foreach(_.commitJob(jobContext))
  }

  override def recoverTask(taskContext: TaskAttemptContext): Unit = {
    super.recoverTask(taskContext)
    committers.foreach(_.recoverTask(taskContext))
  }
}
