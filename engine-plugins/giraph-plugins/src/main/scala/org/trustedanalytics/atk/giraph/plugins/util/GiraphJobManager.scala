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

package org.trustedanalytics.atk.giraph.plugins.util

import java.io.File

import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.job.{ DefaultJobObserver, GiraphJob }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.filecache.DistributedCache
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.trustedanalytics.atk.engine.plugin.{ CommandInvocation, Invocation }
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.moduleloader.Module

import scala.collection.mutable

object GiraphJobListener {
  // TODO: this map doesn't make any sense, there is only one instance of CommandStorage in the Engine so we just need a single reference
  var commandIdMap = new mutable.HashMap[Long, CommandStorage]
}

/**
 * GiraphJobListener overrides jobRunning method which gets called after the internal hadoop job has been submitted
 * We update the progress for the commandId to the commandStorage periodically until the job is complete
 */
class GiraphJobListener extends DefaultJobObserver {

  override def launchingJob(jobToSubmit: Job) = {
    val commandId = getCommandId(jobToSubmit)
    val commandStorage = getCommandStorage(commandId)
    commandStorage.updateProgress(commandId, List(ProgressInfo(0.0f, None)))
  }

  override def jobRunning(submittedJob: Job) = {
    val commandId = getCommandId(submittedJob)
    val commandStorage = getCommandStorage(commandId)
    Stream.continually(submittedJob.isComplete).takeWhile(_ == false).foreach {
      _ =>
        {
          val conf = submittedJob.getConfiguration
          val str = conf.get("giraphjob.maxSteps")
          var maxSteps: Float = 20
          if (str != null) {
            maxSteps = str.toInt + 4 //4 for init, input, step and shutdown
          }
          val group = submittedJob.getCounters.getGroup("Giraph Timers")
          if (null != group) {
            var progress = Math.max((group.size() - 1), 0) / maxSteps
            if (progress > 0.95) progress = 0.95f //each algorithm calculates steps differently and this sometimes cause it to be greater than 1. It is easier to fix it here
            commandStorage.updateProgress(commandId, List(ProgressInfo(progress * 100, None)))
          }
          else {
            commandStorage.updateProgress(commandId, List(ProgressInfo(submittedJob.mapProgress() * 100, None)))
          }
        }
    }
  }

  override def jobFinished(jobToSubmit: Job, passed: Boolean) = {
    val commandId = getCommandId(jobToSubmit)
    GiraphJobListener.commandIdMap.-(commandId)
    println(jobToSubmit.toString)
    if (!jobToSubmit.isSuccessful) {
      val taskCompletionEvents = jobToSubmit.getTaskCompletionEvents(0)
      taskCompletionEvents.lastOption match {
        case Some(e) =>
          val diagnostics = jobToSubmit.getTaskDiagnostics(e.getTaskAttemptId)(0)
          val errorMessage = diagnostics.lastIndexOf("Caused by:") match {
            case index if index > 0 => diagnostics.substring(index)
            case _ => diagnostics
          }
          throw new Exception(s"Execution was unsuccessful. $errorMessage")
        case None => throw new Exception("Execution was unsuccessful, but no further information was provided. " +
          "Consider checking server logs for further information.")
      }
    }
  }

  private def getCommandId(job: Job): Long = job.getConfiguration.getLong(GiraphJobManager.CommandIdPropertyName, 0)

  private def getCommandStorage(commandId: Long): CommandStorage = GiraphJobListener.commandIdMap.getOrElse(commandId, null)

}

/**
 * GiraphManager invokes the Giraph Job and waits for completion. Upon completion - it reads and returns back the
 * report to caller
 */
object GiraphJobManager {

  val CommandIdPropertyName = "atk.commandId"

  def run(jobName: String,
          computationClassCanonicalName: String,
          giraphConf: GiraphConfiguration,
          invocation: Invocation,
          reportName: String): String = {

    val commandInvocation = invocation.asInstanceOf[CommandInvocation]
    GiraphJobListener.commandIdMap(commandInvocation.commandId) = commandInvocation.commandStorage

    giraphConf.setLong(CommandIdPropertyName, commandInvocation.commandId)

    // make sure jars are in HDFS
    BackgroundInit.waitTillCompleted

    // Use jars that have been cached in HDFS
    val hdfs = new FileStorage
    val hdfsLibs = hdfs.hdfsLibs(Module.allJarNames("giraph-plugins"))
    hdfsLibs.foreach(path => DistributedCache.addFileToClassPath(new Path(path), giraphConf))

    // Clear Giraph Report Directory
    val fs = FileSystem.get(new Configuration())
    val outputDir = outputDirectory(fs, commandInvocation.commandId)
    fs.delete(outputDir, true)

    val job = new GiraphJob(giraphConf, jobName)
    FileOutputFormat.setOutputPath(job.getInternalJob, outputDir)

    job.run(true) match {
      case false => throw new RuntimeException("Error: No Learning Report found!!")
      case true =>
        val stream = fs.open(getFullyQualifiedPath(outputDir + File.separator + reportName, fs))
        def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
        val result = readLines.takeWhile(_ != null).toList.mkString("\n")

        fs.delete(outputDir, true)

        result
    }
  }

  /**
   * Get the output directory
   */
  def outputDirectory(fs: FileSystem, commandId: Long): Path = {
    val path = EngineConfig.fsRoot +
      File.separator +
      "giraph-output-tmp" +
      File.separator + commandId
    getFullyQualifiedPath(path, fs)
  }

  def getFullyQualifiedPath(path: String, fs: FileSystem): Path = {
    fs.makeQualified(Path.getPathWithoutSchemeAndAuthority(new Path(path)))
  }

}