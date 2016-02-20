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

import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.domain.jobcontext.JobContext
import org.trustedanalytics.atk.engine.{ EngineConfig, Engine }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.event.EventLogging

/**
 * Runs in the REST server to monitor YARN jobs that should be actively running commands
 */
class YarnJobsMonitor(engine: Engine)(implicit invocation: Invocation) extends Runnable with EventLogging {

  lazy val timeoutMinutes: Long = EngineConfig.yarnMonitorTaskTimeout

  def run(): Unit = {
    info(s"YarnJobsMonitor started.  Task timeout is $timeoutMinutes minutes.")
    while (true) {
      engine.getCommandsNotComplete().foreach { command =>
        engine.getCommandJobContext(command) match {
          case Some(context) => if (hasStaleContext(context)) {
            engine.cancelCommand(command.id, Some(s" by ATK context monitor due to timeout.  The job context ${context.clientId} has not provided an update for more than $timeoutMinutes minutes.  This may indicate that a task is running for a very long time.  Try increasing the 'trustedanalytics.atk.engine.yarn-monitor-task-timeout' config setting."))
          }
          case None => ; // there is no know YARN job to shutdown (command remains not complete, but this is not the responsibility of a YARN jobs monitor
        }
      }
      TimeUnit.MINUTES.sleep(timeoutMinutes)
    }
  }

  def hasStaleContext(context: JobContext): Boolean =
    System.currentTimeMillis() - context.modifiedOn.getMillis > timeoutMinutes * 60 * 1000
}
