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
package org.trustedanalytics.atk.testutils

import org.apache.log4j.{ Level, Logger }

/**
 * Utility methods related to logging in Unit testing.
 * <p>
 * Logging of underlying libraries can get annoying in unit
 * tests so it is nice to be able to change easily.
 * </p>
 *
 * TODO: this class doesn't really seem to work any more.  It needs to be updated to turn off logging.
 *
 */
object LogUtils {

  /**
   * Turn down logging since Spark gives so much output otherwise.
   */
  def silenceSpark() {
    setLogLevels(Level.WARN, Seq("o.a.spark.scheduler.DAGScheduler",
      "o.a.spark.scheduler.TaskSetManager", "org.eclipse.jetty", "akka"))
  }

  private def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
  }

}
