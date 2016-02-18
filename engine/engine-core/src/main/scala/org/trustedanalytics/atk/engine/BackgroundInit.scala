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

package org.trustedanalytics.atk.engine

import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.event.EventLogging

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration
import EngineExecutionContext._

/**
 * Some of the Engine initialization we can do in a background thread.
 *
 * For example, at start up we want to begin copying libraries we'll need into HDFS
 * but we don't want to slow the REST Server from starting up while we do this.
 *
 * We don't actually need the libraries in HDFS until we go to run our first command.
 */
object BackgroundInit extends EventLogging {

  /**
   * Ugly global for waiting
   */
  private[engine] var initFunction: () => Unit = null

  /** start the initFunction */
  lazy val start: Future[Unit] = Future {
    initFunction()
  }

  /**
   * Wait till initFunction is completed
   */
  lazy val waitTillCompleted: Unit = {
    if (initFunction == null) {
      warn("initFunciton wasn't initialized before waiting")
    }
    else {
      Await.result(start, Duration(5, TimeUnit.MINUTES))
    }
  }

}