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

import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

object EngineExecutionContext {

  val config = ConfigFactory.load(this.getClass.getClassLoader)

  val maxThreadsPerExecutionContext: Int = {
    config.getInt("trustedanalytics.atk.engine.spark.max-threads-per-execution-Context")
  }

  implicit val global: ExecutionContext =
    ExecutionContext.fromExecutorService(new ForkJoinPool(maxThreadsPerExecutionContext))
}