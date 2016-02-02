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

import org.trustedanalytics.atk.engine.command.CommandLoader
import org.trustedanalytics.atk.engine.command.mgmt.YarnJobsMonitor
import org.trustedanalytics.atk.engine.gc.GarbageCollector
import org.trustedanalytics.atk.engine.util.{ JvmVersionReporter, EnvironmentLogger }
import org.trustedanalytics.atk.event.EventLogging

class EngineApplicationImpl extends AbstractEngineComponent with EngineApplication with EventLogging {

  EnvironmentLogger.log()
  EngineConfig.logSettings()
  JvmVersionReporter.check()

  override lazy val commandLoader = new CommandLoader(loadFromModules = true)

  metaStore.initializeSchema()

  // Some of the Engine initialization we can do in a background thread.
  BackgroundInit.initFunction = () => {
    new HdfsLibSync(fileStorage).syncLibs()
    GarbageCollector.startup(metaStore, frameFileStorage, backendGraphStorage, modelFileStorage)
  }

  BackgroundInit.start

  val yarnJobsMonitorThread = new Thread(new YarnJobsMonitor(engine))
  yarnJobsMonitorThread.start()
}

