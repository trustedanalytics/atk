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
package org.trustedanalytics.atk.engine

import org.trustedanalytics.atk.engine.command.mgmt.YarnJobShutdownHook
import org.trustedanalytics.atk.engine.jobcontext.JobContextStorageImpl
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.plugin.Call
import org.trustedanalytics.atk.engine.frame.{ SparkFrameStorage, FrameFileStorage }
import org.trustedanalytics.atk.engine.graph.{ SparkGraphStorage, HBaseAdminFactory, SparkGraphHBaseBackend }
import org.trustedanalytics.atk.engine.model.{ ModelFileStorage, ModelStorageImpl }
import org.trustedanalytics.atk.engine.partitioners.SparkAutoPartitioner
import org.trustedanalytics.atk.engine.user.UserStorage
import org.trustedanalytics.atk.engine.command._
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import org.trustedanalytics.atk.repository.{ Profile, SlickMetaStoreComponent, DbProfileComponent }

/**
 * Class Responsible for creating all objects necessary for instantiating an instance of the SparkEngine.
 */
abstract class AbstractEngineComponent extends DbProfileComponent
    with SlickMetaStoreComponent
    with EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  implicit lazy val startupCall = Call(null, EngineExecutionContext.global, null)

  val commandLoader: CommandLoader

  private lazy val commandPluginRegistry: CommandPluginRegistry = new CommandPluginRegistry(commandLoader)

  private val sparkContextFactory = SparkContextFactory

  val fileStorage = withMyClassLoader {
    new FileStorage()
  }

  private val sparkAutoPartitioner = new SparkAutoPartitioner(fileStorage)

  val frameFileStorage = new FrameFileStorage(EngineConfig.fsRoot, fileStorage)

  val frameStorage = new SparkFrameStorage(frameFileStorage, EngineConfig.pageSize, metaStore.asInstanceOf[SlickMetaStore], sparkAutoPartitioner)

  protected val backendGraphStorage: SparkGraphHBaseBackend = new SparkGraphHBaseBackend(hbaseAdminFactory = new HBaseAdminFactory)
  val graphStorage: SparkGraphStorage = new SparkGraphStorage(metaStore, backendGraphStorage, frameStorage)

  val modelFileStorage = new ModelFileStorage(EngineConfig.fsRoot, fileStorage)

  val modelStorage: ModelStorageImpl = new ModelStorageImpl(metaStore.asInstanceOf[SlickMetaStore], modelFileStorage)

  val userStorage = new UserStorage(metaStore.asInstanceOf[SlickMetaStore])

  val commandStorage = new CommandStorageImpl(metaStore.asInstanceOf[SlickMetaStore])

  lazy val commandExecutor: CommandExecutor = new CommandExecutor(engine, commandStorage, commandPluginRegistry)

  val jobContextStorage = new JobContextStorageImpl(metaStore.asInstanceOf[SlickMetaStore])

  override lazy val profile = withContext("engine connecting to metastore") {

    // Initialize a Profile from settings in the config
    val driver = EngineConfig.metaStoreConnectionDriver
    new Profile(Profile.jdbcProfileForDriver(driver),
      connectionString = EngineConfig.metaStoreConnectionUrl,
      driver,
      username = EngineConfig.metaStoreConnectionUsername,
      password = EngineConfig.metaStoreConnectionPassword,
      poolMaxActive = EngineConfig.metaStorePoolMaxActive)
  }(startupCall.eventContext)

  val engine = new EngineImpl(sparkContextFactory,
    commandExecutor, commandStorage, frameStorage, graphStorage, modelStorage, userStorage,
    sparkAutoPartitioner, jobContextStorage, fileStorage) {}

  YarnJobShutdownHook.createHook(jobContextStorage)
}
