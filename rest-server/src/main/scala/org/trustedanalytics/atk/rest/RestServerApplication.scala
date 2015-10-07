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

package org.trustedanalytics.atk.rest

import org.trustedanalytics.atk.engine.plugin.{ Call, Invocation }
import org.trustedanalytics.atk.moduleloader.{ ClassLoaderAware, Module, Component }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.engine.{ EngineApplication, Engine }
import com.typesafe.config.ConfigFactory
import org.trustedanalytics.atk.rest.factory.ServiceFactoryCreator

/**
 * A REST application used by client layer to communicate with the Engine.
 *
 * See the 'api_server.sh' to see how the launcher starts the application.
 */
class RestServerApplication extends Component with EventLogging with ClassLoaderAware {

  val config = ConfigFactory.load(this.getClass.getClassLoader)

  EventLogging.raw = true
  info("REST server setting log adapter from configuration")

  EventLogging.raw = config.getBoolean("trustedanalytics.atk.api.logging.raw")
  info("REST server set log adapter from configuration")

  EventLogging.profiling = config.getBoolean("trustedanalytics.atk.api.logging.profile")
  info(s"REST server profiling: ${EventLogging.profiling}")

  private var engine: Engine = null

  /**
   * Main entry point to start the API Service Application
   */
  override def start(): Unit = {
    implicit val call = Call(null, SprayExecutionContext.global)
    engine = initializeEngine()

    withMyClassLoader {
      val serviceFactory = ServiceFactoryCreator.createFactory(RestServerConfig.serviceMode, RestServerConfig.schemeIsHttps)
      val scheme = if (RestServerConfig.schemeIsHttps) "https" else "http"
      info(s"Binding service to $scheme://${RestServerConfig.host}:${RestServerConfig.port}")
      // Bind the Spray Actor to an HTTP(s) Port
      val serviceInstance = serviceFactory.createInstance(engine)
      serviceFactory.startInstance(serviceInstance)
    }
  }

  /**
   * Stop this component
   */
  override def stop(): Unit = {
    engine.shutdown()
  }

  /**
   * Initialize API Server dependencies and perform dependency injection as needed.
   */
  private def initializeEngine()(implicit invocation: Invocation): Engine = {
    val engineApp: EngineApplication = Module.load("engine", "org.trustedanalytics.atk.engine.EngineApplicationImpl")
    engineApp.engine
  }

}
