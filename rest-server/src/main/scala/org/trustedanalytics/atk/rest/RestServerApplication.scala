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

package org.trustedanalytics.atk.rest

import org.trustedanalytics.atk.engine.plugin.{ Call, Invocation }
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import scala.concurrent.duration._
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.component.{ Boot, ArchiveDefinition, Archive }
import org.trustedanalytics.atk.engine.Engine
import com.typesafe.config.Config
import scala.concurrent.Await
import scala.reflect.ClassTag
import org.trustedanalytics.atk.rest.factory.ServiceFactoryCreator

/**
 * A REST application used by client layer to communicate with the Engine.
 *
 * See the 'api_server.sh' to see how the launcher starts the application.
 */
class RestServerApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging {

  EventLogging.raw = true
  info("REST server setting log adapter from configuration")

  EventLogging.raw = configuration.getBoolean("trustedanalytics.atk.api.logging.raw")
  info("REST server set log adapter from configuration")

  EventLogging.profiling = configuration.getBoolean("trustedanalytics.atk.api.logging.profile")
  info(s"REST server profiling: ${EventLogging.profiling}")

  //Direct subsequent archive messages to the normal log
  Archive.logger = s => info(s)
  Archive.logger("Archive logger installed")

  // TODO: implement or remove get()
  def get[T](descriptor: String): T = {
    throw new IllegalArgumentException("This component provides no services")
  }

  /**
   * Main entry point to start the API Service Application
   */
  override def start() = {
    implicit val call = Call(null, SprayExecutionContext.global)
    val engine = initializeEngine()
    val serviceFactory = ServiceFactoryCreator.createFactory(RestServerConfig.serviceMode, RestServerConfig.schemeIsHttps)
    val scheme = if (RestServerConfig.schemeIsHttps) "https" else "http"
    info(s"Binding service to $scheme://${RestServerConfig.host}:${RestServerConfig.port}")
    // Bind the Spray Actor to an HTTP(s) Port
    val serviceInstance = serviceFactory.createInstance(engine)
    serviceFactory.startInstance(serviceInstance)
  }

  /**
   * Initialize API Server dependencies and perform dependency injection as needed.
   */
  private def initializeEngine()(implicit invocation: Invocation): Engine = {

    //TODO: later engine will be initialized in a separate JVM
    lazy val engine = Boot.getArchive("engine-core")
      .get[Engine]("engine")

    //make sure engine is initialized
    Await.ready(engine.getCommands(0, 1), 30 seconds)
    engine
  }

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    throw new Exception("REST server provides no components at this time")
  }
}
