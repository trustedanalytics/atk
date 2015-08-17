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

package org.trustedanalytics.atk.rest.factory

import akka.actor.{ ActorRef, Props }
import akka.io.IO
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest._
import spray.can.Http
import spray.routing.Directives
import akka.pattern.ask

// Creates a factory for Api Service with support for all routes
class StandardServiceFactory(override val name: String) extends AbstractServiceFactory {
  override def createServiceDefinition(engine: Engine)(implicit invocation: Invocation): ApiService = {
    // setup common directives
    val serviceAuthentication = new AuthenticationDirective(engine)
    val commonDirectives = new CommonDirectives(serviceAuthentication)

    // setup V1 Services
    val commandService = new v1.CommandService(commonDirectives, engine)
    val dataFrameService = new v1.FrameService(commonDirectives, engine)
    val graphService = new v1.GraphService(commonDirectives, engine)
    val modelService = new v1.ModelService(commonDirectives, engine)
    val apiV1Service = new v1.ApiV1Service(dataFrameService, commandService, graphService, modelService)

    // setup main entry point
    new ApiService(commonDirectives, apiV1Service)
  }
  override def createActorProps(service: Directives): Props = {
    Props(new ApiServiceActor(service.asInstanceOf[ApiService]))
  }
}

class StandardServiceFactoryOnHttps(override val name: String)
    extends StandardServiceFactory(name) with RestSslConfiguration {
  override def startInstance(serviceInstance: ActorRef): Unit = {
    IO(Http) ? Http.Bind(serviceInstance, interface = RestServerConfig.host, port = RestServerConfig.port)
  }
}
