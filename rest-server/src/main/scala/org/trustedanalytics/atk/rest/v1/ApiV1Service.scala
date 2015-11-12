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

package org.trustedanalytics.atk.rest.v1

import spray.routing._

/**
 * Single entry point for classes that implement the Trusted Analytics V1 REST API
 */
class ApiV1Service(val dataFrameService: FrameService,
                   val commandService: CommandService,
                   val graphService: GraphService,
                   val modelService: ModelService,
                   val dataCatalogService: DataCatalogService) extends Directives {
  def route: Route = {
    dataFrameService.frameRoutes() ~ commandService.commandRoutes() ~ graphService.graphRoutes() ~ modelService.modelRoutes() ~ dataCatalogService.dataCatalogRoutes()
  }
}
