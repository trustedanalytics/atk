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

package org.trustedanalytics.atk.rest.v1

import org.trustedanalytics.atk.domain.datacatalog.{ DataCatalogRestImplicits, DataCatalogRestResponseJsonProtocol, CatalogMetadata }
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogResponseJsonProtocol._
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.datacatalog.TrustedAnalyticsDataCatalog
import org.trustedanalytics.atk.rest.CommonDirectives
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport
import spray.json._
import spray.routing.Directives
import org.trustedanalytics.atk.event.EventLogging
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

object DataCatalogRestImplicitsWithSprayHttpx extends DataCatalogRestImplicits with SprayJsonSupport

class DataCatalogService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Data Catalog service.
   */
  def dataCatalogRoutes() = {
    val prefix = "datacatalog"

    import DataCatalogRestImplicitsWithSprayHttpx._
    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        (path(prefix) & pathEnd) {
          requestUri { uri =>
            get {
              onComplete(TrustedAnalyticsDataCatalog.list) {
                case Success(catalogs) => complete(catalogs)
                case Failure(ex) =>
                  complete(StatusCodes.Unauthorized, ex.getMessage)
              }
            } ~ post {
              entity(as[CatalogMetadata]) {
                catalogmetadata =>
                  complete(TrustedAnalyticsDataCatalog.publishToDataCatalog(catalogmetadata))
              }
            }
          }
        }
    }
  }
}

