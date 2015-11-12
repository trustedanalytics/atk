package org.trustedanalytics.atk.rest.v1

import org.apache.commons.httpclient.auth.AuthenticationException
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.datacatalog.{ DataCatalogResponseJsonProtocol, TrustedAnalyticsDataCatalog }
import org.trustedanalytics.atk.rest.CommonDirectives
import spray.http.StatusCodes
import spray.routing.Directives
import org.trustedanalytics.atk.event.EventLogging
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._
import DataCatalogResponseJsonProtocol._

class DataCatalogService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  /**
   * The spray routes defining the Data Catalog service.
   */
  def dataCatalogRoutes() = {
    val prefix = "datacatalog"

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        onComplete(TrustedAnalyticsDataCatalog.list) {
          case Success(catalogs) => complete(catalogs)
          case Failure(ex) =>
            complete(StatusCodes.Unauthorized, ex.getMessage)
        }
    }
  }
}

