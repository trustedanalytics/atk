package org.trustedanalytics.atk.rest.v1

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.atk.domain.catalog.GenericCatalogResponse
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.rest.CommonDirectives

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.rest.threading.SprayExecutionContext
import org.trustedanalytics.atk.rest.v1.CatalogServiceImplicits._
import org.trustedanalytics.atk.rest.v1.viewmodels.RelLink
import org.trustedanalytics.atk.rest.v1.viewmodels.ViewModelJsonImplicits
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol
import scala.concurrent._
import scala.util._
import org.trustedanalytics.atk.rest.v1.viewmodels.{ RelLink, ViewModelJsonImplicits }
import org.trustedanalytics.atk.rest.CommonDirectives
import spray.routing.Directives
import SprayExecutionContext.global
import org.trustedanalytics.atk.event.EventLogging

import spray.httpx.SprayJsonSupport
import spray.json._
import org.trustedanalytics.atk.spray.json.AtkDefaultJsonProtocol

case class GetCatalog(catalogName: String, links: List[RelLink])
case class CatalogServiceResponse(catalogName: String, metadata: List[String], data: List[List[String]])

case class DataCatalogServiceResponse(catalogName: String, metadata: List[String], data: List[List[String]])

object CatalogServiceImplicits extends AtkDefaultJsonProtocol with SprayJsonSupport {

  //this is needed for implicits
  import ViewModelJsonImplicits._
  implicit val dataCatalogResponseFormat = jsonFormat3(DataCatalogServiceResponse)
  implicit val getCatalogFormat = jsonFormat2(GetCatalog)
  implicit val catalogResponseFormat = jsonFormat3(CatalogServiceResponse)

}

class CatalogService(commonDirectives: CommonDirectives, engine: Engine) extends Directives with EventLogging {

  def catalogRoutes() = {
    val prefix = "catalogs"

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        (path(prefix) & pathEnd) {
          requestUri { uri =>
            val baseUri = StringUtils.substringBeforeLast(uri.toString(), "/")
            val entities = List("frames", "graphs", "models", "data")
            val uris = for {
              i <- entities
            } yield GetCatalog(s"${i}_catalog", List(RelLink("catalog_uri", s"$baseUri/$i/catalog", "GET")))

            import CatalogServiceImplicits._
            implicit val getCatalogFormat = CatalogServiceImplicits.getCatalogFormat
            complete(uris)
          }
        }
    }
  }

  /**
   * The spray routes defining the Catalog service.
   */
  def dataCatalogRoutes() = {
    val prefix = "data"

    commonDirectives(prefix) {
      implicit invocation: Invocation =>
        pathPrefix(prefix / "catalog") {
          requestUri { uri =>
            onComplete(engine.listCatalog(prefix)) {
              case Success(catalogs) =>
                val baseUri = StringUtils.substringBeforeLast(uri.toString(), "/")
                import CatalogServiceImplicits._
                val response = catalogs.map(_.asInstanceOf[GenericCatalogResponse])
                  .map(catalog => DataCatalogServiceResponse(catalog.name, catalog.metadata, catalog.data))
                complete(response)
              case Failure(ex) => throw ex
            }
          }
        }
    }
  }
}
