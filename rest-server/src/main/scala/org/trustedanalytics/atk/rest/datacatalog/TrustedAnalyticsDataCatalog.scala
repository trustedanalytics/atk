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
package org.trustedanalytics.atk.rest.datacatalog

import akka.actor.ActorSystem
import org.trustedanalytics.atk.domain.StringValue
import org.trustedanalytics.atk.domain.datacatalog._
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column }
import org.trustedanalytics.atk.domain.util.DataToJson
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.rest.v1.DataCatalogRestImplicitsWithSprayHttpx
import org.trustedanalytics.atk.rest.{ RestServerConfig, CfRequests }
import scala.reflect.runtime.universe._
import scala.concurrent._
import scala.util._
import spray.client.pipelining._
import scala.concurrent.Future
import DataCatalogRestResponseJsonProtocol._
import java.net.URLEncoder

object TrustedAnalyticsDataCatalog extends EventLogging {

  /**
   * Use reflection to get members of a case class and their types
   */
  def getCaseClassParameters[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList.map(elem => (elem.name.toString, elem.returnType)).reverse

  // Case Boolean columns to String as booleans are not yet supported in Schema
  def maskBooleanColumnsToString(array: Array[Any]) = {
    for {
      i <- array
      value = if (i.isInstanceOf[Boolean]) i.toString else i
    } yield value
  }

  // Validate if Server has data catalog dependency
  def validateDataCatalogDependency() {
    require(RestServerConfig.dataCatalogUri != "<INVALID DATA_CATALOG_URI>",
      "Server does not point to a valid data catalog")
  }

  /**
   * List Datacatalog and create a list response
   * @param invocation Invocation object which holds the user information and other credentials
   * @return CatalogServiceResponse: JSON Serialized Catalog entry with schema which can be sent back to client
   */
  def list(implicit invocation: Invocation): Future[CatalogServiceResponse] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    future {
      validateDataCatalogDependency()
      val oauthToken = invocation.user.token.getOrElse(throw new RuntimeException("User has no OAuth token defined"))

      val metadata = getCaseClassParameters[TapDataCatalogResponse]
      val schema = FrameSchema(for { (name, dtype) <- metadata } yield Column(name, dtype))

      val headers = List(("Authorization", s"Bearer $oauthToken"))

      val numEntriesResponse = CfRequests.httpsGetQuery(RestServerConfig.dataCatalogUri, s"/rest/datasets/count", headers)
      val numEntries = numEntriesResponse.convertTo[Int]

      info(s"Datasets count in Data Catalog $numEntries")

      val queryString = s"""{\"from\":0,\"size\":$numEntries}"""
      val encodedQueryString = URLEncoder.encode(queryString, "UTF-8")

      val res = CfRequests.httpsGetQuery(RestServerConfig.dataCatalogUri, s"/rest/datasets?query=$encodedQueryString", headers)

      import org.trustedanalytics.atk.domain.datacatalog.DataCatalogResponseJsonProtocol._
      val dataCatalogResponse = res.asJsObject.convertTo[DataCatalogResponse]

      val rawData = dataCatalogResponse.hits.map(elem => maskBooleanColumnsToString(elem.productIterator.toArray)).toIterable
      val data = DataToJson(rawData)
      CatalogServiceResponse(schema, data)
    }
  }

  /**
   * Publish to data catalog given the catalog metadata
   * In case this method fails or data catalog has not been configured for this environment, return the published file
   * path back to the client
   * @param exportMetadata Metadata entry which has been exported by plugins and needs to be published to data catalog
   * @param invocation Invocation object which holds the user information and other credentials
   * @return File path to final datacatalog entry (target URI)
   */
  def publishToDataCatalog(exportMetadata: ExportMetadata)(implicit invocation: Invocation): StringValue = {

    try {
      validateDataCatalogDependency()
      implicit val system = ActorSystem()
      import system.dispatcher

      val pipeline = (
        addHeader("Accept", "application/json")
        ~> addHeader("Authorization", s"Bearer ${invocation.user.token.get}")
        ~> sendReceive
      )

      // Generate random id entry for data catalog
      val randomGen = Random.alphanumeric
      val id = s"${randomGen.slice(0, 8).mkString}-" ++ s"${randomGen.slice(8, 12).mkString}-" ++
        s"${randomGen.slice(12, 16).mkString}-" ++ s"${randomGen.slice(16, 20).mkString}-" ++
        s"${randomGen.slice(20, 32).mkString}"

      val orgUUID = invocation.user.appOrgId.getOrElse(throw new RuntimeException("Missing organization ID for the application"))
      import DataCatalogRestImplicitsWithSprayHttpx._

      val uri = s"http://${RestServerConfig.dataCatalogUri}/rest/datasets/$id"
      info(s"Seding request to URI $uri")

      val response = pipeline {
        Put(uri, ConvertExportMetadataToInputMetadataEntry.convert(exportMetadata, orgUUID))
      }

      response.onComplete {
        case Success(r) =>
          info("Suceeded in adding to data catalog")
          Unit
        case Failure(ex) =>
          error("Error while adding to data catalog")
          error(ex.getMessage)
          throw ex
      }
    }
    catch {
      case ex: Throwable =>
        error(s"Invalid Datacatalog dependency or error adding to data catalog ${ex.getMessage}")
    }
    StringValue(exportMetadata.targetUri)
  }

}
