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

package org.trustedanalytics.atk.rest.datacatalog

import akka.actor.ActorSystem
import org.trustedanalytics.atk.domain.StringValue
import org.trustedanalytics.atk.domain.datacatalog._
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes, Column }
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

object TrustedAnalyticsDataCatalog extends EventLogging {

  /**
   * Use reflection to get members of a case class and their types
   */
  def getCaseClassParameters[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList.map(elem => (elem.name.toString, elem.returnType)).reverse

  /**
   * Create Column given column name and scala type
   * @param name Column name
   * @param dtype runtime type (using scala reflection)
   * @return Column
   */
  def getColumn(name: String, dtype: reflect.runtime.universe.Type) = {
    val columnDataType = dtype match {
      case t if t <:< definitions.IntTpe => DataTypes.int32
      case t if t <:< definitions.LongTpe => DataTypes.int64
      case t if t <:< definitions.FloatTpe => DataTypes.float32
      case t if t <:< definitions.DoubleTpe => DataTypes.float64
      case _ => DataTypes.string
    }
    Column(name, columnDataType)
  }

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

  // QUery Datacatalog and create a list response
  def list(implicit invocation: Invocation): Future[CatalogServiceResponse] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    future {
      validateDataCatalogDependency()
      val oauthToken = invocation.user.token.getOrElse(throw new RuntimeException("User has no OAuth token defined"))

      val metadata = getCaseClassParameters[IndexedMetadataEntryWithID]
      val schema = FrameSchema(for { (name, dtype) <- metadata } yield getColumn(name, dtype))

      val headers = List(("Authorization", s"Bearer $oauthToken"))
      val res = CfRequests.httpsGetQuery(RestServerConfig.dataCatalogUri, s"/rest/datasets", headers)

      import org.trustedanalytics.atk.domain.datacatalog.DataCatalogResponseJsonProtocol._
      val dataCatalogResponse = res.asJsObject.convertTo[DataCatalogResponse]

      val rawData = dataCatalogResponse.hits.map(elem => maskBooleanColumnsToString(elem.productIterator.toArray)).toIterable
      val data = DataToJson(rawData)
      CatalogServiceResponse(schema, data)
    }
  }

  // Publish to data catalog given the catalog metadata
  def publishToDataCatalog(catalogMetadata: CatalogMetadata)(implicit invocation: Invocation): StringValue = {

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
        Put(uri, ConvertCatalogMetadataToInputMetadataEntry.convert(catalogMetadata, orgUUID))
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
      StringValue(catalogMetadata.targetUri)
    }
    catch {
      case ex: Throwable =>
        error(s"Invalid Datacatalog dependency or error adding to data catalog ${ex.getMessage}")
        StringValue(catalogMetadata.targetUri)
    }
  }

}
