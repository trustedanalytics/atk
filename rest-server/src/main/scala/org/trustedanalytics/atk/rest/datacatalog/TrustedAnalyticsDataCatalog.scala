package org.trustedanalytics.atk.rest.datacatalog

import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes, Column }
import org.trustedanalytics.atk.domain.util.DataToJson
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.rest.{ RestServerConfig, CfRequests }
import spray.json._
import scala.reflect.runtime.universe._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

case class DataCatalogResponse(total: Int,
                               hits: List[IndexedMetadataEntryWithID],
                               formats: List[String],
                               categories: List[String])

case class IndexedMetadataEntryWithID(id: String,
                                      size: Int,
                                      title: String,
                                      dataSample: String,
                                      recordCount: Int,
                                      isPublic: Boolean,
                                      targetUri: String,
                                      orgUUID: String,
                                      category: String,
                                      format: String,
                                      creationTime: String,
                                      sourceUri: String)

object TrustedAnalyticsDataCatalog {

  def getCaseClassParameters[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList.map(elem => (elem.name.toString, elem.returnType)).reverse

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

  def maskBooleanColumnsToString(array: Array[Any]) = {
    for {
      i <- array
      value = if (i.isInstanceOf[Boolean]) i.toString else i
    } yield value
  }

  def list(implicit invocation: Invocation): Future[CatalogServiceResponse] = {
    future {
      val oauthToken = invocation.user.token.getOrElse(throw new RuntimeException("User has no OAuth token defined"))

      val metadata = getCaseClassParameters[IndexedMetadataEntryWithID]
      val schema = FrameSchema(for { (name, dtype) <- metadata } yield getColumn(name, dtype))

      val headers = List(("Authorization", s"Bearer $oauthToken"))
      val res = CfRequests.httpsGetQuery(RestServerConfig.dataCatalogUri, s"/rest/datasets", headers)

      import DataCatalogResponseJsonProtocol._
      val dataCatalogResponse = res.asJsObject.convertTo[DataCatalogResponse]

      val rawData = dataCatalogResponse.hits.map(elem => maskBooleanColumnsToString(elem.productIterator.toArray)).toIterable
      val data = DataToJson(rawData)
      CatalogServiceResponse(schema, data)
    }
  }

}
