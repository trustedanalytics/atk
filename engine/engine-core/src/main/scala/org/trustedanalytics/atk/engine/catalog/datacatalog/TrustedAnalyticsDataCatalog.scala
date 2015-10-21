package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.apache.commons.httpclient.{ HttpsURL, HttpURL }
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{ CloseableHttpResponse, HttpGet }
import org.apache.http.impl.client.HttpClients
import org.trustedanalytics.atk.domain.catalog.{ DataCatalog, GenericCatalogResponse, CatalogResponse }
import org.trustedanalytics.atk.engine.{ Engine, EngineConfig }
import org.trustedanalytics.atk.engine.plugin.Invocation

import spray.json.JsValue
import scala.concurrent.{ Future, Await }
import scala.util.{ Success, Failure }
import scala.concurrent.duration._
import spray.json.{ DefaultJsonProtocol }
import scala.reflect.runtime.universe._
import spray.json._
import DefaultJsonProtocol._

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

object DataCatalogResponseJsonProtocol extends DefaultJsonProtocol {
  implicit val indexedMetadataEntryFormat = jsonFormat12(IndexedMetadataEntryWithID)
  implicit val datacatalogResponseFormat = jsonFormat4(DataCatalogResponse)
}

object TrustedAnalyticsDataCatalog extends DataCatalog {

  override val name: String = "TrustedAnalytics"

  def getCaseClassParameters[T: TypeTag] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList.map(_.name.toString)

  override def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {
    val metadata = getCaseClassParameters[IndexedMetadataEntryWithID]

    val oauthTokenReceived = invocation.user.user.apiKey.getOrElse("Invalid OAuth token for TrustedAnalytics User")
    println(s"Oauth Token Received: $oauthTokenReceived")
    val oauthToken = EngineConfig.oauthToken
    println(s"Oauth Token ${oauthToken}")
    val headers = List(("Authorization", s"Bearer $oauthToken"))
    println(s"Data Catalog URI ${EngineConfig.dataCatalogUri}")
    val res = DataCatalogRequests.httpsGetQuery(EngineConfig.dataCatalogUri, s"/rest/datasets", headers)
    println(s"The result is ${res.prettyPrint}")
    import DataCatalogResponseJsonProtocol._
    val dataCatalogResponse = res.asJsObject.convertTo[DataCatalogResponse]

    val result = dataCatalogResponse.hits.groupBy(elem => elem.format).mapValues {
      case itemList: List[IndexedMetadataEntryWithID] =>
        val formattedList = itemList.map(elem => elem.productIterator.toList.map(_.toString))
        (metadata, formattedList)
    }

    result.map { case (formatType, data) => GenericCatalogResponse(s"$name : $formatType", data._1, data._2) }.toList
  }

}

object DataCatalogRequests {

  def httpsGetQuery(host: String, queryString: String, headers: List[(String, String)]): JsValue = {

    val httpClient = HttpClients.createDefault()
    val target = new HttpHost(host)

    val (proxyHostConfigString, proxyPortConfigString) = ("https.proxyHost", "https.proxyPort")
    try {
      val proxy = (sys.props.contains(proxyHostConfigString), sys.props.contains(proxyPortConfigString)) match {
        case (true, true) => Some(new HttpHost(sys.props.get(proxyHostConfigString).get, sys.props.get(proxyPortConfigString).get.toInt))
        case _ => None
      }

      val config = {
        val cfg = RequestConfig.custom().setConnectTimeout(30)
        if (proxy.isDefined)
          cfg.setProxy(proxy.get).build()
        else cfg.build()
      }

      val request = new HttpGet(queryString)
      for ((headerTag, headerData) <- headers)
        request.addHeader(headerTag, headerData)
      request.setConfig(config)

      println("Executing request " + request.getRequestLine() + " to " + target + " via " + proxy.getOrElse("No proxy") + " with headers: "
        + request.getAllHeaders.map(header => s"${header.getName}: ${header.getValue}").mkString(","))

      var response: Option[CloseableHttpResponse] = None
      try {
        response = Some(httpClient.execute(target, request))
        val inputStream = response.get.getEntity().getContent
        scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n").parseJson
      }
      catch {
        case ex: Throwable =>
          error(s"Error executing request ${ex.getMessage}")
          throw new RuntimeException(s"Error connecting to $host")
      }
      finally {
        if (response.isDefined)
          response.get.close()
      }
    }
    finally {
      httpClient.close()
    }
  }

}