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
    val oauthToken = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiIzMjdkMjhkNy1jM2NlLTRkNTctODQ4NS1jZGMxZjA5NGE0MzkiLCJzdWIiOiI2MzgwNGY2YS0wZmEyLTRjMjItODc1Yi1mYWEyNTYyMDAxMzUiLCJzY29wZSI6WyJzY2ltLnJlYWQiLCJjbG91ZF9jb250cm9sbGVyLmFkbWluIiwicGFzc3dvcmQud3JpdGUiLCJzY2ltLndyaXRlIiwib3BlbmlkIiwiY2xvdWRfY29udHJvbGxlci53cml0ZSIsImNsb3VkX2NvbnRyb2xsZXIucmVhZCIsImRvcHBsZXIuZmlyZWhvc2UiXSwiY2xpZW50X2lkIjoiY2YiLCJjaWQiOiJjZiIsImF6cCI6ImNmIiwiZ3JhbnRfdHlwZSI6InBhc3N3b3JkIiwidXNlcl9pZCI6IjYzODA0ZjZhLTBmYTItNGMyMi04NzViLWZhYTI1NjIwMDEzNSIsInVzZXJfbmFtZSI6ImFkbWluIiwiZW1haWwiOiJhZG1pbiIsImlhdCI6MTQ0MDAyMzcyMCwiZXhwIjoxNDQwMDI0MzIwLCJpc3MiOiJodHRwczovL3VhYS41Mi4yNC4xMTIuMjM5LnhpcC5pby9vYXV0aC90b2tlbiIsImF1ZCI6WyJkb3BwbGVyIiwic2NpbSIsIm9wZW5pZCIsImNsb3VkX2NvbnRyb2xsZXIiLCJwYXNzd29yZCIsImNmIl19.OY_Djrsc0jVkG8tTtAKmy_HE82EgEtzsh8S9SGJEdsEjjLxm3hEUjMSHlukhr2WknEkpuUEHFUaYlrJpvtYeRODRveR4WtxeKlXaor8zGNMYiJJm7tZOfiLm4wWUD9XHvQ8FiGj8QYvxXA2O_6sy465bGEk0Fg_sTZckxj-2D08"
    val headers = List(("Authorization", s"Bearer $oauthToken"))
    val res = DataCatalogRequests.httpsGetQuery("data-catalog.52.24.112.239.xip.io", s"/rest/datasets", headers)
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