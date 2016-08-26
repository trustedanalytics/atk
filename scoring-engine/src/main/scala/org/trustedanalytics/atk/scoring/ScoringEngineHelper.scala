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

package org.trustedanalytics.atk.scoring

import java.io.File
import java.net.URI

import org.apache.http.HttpHost
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.message.BasicNameValuePair
import org.apache.http.impl.client.{ BasicCredentialsProvider, HttpClientBuilder }
import org.apache.http.client.methods.{ CloseableHttpResponse, HttpPost }
import org.apache.http.client.config.RequestConfig
import org.trustedanalytics.atk.event.EventLogging
import spray.json.JsString
import java.util.{ ArrayList => JArrayList }

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.model.publish.format.ModelPublishFormat
import org.trustedanalytics.atk.scoring.interfaces.Model
import org.trustedanalytics.hadoop.config.client.helper.Hdfs
import org.trustedanalytics.hadoop.config.client.oauth.TapOauthToken
import spray.json._

object ScoringEngineHelper extends EventLogging {

  /**
   *
   * @param model        Original model
   * @param revisedModel Revised model
   * @return true if model compatible i.e model type is same and model input/output parameters are same
   *         else returns false
   */
  def isModelCompatible(model: Model, revisedModel: Model): Boolean = {
    if (model.modelMetadata().modelType == revisedModel.modelMetadata().modelType &&
      model.input().deep == revisedModel.input().deep &&
      model.output().deep == revisedModel.output().deep) {
      return true
    }
    false
  }

  def getModel(modelFilePath: String): Model = {
    var tempTarFile: File = null
    var tarFilePath = modelFilePath
    try {
      if (tarFilePath.startsWith("hdfs:/")) {
        if (!tarFilePath.startsWith("hdfs://")) {
          val relativePath = tarFilePath.substring(tarFilePath.indexOf("hdfs:") + 6)
          tarFilePath = "hdfs://" + relativePath
        }

        val hdfsFileSystem = try {
          val token = new TapOauthToken(getJwtToken())
          println(s"Successfully retreived a token for user ${token.getUserName}")
          Hdfs.newInstance().createFileSystem(token)
        }
        catch {
          case t: Throwable =>
            t.printStackTrace()
            info("Failed to create HDFS instance using hadoop-library. Default to FileSystem")
            org.apache.hadoop.fs.FileSystem.get(new URI(tarFilePath), new Configuration())
        }
        tempTarFile = File.createTempFile("modelTar", ".tar")
        hdfsFileSystem.copyToLocalFile(false, new Path(tarFilePath), new Path(tempTarFile.getAbsolutePath))
        tarFilePath = tempTarFile.getAbsolutePath
      }
      ModelPublishFormat.read(new File(tarFilePath), this.getClass.getClassLoader.getParent)
    }
    finally {
      FileUtils.deleteQuietly(tempTarFile)
    }
  }

  def getJwtToken(): String = withContext("httpsGetQuery") {

    val query = s"http://${System.getenv("UAA_URI")}/oauth/token"
    val headers = List(("Accept", "application/json"))
    val data = List(("username", System.getenv("FS_TECHNICAL_USER_NAME")), ("password", System.getenv("FS_TECHNICAL_USER_PASSWORD")), ("grant_type", "password"))
    val credentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(System.getenv("UAA_CLIENT_NAME"), System.getenv("UAA_CLIENT_PASSWORD")))

    // TODO: This method uses Apache HttpComponents HttpClient as spray-http library does not support proxy over https
    val (proxyHostConfigString, proxyPortConfigString) = ("https.proxyHost", "https.proxyPort")
    val httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
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

      val request = new HttpPost(query)
      val nvps = new JArrayList[BasicNameValuePair]
      data.foreach { case (k, v) => nvps.add(new BasicNameValuePair(k, v)) }
      request.setEntity(new UrlEncodedFormEntity(nvps))

      for ((headerTag, headerData) <- headers)
        request.addHeader(headerTag, headerData)
      request.setConfig(config)

      var response: Option[CloseableHttpResponse] = None
      try {
        response = Some(httpClient.execute(request))
        val inputStream = response.get.getEntity().getContent
        val result = scala.io.Source.fromInputStream(inputStream).getLines().mkString("\n")
        println(s"Response From UAA Server is $result")
        result.parseJson.asJsObject().getFields("access_token") match {
          case values => values(0).asInstanceOf[JsString].value
        }
      }
      catch {
        case ex: Throwable =>
          error(s"Error executing request ${ex.getMessage}")
          // We need this exception to be thrown as this is a generic http request method and let caller handle.
          throw ex
      }
      finally {
        if (response.isDefined)
          response.get.close()
      }
    }
    finally {
      httpClient.close()
    }
  }(null)

}
