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

package org.trustedanalytics.atk.engine.util

import java.security.AccessController
import javax.security.auth.Subject

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.EngineConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import org.trustedanalytics.hadoop.config.client.oauth.{ TapOauthToken, JwtToken }
import org.trustedanalytics.hadoop.config.client.{ ServiceType, Configurations, ServiceInstanceConfiguration }
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory
import scala.reflect.io.Directory
import scala.util.control.NonFatal
import org.trustedanalytics.hadoop.config.client.Property

import org.apache.http.message.BasicNameValuePair
import org.apache.http.auth.{ AuthScope, UsernamePasswordCredentials }
import org.apache.http.impl.client.{ BasicCredentialsProvider, HttpClientBuilder }
import org.apache.http.client.methods.{ HttpPost, CloseableHttpResponse }
import org.apache.http.HttpHost
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import java.util.{ ArrayList => JArrayList }
import spray.json._

/**
 * Static methods for accessing a Kerberos secured hadoop cluster.
 */
object KerberosAuthenticator extends EventLogging with EventLoggingImplicits with ClassLoaderAware {

  val DEFAULT_VALUE = ""
  val AUTHENTICATION_METHOD = "kerberos"
  val AUTHENTICATION_METHOD_PROPERTY = "hadoop.security.authentication"

  // TODO: Allow support for multiple keytabs once namespaces is implemented

  /**
   * Login to Kerberos cluster with classloader
   * @return Configuration Hadoop Configuration for the cluster
   */
  def loginConfigurationWithClassLoader(): Configuration = withMyClassLoader {
    loginUsingHadoopUtils().configuration
  }

  def getKerberosConfigJVMParam: Option[String] = sys.env.get("JAVA_KRB_CONF")

  def getPropertyValue(property: Property): String = {
    val helper = Configurations.newInstanceFromEnv()
    val result = helper.getServiceConfig(ServiceType.KERBEROS_TYPE).getProperty(property)
    result.isPresent match {
      case true => result.get()
      case false => DEFAULT_VALUE
    }
  }

  def isKerberosEnabled(hdfsConf: ServiceInstanceConfiguration): Boolean =
    isKerberosEnabled(hdfsConf.asHadoopConfiguration())

  def isKerberosEnabled(hadoopConf: Configuration) =
    AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY))

  def loginUsingHadoopUtils(): UserAuthenticatedConfiguration = withMyClassLoader {
    try {
      val helper = Configurations.newInstanceFromEnv()
      val hdfsConf = helper.getServiceConfig(ServiceType.HDFS_TYPE)
      val res = hdfsConf.asHadoopConfiguration()
      if (KerberosAuthenticator.isKerberosEnabled(hdfsConf)) {
        val kerberosProperties = new KerberosProperties
        val loginManager = KrbLoginManagerFactory.getInstance()
          .getKrbLoginManagerInstance(kerberosProperties.kdc, kerberosProperties.realm)
        val subject = loginManager.loginWithJWTtoken(new TapOauthToken(KerberosAuthenticator.getJwtToken()))
        loginManager.loginInHadoop(subject, res)
        UserAuthenticatedConfiguration(subject, res)
      }
      else UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), res)
    }
    catch {
      case t: Throwable =>
        info(s"Failed to loginUsingHadooputils. Either kerberos is not enabled or invalid setup or " +
          "using System credentials for authentication. Returning default configuration")
        UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
    }
  }

  def submitYarnJobAsAuthenticatedUser(jwtToken: Option[JwtToken]): UserAuthenticatedConfiguration = {
    jwtToken.isDefined match {
      case false => UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
      case true =>
        try {
          val helper = Configurations.newInstanceFromEnv()
          val hdfsConf = helper.getServiceConfig(ServiceType.YARN_TYPE)
          if (KerberosAuthenticator.isKerberosEnabled(hdfsConf)) {
            val kerberosProperties = new KerberosProperties
            val loginManager = KrbLoginManagerFactory.getInstance()
              .getKrbLoginManagerInstance(kerberosProperties.kdc, kerberosProperties.realm)
            val res = hdfsConf.asHadoopConfiguration()
            val subject = loginManager.loginWithJWTtoken(jwtToken.get)
            loginManager.loginInHadoop(subject, res)
            UserAuthenticatedConfiguration(subject, res)
          }
          else UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
        }
        catch {
          case t: Throwable =>
            info("Printing stack trace as to why kinit failed")
            t.printStackTrace()
            info(s"Failed to loginUsingHadooputils. Either kerberos is not enabled or invalid setup or " +
              "using System credentials for authentication. Returning default configuration")
            UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
        }
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

case class UserAuthenticatedConfiguration(subject: Subject, configuration: Configuration)

case class KerberosProperties(kdc: String = KerberosAuthenticator.getPropertyValue(Property.KRB_KDC),
                              realm: String = KerberosAuthenticator.getPropertyValue(Property.KRB_REALM),
                              user: String = KerberosAuthenticator.getPropertyValue(Property.USER),
                              password: String = KerberosAuthenticator.getPropertyValue(Property.PASSWORD))
