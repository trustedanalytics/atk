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
import org.trustedanalytics.hadoop.config.{ ConfigurationHelperImpl, PropertyLocator }
import org.trustedanalytics.hadoop.config.client.{ ServiceType, Configurations, ServiceInstanceConfiguration }
import org.trustedanalytics.hadoop.kerberos.KrbLoginManagerFactory
import scala.reflect.io.Directory
import scala.util.control.NonFatal

/**
 * Static methods for accessing a Kerberos secured hadoop cluster.
 */
object KerberosAuthenticator extends EventLogging with EventLoggingImplicits with ClassLoaderAware {

  val confHelper = ConfigurationHelperImpl.getInstance()
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

  def getPropertyValue(property: PropertyLocator): String = {
    val value = confHelper.getPropertyFromEnv(property)
    value.isPresent match {
      case true => value.get()
      case false => DEFAULT_VALUE
    }
  }

  def isKerberosEnabled(hdfsConf: ServiceInstanceConfiguration): Boolean =
    isKerberosEnabled(hdfsConf.asHadoopConfiguration())

  def isKerberosEnabled(hadoopConf: Configuration) =
    AUTHENTICATION_METHOD.equals(hadoopConf.get(AUTHENTICATION_METHOD_PROPERTY))

  def loginUsingHadoopUtils(): UserAuthenticatedConfiguration = {
    try {
      val helper = Configurations.newInstanceFromEnv()
      val hdfsConf = helper.getServiceConfig(ServiceType.HDFS_TYPE)
      if (KerberosAuthenticator.isKerberosEnabled(hdfsConf)) {
        val kerberosProperties = new KerberosProperties
        val loginManager = KrbLoginManagerFactory.getInstance()
          .getKrbLoginManagerInstance(kerberosProperties.kdc, kerberosProperties.realm)
        val res = hdfsConf.asHadoopConfiguration()
        val subject = loginManager.loginWithCredentials(kerberosProperties.user, kerberosProperties.password.toCharArray())
        loginManager.loginInHadoop(subject, res)
        UserAuthenticatedConfiguration(subject, res)
      }
      else UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
    }
    catch {
      case t: Throwable =>
        info(s"Failed to loginUsingHadooputils. Either kerberos is not enabled or invalid setup or " +
          "using System credentials for authentication. Returning default configuration")
        UserAuthenticatedConfiguration(Subject.getSubject(AccessController.getContext()), new Configuration())
    }
  }

  def loginAsAuthenticatedUser(): Unit = {
    try {
      val yarn_authenticated_user = System.getProperty("YARN_AUTHENTICATED_USERNAME")
      val yarn_authenticated_password = System.getProperty("YARN_AUTHENTICATED_PASSWORD")
      import sys.process._
      // Run kinit on a node and key in the password. The user and password are supplied by the environment
      s"echo $yarn_authenticated_password" #| s"kinit $yarn_authenticated_user" !
    }
    catch {
      case t: Throwable => info("Failed to login as Authenticated User. Kerberos not set or invalid credentials")
    }
  }

}

case class UserAuthenticatedConfiguration(subject: Subject, configuration: Configuration)

case class KerberosProperties(kdc: String = KerberosAuthenticator.getPropertyValue(PropertyLocator.KRB_KDC),
                              realm: String = KerberosAuthenticator.getPropertyValue(PropertyLocator.KRB_REALM),
                              user: String = KerberosAuthenticator.getPropertyValue(PropertyLocator.USER),
                              password: String = KerberosAuthenticator.getPropertyValue(PropertyLocator.PASSWORD))
