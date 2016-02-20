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
package org.trustedanalytics.atk.engine.util

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.EngineConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import scala.reflect.io.Directory
import scala.util.control.NonFatal

/**
 * Static methods for accessing a Kerberos secured hadoop cluster.
 */
object KerberosAuthenticator extends EventLogging with EventLoggingImplicits with ClassLoaderAware {
  // TODO: Allow support for multiple keytabs once namespaces is implemented

  /**
   * Login to Kerberos cluster using a keytab and principal name specified in config files
   */
  def loginWithKeyTab(): Unit = {
    if (EngineConfig.enableKerberos) {
      debug("Listing Files under Current Directory")
      Directory.Current.get.deepFiles.foreach(f => debug(f.name))
      //if kerberos is enabled the following configs will have been set.
      val keyTabPrincipal: String = EngineConfig.kerberosPrincipalName.get
      val keyTabFile: String = EngineConfig.kerberosKeyTabPath.get
      info(s"Authenticate with Kerberos\n\tPrincipal: $keyTabPrincipal\n\tKeyTab File: $keyTabFile")
      UserGroupInformation.loginUserFromKeytab(
        keyTabPrincipal,
        keyTabFile)
    }
  }

  /**
   * Login to Kerberos cluster using a keytab and principal name specified in config files
   * using a specific HadoopConfiguration
   * @param configuration HadoopConfiguration
   * @return UserGroupInformation for Kerberos TGT ticket
   */
  def loginConfigurationWithKeyTab(configuration: Configuration): Unit = withMyClassLoader {
    if (EngineConfig.enableKerberos) {
      UserGroupInformation.setConfiguration(configuration)
      KerberosAuthenticator.loginWithKeyTab()
    }
  }

  /**
   * Login to Kerberos using a keytab and principal name specified in config files via kinit command
   */
  def loginWithKeyTabCLI(): Unit = {
    //Note this method logs executes kinit for the user running ATK Rest Server. This user must be able to get a valid TGT.
    if (EngineConfig.enableKerberos) {
      try {
        info("Authenticate to Kerberos using kinit")
        val kerberosConfig = s"env KRB5_CONFIG=${sys.env.get("KRB5_CONFIG").getOrElse("")}"
        val command = s"$kerberosConfig kinit ${EngineConfig.kerberosPrincipalName.get} -k -t ${EngineConfig.kerberosKeyTabPath.get}"
        info(s"Command: $command")
        val p = Runtime.getRuntime.exec(command)
        val exitValue = p.waitFor()
        info(s"kinit exited with Exit Value: $exitValue")
        if (exitValue == 1) {
          warn(s"Problem executing kinit. May not have valid TGT.")
        }
      }
      catch {
        case NonFatal(e) => error("Error executing kinit.", exception = e)
      }
    }
  }

  def getKerberosConfigJVMParam: Option[String] = sys.env.get("JAVA_KRB_CONF")

}
