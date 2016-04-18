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

package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.engine.util.KerberosAuthenticator
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware

/**
 * Create HBaseAdmin instances
 *
 * HBaseAdmin should not be re-used forever: you should create, use, throw away - and then get another one next time
 */
class HBaseAdminFactory extends ClassLoaderAware {

  /**
   * HBaseAdmin should not be re-used forever: you should create, use, throw away - and then get another one next time
   */
  def createHBaseAdmin(): HBaseAdmin = withMyClassLoader {
    val config = HBaseConfiguration.addHbaseResources(new Configuration())

    // for some reason HBaseConfiguration wasn't picking up hbase-default.xml automatically, so manually adding here
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-default.xml"))
    config.addResource(getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))

    // Skip check for default hbase version which causes intermittent errors "|hbase-default.xml file seems to be for and old version of HBase (null), this version is 0.98.1-cdh5.1.2|"
    // This error shows up despite setting the correct classpath in bin/rest-server.sh and packaging the correct cdh hbase jars
    config.setBoolean("hbase.defaults.for.version.skip", true)
    KerberosAuthenticator.loginConfigurationWithClassLoader()

    new HBaseAdmin(config)
  }

}
