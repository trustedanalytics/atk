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
package org.trustedanalytics.atk.plugins.orientdb

import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * the database parsed configurations
 */

case class DbConfigurations(@ArgDoc("""OrientDB database URI.""") dbUri: String,
                            @ArgDoc("""User name.""") dbUserName: String,
                            @ArgDoc("""User password.""") dbPassword: String,
                            @ArgDoc("""Port number.""") portNumber: String,
                            @ArgDoc("""The database host.""") dbHost: String) extends Serializable {

  require(dbUri != null, "database URI is required")
  require(dbUserName != null, "User name is required")
  require(dbUserName != "invalid-orientdb-user", """User name is "invalid-orientdb-user", please update the configurations file """)
  require(dbPassword != null, "Password is required")
  require(dbPassword != "invalid-orientdb-password", """Password is "invalid-orientdb-password, please update the configurations file"""")
  require(portNumber != null, "the port number is required")
  require(portNumber != "invalid-orientdb-port", """the port number is "invalid-orientdb-port", please update the configurations file""")
  require(dbHost != null, "the host name is required")
  require(dbHost != "invalid-orientdb-host", """the host name is "invalid-orientdb-host", please update the configurations file""")

}
