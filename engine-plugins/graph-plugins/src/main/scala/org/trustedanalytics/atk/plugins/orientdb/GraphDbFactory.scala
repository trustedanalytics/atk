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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraph }

/**
 * Created by wtaie on 4/1/16.
 */

class GraphDbFactory {
  /**
   * Method to create/connect to Orient database
   * @param dbUri the database with default username; "admin" and password : "admin".
   * @return a transcational Orient graph database instance
   */

  def GraphDbConnector(dbUri: String): OrientGraph = {

    val orientDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbUri)
    if (!orientDb.exists()) {
      orientDb.create()
    }
    else {
      System.out.println("the database already exists and now open")
      orientDb.open("admin", "admin")
    }
    val orientGraphDb = new OrientGraph(orientDb)
    orientGraphDb
  }

  /**
   * Method for creating Orient graph database
   * @param dbUri the database URL
   * @param userName database username
   * @param password the database password
   * @return a transcational Orient graph database instance
   */
  def createGraphDb(dbUri: String, userName: String, password: String): OrientGraph = {

    val factory: OrientGraphFactory = new OrientGraphFactory(dbUri, userName, password)
    var graph: OrientGraph = factory.getTx
    graph
  }

}
