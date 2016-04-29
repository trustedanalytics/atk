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

import com.orientechnologies.orient.client.remote.OServerAdmin
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraph }
import scala.util.{ Failure, Success, Try }

/**
 * OrientDB graph factory
 */

object GraphDbFactory {

  /**
   * Method to create/connect to OrientDB graph
   *
   * @param dbConfigurations OrientDB configurations
   * @return a transactional OrientDB graph database instance
   */
  def graphDbConnector(dbConfigurations: DbConfigurations): OrientGraph = {
    val orientDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbConfigurations.dbUri)
    val orientGraphDb = if (dbConfigurations.dbUri.startsWith("remote:") && !new OServerAdmin(dbConfigurations.dbUri).connect("root", dbConfigurations.rootPassword).existsDatabase()) {
      new OServerAdmin(dbConfigurations.dbUri).connect("root", dbConfigurations.rootPassword).createDatabase("graph", "plocal")
      openGraphDb(orientDb, dbConfigurations)
    }
    else if (dbConfigurations.dbUri.startsWith("remote:") && new OServerAdmin(dbConfigurations.dbUri).connect("root", dbConfigurations.rootPassword).existsDatabase()) {
      new OServerAdmin(dbConfigurations.dbUri).connect("root", dbConfigurations.rootPassword)
      openGraphDb(orientDb, dbConfigurations)
    }
    else if (!orientDb.exists()) {
      createGraphDb(dbConfigurations)
    }
    else {
      openGraphDb(orientDb, dbConfigurations)
    }
    orientGraphDb
  }

  /**
   * Method for creating Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a transactional Orient graph database instance
   */
  def createGraphDb(dbConfigurations: DbConfigurations): OrientGraph = {

    val graph = Try {
      val factory = new OrientGraphFactory(dbConfigurations.dbUri, dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.getDatabase.getMetadata.getSecurity.authenticate(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.getTx
    } match {
      case Success(orientGraph) => orientGraph
      case Failure(ex) => throw new RuntimeException(s"Unable to create database: ${dbConfigurations.dbUri}", ex)
    }
    graph
  }

  /**
   * Method for opening Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a transactional Orient graph database instance
   */
  private def openGraphDb(orientDb: ODatabaseDocumentTx, dbConfigurations: DbConfigurations): OrientGraph = {
    Try {
      val db: ODatabaseDocumentTx = orientDb.open(dbConfigurations.dbUserName, dbConfigurations.dbUserName)
      db
    } match {
      case Success(db) => new OrientGraph(db)
      case Failure(ex) => throw new scala.RuntimeException(s"Unable to open database: ${dbConfigurations.dbUri}", ex)
    }
  }

}
