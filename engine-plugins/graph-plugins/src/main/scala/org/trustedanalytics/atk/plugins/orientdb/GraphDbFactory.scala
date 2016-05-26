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
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientGraphFactory }
import org.trustedanalytics.atk.event.EventLogging
import scala.util.{ Failure, Success, Try }

/**
 * OrientDB graph factory
 */

object GraphDbFactory extends EventLogging {

  val rootUserName = "root"

  /**
   * Method to create/connect to OrientDB graph
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional OrientDB graph database instance
   */
  def graphDbConnector(dbConfigurations: DbConfiguration): OrientGraphNoTx = {
    val orientDb: ODatabaseDocumentTx = new ODatabaseDocumentTx(dbConfigurations.dbUri)
    val orientGraphDb = if (dbConfigurations.dbUri.startsWith("remote:")) {
      if (!new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword).existsDatabase()) {
        new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword).createDatabase("graph", "plocal")
        openGraphDb(orientDb, dbConfigurations)
      }
      else {
        new OServerAdmin(dbConfigurations.dbUri).connect(rootUserName, dbConfigurations.rootPassword)
        openGraphDb(orientDb, dbConfigurations)
      }
    }
    else if (!orientDb.exists()) {
      createGraphDb(dbConfigurations)
    }
    else {
      openGraphDb(orientDb, dbConfigurations)
    }
    orientGraphDb.declareIntent(new OIntentMassiveInsert())
    orientGraphDb
  }

  /**
   * Method for creating Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional Orient graph database instance
   */
  def createGraphDb(dbConfigurations: DbConfiguration): OrientGraphNoTx = {

    val graph = Try {
      val factory = new OrientGraphFactory(dbConfigurations.dbUri, dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.declareIntent(new OIntentMassiveInsert())
      factory.getDatabase.getMetadata.getSecurity.authenticate(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      factory.getNoTx
    } match {
      case Success(orientGraph) => orientGraph
      case Failure(ex) =>
        error(s"Unable to create database", exception = ex)
        throw new RuntimeException(s"Unable to create database: ${dbConfigurations.dbUri}, ${ex.getMessage}")
    }
    graph
  }

  /**
   * Method for opening Orient graph database
   *
   * @param dbConfigurations OrientDB configurations
   * @return a non transactional Orient graph database instance
   */
  private def openGraphDb(orientDb: ODatabaseDocumentTx, dbConfigurations: DbConfiguration): OrientGraphNoTx = {
    Try {
      val db: ODatabaseDocumentTx = orientDb.open(dbConfigurations.dbUserName, dbConfigurations.dbPassword)
      db
    } match {
      case Success(db) => new OrientGraphNoTx(db)
      case Failure(ex) =>
        println("stack" + ex.printStackTrace())
        error(s"Unable to open database", exception = ex)
        throw new scala.RuntimeException(s"Unable to open database: ${dbConfigurations.dbUri}, ${ex.getMessage}")
    }
  }

}
