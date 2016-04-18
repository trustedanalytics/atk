package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphFactory, OrientGraph }

/**
 * Created by wtaie on 4/1/16.
 */

object GraphDbFactory {

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
