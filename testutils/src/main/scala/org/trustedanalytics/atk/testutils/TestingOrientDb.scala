package org.trustedanalytics.atk.testutils

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientGraph

/**
 * Created by wtaie on 3/31/16.
 */
trait TestingOrientDb {

  var orientGraph: OrientGraph = null
  var orientDb: OrientGraph = null
  /**
   * create in memory Orient graph database
   */
  def setupOrientDbInMemory(): Unit = {
    val uuid = java.util.UUID.randomUUID.toString
    orientGraph = new OrientGraph("memory:OrientTestDb" + uuid)
  }

  /**
   *
   */
  def setupOrientDb(): Unit = {

    val orientDocDb: ODatabaseDocumentTx = new ODatabaseDocumentTx("plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbTest")
    if (!orientDocDb.exists()) {
      orientDocDb.create()
    }
    else {
      System.out.println("the database already exists and now open")
      orientDocDb.open("admin", "admin")
    }
    val orientDb = new OrientGraph(orientDocDb)
  }

  /**
   * commit the transcation and close/drop the ograph, for plocal and remote database creation modes
   */
  def cleanupOrientDb(): Unit = {
    try {
      if (orientDb != null) {
        orientDb.commit()
        orientDb.drop()
      }
    }
    finally {
      val orientDocDb: ODatabaseDocumentTx = new ODatabaseDocumentTx("plocal:/home/wtaie/graphDBs_home/orientdb-community-2.1.12/databases/OrientDbTest")
      System.out.println("the database already exists and now open")
      orientDocDb.open("admin", "admin")
      orientDocDb.drop()
    }
  }

  /**
   * commit the transcation and close the ograph
   */
  def cleanupOrientDbInMemory(): Unit = {
    try {
      if (orientGraph != null) {
        orientGraph.commit()
      }
    }
    finally {
      orientGraph.shutdown()
    }
  }
}
