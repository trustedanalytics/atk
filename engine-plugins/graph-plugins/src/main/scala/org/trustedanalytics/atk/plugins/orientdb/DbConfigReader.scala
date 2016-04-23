package org.trustedanalytics.atk.plugins.orientdb

import com.typesafe.config.ConfigFactory

/**
 * reads the database configurations from a file
 */
object DbConfigReader extends Serializable {

  /**
   * A method to extract the configurations from a file
   * @param dbName database name
   * @return database configurations
   */
  def extractConfigurations(dbName: String): DbConfigurations = {

    val userName = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.username")
    val password = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.password")
    val dbDir = ConfigFactory.load().getString("trustedanalytics.atk.datastore.connection-orientdb.url")
    val dbUri = dbDir + "/" + dbName
    DbConfigurations(dbUri, userName, password)
  }
}
