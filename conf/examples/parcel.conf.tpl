trustedanalytics.atk.metastore.connection-postgresql.url = "jdbc:postgresql://"${trustedanalytics.atk.metastore.connection-postgresql.host}":"${trustedanalytics.atk.metastore.connection-postgresql.port}"/"${trustedanalytics.atk.metastore.connection-postgresql.database}
trustedanalytics.atk.metastore.connection = ${trustedanalytics.atk.metastore.connection-postgresql}
trustedanalytics.atk.engine.titan.query {
  storage {
    # query does use the batch load settings in titan.load
    backend = ${trustedanalytics.atk.engine.titan.load.storage.backend}
    hostname = ${trustedanalytics.atk.engine.titan.load.storage.hostname}
    port = ${trustedanalytics.atk.engine.titan.load.storage.port}
  }
}