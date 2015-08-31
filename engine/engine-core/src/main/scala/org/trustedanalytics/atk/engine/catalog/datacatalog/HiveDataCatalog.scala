package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.apache.hadoop.hive.metastore.HiveMetaStore
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory
import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, DataCatalog, GenericCatalogResponse }
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation

object HiveDataCatalog extends DataCatalog {
  override val name: String = "Hive"
  override def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {
    // We do not directly connect to Hive today. This stub is for future implementation
    List(GenericCatalogResponse(name, List(), List()))
  }
}
