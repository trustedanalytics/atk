package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.apache.hadoop.hive.metastore.HiveMetaStore
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory
import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, DataCatalog, GenericCatalogResponse }

class HiveDataCatalog() extends DataCatalog {
  override val name: String = "Hive"
  override def list: List[CatalogResponse] = {
    List(GenericCatalogResponse(name, List(), List()))
  }
}
