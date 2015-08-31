package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, DataCatalog }
import org.trustedanalytics.atk.engine.{ EngineConfig, Engine }
import org.trustedanalytics.atk.engine.catalog.SimpleCatalogFactory
import org.trustedanalytics.atk.engine.plugin.Invocation

object AggregatedDataCatalog extends DataCatalog {

  override val name: String = "Aggregated Data Catalog"
  override def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {
    val available_data_catalogs = EngineConfig.dataCatalogUri match {
      // Default for ATK
      case "<INVALID DATA_CATALOG_URI>" => List(HdfsDataCatalog, HBaseDataCatalog, HiveDataCatalog)
      // TrustedAnalytics only allows view of data sets available from Data catalog service
      case uri: String => List(TrustedAnalyticsDataCatalog)
      case _ => throw new Exception("Unsupported Data Catalog")
    }
    val catalogs = for {
      ds <- available_data_catalogs
    } yield ds.list(engine)
    catalogs.flatten
  }

}
