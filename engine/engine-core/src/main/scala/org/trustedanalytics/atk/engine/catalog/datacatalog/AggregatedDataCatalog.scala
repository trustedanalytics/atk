package org.trustedanalytics.atk.engine.catalog.datacatalog

import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, DataCatalog }
import org.trustedanalytics.atk.engine.{ EngineConfig, Engine }
import org.trustedanalytics.atk.engine.catalog.SimpleCatalogFactory
import org.trustedanalytics.atk.engine.plugin.Invocation

class AggregatedDataCatalog(engine: Engine)(implicit invocation: Invocation) extends DataCatalog {

  override val name: String = "Aggregated Data Catalog"
  override def list: List[CatalogResponse] = {
    val available_data_sources = EngineConfig.catalogList
    val catalogs = for {
      ds <- available_data_sources
    } yield SimpleCatalogFactory.getCatalogInstance(ds, engine).list
    catalogs.flatten
  }

}
