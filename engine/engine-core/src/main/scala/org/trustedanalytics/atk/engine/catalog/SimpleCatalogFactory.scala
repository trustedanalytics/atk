package org.trustedanalytics.atk.engine.catalog

import org.trustedanalytics.atk.domain.catalog.Catalog
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.catalog.datacatalog._
import org.trustedanalytics.atk.engine.catalog.framecatalog.FrameCatalog
import org.trustedanalytics.atk.engine.plugin.Invocation

object SimpleCatalogFactory {

  def getCatalogInstance(catalogType: String, engine: Engine)(implicit invocation: Invocation): Catalog = {
    catalogType match {
      case "data" => new AggregatedDataCatalog(engine)
      case "trustedanalytics" => new TrustedAnalyticsDataCatalog
      case "hdfs" => new HdfsDataCatalog
      case "hbase" => new HBaseDataCatalog
      case "hive" => new HiveDataCatalog
      case "frames" => new FrameCatalog(engine)
      case _ => throw new Exception("Unknown catalog")
    }
  }

}