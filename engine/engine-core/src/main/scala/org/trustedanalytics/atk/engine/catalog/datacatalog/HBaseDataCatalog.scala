package org.trustedanalytics.atk.engine.catalog.datacatalog
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HBaseConfiguration

import org.trustedanalytics.atk.domain.catalog.{ CatalogResponse, GenericCatalogResponse, DataCatalog }
import org.trustedanalytics.atk.engine.Engine
import org.trustedanalytics.atk.engine.plugin.Invocation

object HBaseDataCatalog extends DataCatalog {
  override val name: String = "HBase"
  override def list(engine: Engine)(implicit invocation: Invocation): List[CatalogResponse] = {

    val config = HBaseConfiguration.create()
    val hbadmin = ConnectionFactory.createConnection(config).getAdmin
    val metadata = List("tablename", "is_enabled", "column_families")
    val tables = hbadmin.listTables()
    val data = for {
      table <- tables
      tablename = table.getTableName
      tablenameAsString = tablename.getNameAsString
      isEnabled = hbadmin.isTableEnabled(tablename).toString
      columnFamilies = table.getColumnFamilies.map(cf => cf.getNameAsString).mkString(",")
    } yield List(tablenameAsString, isEnabled, columnFamilies)
    hbadmin.close()
    List(GenericCatalogResponse(name, metadata, data.toList))
  }
}
