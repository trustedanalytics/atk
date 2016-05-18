package org.trustedanalytics.atk.plugins.orientdbimport

import org.apache.spark.Partition

case class OrientDbPartition(clusterId: Int, className: String, idx: Int) extends Partition {
  override def index: Int = {
    idx
  }
}
