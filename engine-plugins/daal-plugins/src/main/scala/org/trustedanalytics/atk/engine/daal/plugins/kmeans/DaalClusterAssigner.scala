package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import com.intel.daal.algorithms.kmeans.{ ResultId, InputId, Method, DistributedStep1Local }
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

case class DaalClusterAssigner(featureTable: DistributedNumericTable,
                               inputCentroids: IndexedNumericTable,
                               labelColumn: String) {

  /**
   * Get table with cluster assignments
   *
   * @return Frame of cluster assignments
   */
  def assign(): FrameRdd = {

    val schema = FrameSchema(List(Column(labelColumn, DataTypes.int32)))
    var numRows = 0L
    val rdd = featureTable.rdd.map { table =>
      val context = new DaalContext
      val local = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, inputCentroids.numRows)
      local.input.set(InputId.data, table.getUnpackedTable(context))
      local.input.set(InputId.inputCentroids, inputCentroids.getUnpackedTable(context))
      local.parameter.setAssignFlag(true)
      val partialResults = local.compute
      partialResults.pack()

      val result = local.finalizeCompute()
      val assignmentTable = result.get(ResultId.assignments).asInstanceOf[HomogenNumericTable]
      val assignments = IndexedNumericTable(table.index, assignmentTable)
      numRows += assignments.numRows
      context.dispose()
      assignments
    }

    DistributedNumericTable(rdd, numRows).toFrameRdd(schema)
  }

  /**
   * Compute size of predicted clusters
   *
   * @param assignmentFrame Frame with cluster assignments
   * @return Map of cluster names and sizes
   */
  def cluster_sizes(assignmentFrame: FrameRdd): Map[String, Long] = {
    assignmentFrame.mapRows(row => {
      val clusterId = row.intValue(labelColumn) + 1
      ("Cluster:" + clusterId.toString, 1L)
    }).reduceByKey(_ + _).collect().toMap
  }
}
