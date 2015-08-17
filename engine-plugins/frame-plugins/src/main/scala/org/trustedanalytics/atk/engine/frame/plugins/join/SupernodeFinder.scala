package org.trustedanalytics.atk.engine.frame.plugins.join

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{ MemoryEstimator, ReservoirSampler, SparkRddStatus }

import scala.reflect.ClassTag

case class KeyFrequency(sampleSize: Long = 0, estimatedFrequency: Long = 0, estimatedNumPartitions: Int = 1)

/**
 * Find supernodes in a key-value RDD
 *
 * Supernodes are keys whose total record size exceeds the size of a Spark partition
 *
 * @param rdd Key-value RDD
 */
case class SupernodeFinder[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  /**
   * Find supernodes in key-value RDD
   *
   * @return Map containing supernode keys, and estimated frequency
   */
  def findSuperNodes(): Map[K, KeyFrequency] = {
    val (totalRows, sampledKeys) = ReservoirSampler.sampleAndCountKeys(rdd)
    val keyFrequency = sampledKeys.map {
      case (key, sampleSize) =>
        val estimatedFreq = estimateKeyFrequency(sampleSize, totalRows)
        (key, KeyFrequency(sampleSize, estimatedFreq))
    }
    val averageRowSize = getAverageRowSize(100)
    val numRowsPerPartition = estimateNumRowsPerPartition(averageRowSize)
    keyFrequency.filter { case (key, freq) => freq.estimatedFrequency > numRowsPerPartition }
  }

  def estimateNumRowsPerPartition(averageRowSize: Double): Long = {
    if (averageRowSize > 0) {
      (SparkRddStatus.getExecutorShuffleMemory(rdd) / averageRowSize).toLong
    }
    else {
      0L
    }
  }

  def estimateKeyFrequency(sampleSize: Long, totalRows: Long): Long = {
    if (totalRows <= 0) {
      0L
    }
    else {
      val r = sampleSize.toDouble / totalRows
      val estimatedFrequency = totalRows * (r + 1.96 * Math.sqrt(r * (1 - r) / totalRows))
      estimatedFrequency.toLong
    }
  }

  /** Get average size of the specified number of rows in bytes **/
  private def getAverageRowSize(numRows: Int): Double = {
    val rows = rdd.take(numRows)
    if (rows.isEmpty) return 0d

    val totalSize = rows.map {
      case (k, row) =>
        MemoryEstimator.deepSize(row.asInstanceOf[AnyRef])
    }.sum

    if (rows.size > 0) totalSize / rows.size else 0d
  }

}
