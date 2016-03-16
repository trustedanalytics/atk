package org.trustedanalytics.atk.engine.daal.plugins

import com.intel.daal.algorithms.{ Result, PartialResult }
import com.intel.daal.services.DaalContext
import org.apache.spark.rdd.RDD

trait DistributedAlgorithm[P <: PartialResult, R <: Result] {

  def computePartialResults(): RDD[P]

  def mergePartialResults(daalContext: DaalContext, rdd: RDD[P]): R

}
