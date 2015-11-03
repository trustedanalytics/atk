/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.engine.frame.plugins.assignsample

import org.trustedanalytics.atk.engine.frame.plugins.MLDataSplitter
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFunSuite

import scala.util.Random

class MLDataSplitterSuite extends TestingSparkContextFunSuite with Matchers {

  // test if we can randomly split a RDD according to a percentage distribution
  test("MLDataSplitter") {

    val nPoints = 10000
    val percentages = Array(0.7, 0.1, 0.2)
    val labels = Array("bin #1", "bin #2", "bin #3")

    // generate testRDD
    val rnd = new Random(41)
    val testData = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val testRDD = sparkContext.parallelize(testData, 2)

    // test the size of generated RDD
    val nTotal = testRDD.count()
    assert(nTotal == nPoints, "# data points generated isn't equal to specified.")

    // split the RDD by labelling
    val splitter = new MLDataSplitter(percentages, labels, 42)
    val labeledRDD = splitter.randomlyLabelRDD(testRDD)

    // collect the size of each partition
    val partitionSizes = new Array[Long](percentages.size)
    (0 until percentages.size).foreach { i =>
      val partitionRDD = labeledRDD.filter(p => p.label == labels.apply(i)).map(_.entry)
      partitionSizes(i) = partitionRDD.count()
    }

    // test the total #samples
    val nTotalSamples = partitionSizes.sum
    assert(nTotalSamples == nPoints, "# data points sampled isn't equal to specified.")

    // check if partition percentages are expected
    (0 until percentages.size).foreach { i =>
      val realPercentage = partitionSizes(i).toDouble / nTotalSamples
      assert(Math.abs(realPercentage - percentages(i)) < 0.05,
        "partition percentage isn't in [%f, %f].".format(percentages(i) - 0.05, percentages(i) + 0.05))
    }
  }

}
