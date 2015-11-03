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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan.examples

// $COVERAGE-OFF$
// This is utility code only, not part of the main product

import java.util.Date
import org.apache.spark.SparkContext

/**
 * Test if you can connect to Spark and do something trivial.
 *
 * Helpful when troubleshooting if Spark is working correctly.
 */
class SparkSanityTest {

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName + " " + new Date()

    val sc = new SparkContext(ExamplesUtils.sparkMaster, appName)

    val count = sc.parallelize(1 to 100).count()

    println("count: " + count)
    println("Spark is working: " + (count == 100))
  }
}
