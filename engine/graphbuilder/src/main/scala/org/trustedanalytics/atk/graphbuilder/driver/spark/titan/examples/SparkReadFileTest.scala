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
// This is example code only, not part of the main product

import java.util.Date

import org.apache.spark.SparkContext

/**
 * This utility is for testing that Spark can read a file from HDFS.
 *
 * Helpful when troubleshooting if Spark is working correctly.
 */
object SparkReadFileTest {

  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getSimpleName + " " + new Date()

    val sc = new SparkContext(ExamplesUtils.sparkMaster, appName)

    println("Trying to read file: " + ExamplesUtils.movieDataset)

    val inputRows = sc.textFile(ExamplesUtils.movieDataset, System.getProperty("PARTITIONS", "120").toInt)
    val inputRdd = inputRows.map(row => row.split(","): Seq[_])

    println("Spark Read Input Rows: " + inputRdd.count())
  }
}
