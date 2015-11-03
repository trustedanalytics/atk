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


package org.trustedanalytics.atk.graphbuilder.graph.titan

import com.thinkaurelius.titan.hadoop.formats.titan_054.hbase.CachedTitanHBaseRecordReader
import org.apache.spark.scheduler.{ SparkListener, SparkListenerApplicationEnd }

/**
 * Ensures clean shut down by invalidating all entries in the Titan graph cache
 * when the spark application shuts down.
 */
class TitanGraphCacheListener() extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Invalidating Titan graph cache:")
    TitanGraphConnector.invalidateGraphCache()
  }
}

/**
 * Ensures clean shut down by invalidating all entries in the Titan/Hadoop HBase graph cache
 * when the spark application shuts down.
 */
class TitanHadoopHBaseCacheListener() extends SparkListener {

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Invalidating Titan/Hadoop HBase graph cache:")
    CachedTitanHBaseRecordReader.invalidateGraphCache()
  }
}
