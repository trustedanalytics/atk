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
package org.trustedanalytics.atk.plugins.orientdbimport

import org.apache.spark.Partition

/**
 * parallelizes the import from OrientDB so that we read data from a single cluster and class in each partition
 * @param clusterId OrientDB cluster ID
 * @param className OrientDB class name
 * @param idx partition index
 */
case class OrientDbPartition(clusterId: Int, className: String, idx: Int) extends Partition {
  override def index: Int = {
    idx
  }
}
