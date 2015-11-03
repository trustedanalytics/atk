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

package org.trustedanalytics.atk.plugins.communitydetection

import scala.collection.JavaConversions._

/**
 * Scala collection to java collection converter with serialization
 *
 * This converter has been added to work with serialization. The serialization issue with
 * the scala.collection.JavaConvertions/JavaConverters is that these converters are wrappers that use
 * the underlying (scala/java) object. For it to be effectively serializable,
 * they must have a warranty that the underlying structure is serializable.
 *
 * One of the easiest way to work this out is to implement a structural copy of the conversion method
 */
object ScalaToJavaCollectionConverter extends Serializable {

  /**
   * convert the scala.collection.Set[Long] to java.util.Set[Long]
   * @param scalaSet a scala set of Long
   * @return java.util.Set of Long
   */
  def convertSet(scalaSet: Set[Long]): java.util.Set[Long] = {
    val javaSet = new java.util.HashSet[Long]()
    scalaSet.foreach(entry => javaSet.add(entry))
    javaSet
  }

}
