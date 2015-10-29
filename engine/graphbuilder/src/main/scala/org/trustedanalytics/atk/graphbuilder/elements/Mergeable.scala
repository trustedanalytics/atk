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


package org.trustedanalytics.atk.graphbuilder.elements

/**
 * Mergeable items can be combined into a new single item.
 * <p>
 * In the case of Graph Elements, merging means creating a new Graph Element that has a combined set of properties.
 * </p>
 */
trait Mergeable[T] {

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  def id: Any

  /**
   * Merge two items into one.
   *
   * @param other item to merge
   * @return the new merged item
   */
  def merge(other: T): T

}
