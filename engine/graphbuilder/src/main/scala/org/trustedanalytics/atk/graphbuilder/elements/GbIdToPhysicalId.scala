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
 * This wrapper maps a gbId to a Physical ID
 * <p>
 * This is to get around the fact that Titan won't allow you to specify physical ids.
 * It might be useful for other vendors too though.
 * </p>
 *
 * @param gbId the unique property used by Graph Builder
 * @param physicalId the ID used by the underlying graph storage
 */
case class GbIdToPhysicalId(gbId: Property, physicalId: AnyRef) {

  def toTuple: (Property, AnyRef) = {
    (gbId, physicalId)
  }
}
