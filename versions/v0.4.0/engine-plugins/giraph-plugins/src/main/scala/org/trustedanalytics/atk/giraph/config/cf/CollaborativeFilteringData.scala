/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.giraph.config.cf

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference

/**
 *
 * @param userFrameReference user frame name
 * @param itemFrameReference item frame name
 * @param userColumnName user column name
 * @param itemColumnName item column name
 * @param factorsColumnName factors column name
 * @param numFactors dimensions for factor vector
 */
case class CollaborativeFilteringData(userFrameReference: FrameReference,
                                      itemFrameReference: FrameReference,
                                      userColumnName: String,
                                      itemColumnName: String,
                                      factorsColumnName: String,
                                      numFactors: Int) {
  require(null != userFrameReference, "user frame reference is required")
  require(null != itemFrameReference, "item frame reference is required")
  require(StringUtils.isNotBlank(userColumnName), "user column name is required")
  require(StringUtils.isNotBlank(itemColumnName), "item column name is required")
  require(StringUtils.isNotBlank(factorsColumnName), "factors column name is required")
  require(numFactors > 0, "factors dimension must be greater than 0")
}
