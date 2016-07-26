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

package org.trustedanalytics.atk.engine.frame.plugins.timeseries

import org.apache.commons.lang.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class TimeSeriesAugmentedDickeyFullerTestArgs(@ArgDoc("Frame of time series data") frame: FrameReference,
                                                   @ArgDoc("Name of the column that contains the time series values to use with the ADF test. ") tsColumn: String,
                                                   @ArgDoc("The lag order to calculate the test statistic. ") maxLag: Int,
                                                   @ArgDoc("The method of regression that was used. Following MacKinnon's " +
                                                     "notation, this can be \"c\" for constant, \"nc\" for no constant, " +
                                                     "\"ct\" for constant and trend, and \"ctt\" for constant, trend, and " +
                                                     "trend-squared. ") regression: Option[String]) {
  require(frame != null, "frame is required")
  require(StringUtils.isNotEmpty(tsColumn), "ts_column must not be null or empty.")
}
