/**
 *  Copyright (c) 2016 Intel Corporation 
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

import org.joda.time.DateTime
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class TimeSeriesFromObservationsArgs(
    @ArgDoc("Frame of observations to format as a time series") frame: FrameReference,
    @ArgDoc("DateTimeIndex to conform all series to.") dateTimeIndex: List[DateTime],
    @ArgDoc("The name of the column telling when the observation occurred.") timestampColumn: String,
    @ArgDoc("The name of the column that contains which string key the observation belongs to.") keyColumn: String,
    @ArgDoc("The name of the column that contains the observed value.") valueColumn: String) {
  require(frame != null, "frame is required")
  require(dateTimeIndex != null, "date/time index is required")
  require(timestampColumn != null && timestampColumn.nonEmpty, "timestamp column is required")
  require(keyColumn != null && keyColumn.nonEmpty, "key column is required")
  require(valueColumn != null && valueColumn.nonEmpty, "value column is required")
}
