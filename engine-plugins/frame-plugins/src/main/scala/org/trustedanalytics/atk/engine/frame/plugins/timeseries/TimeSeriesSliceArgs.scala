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

case class TimeSeriesSliceArgs(
    @ArgDoc("Frame formatted as a time series (must have a string key column and a vector column that contains the values that correspond to the specified DateTimeIndex.") frame: FrameReference,
    @ArgDoc("DateTimeIndex to conform all series to.") dateTimeIndex: List[DateTime],
    @ArgDoc("The start date for the slice in the ISO 8601 format, like: yyyy-MM-dd'T'HH:mm:ss.SSSZ ") start: DateTime,
    @ArgDoc("The end date for the slice (inclusive) in the ISO 8601 format, like: yyyy-MM-dd'T'HH:mm:ss.SSSZ.") end: DateTime) {
  require(frame != null, "frame is required")
  require(dateTimeIndex != null, "date/time index is required")
  require(start != null, "start date is required")
  require(end != null, "end date is required")
  require(start.compareTo(end) < 0, "start date must be less than end date")
}
