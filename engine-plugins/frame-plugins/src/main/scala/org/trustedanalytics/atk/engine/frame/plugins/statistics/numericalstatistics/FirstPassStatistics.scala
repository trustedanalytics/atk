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


package org.trustedanalytics.atk.engine.frame.plugins.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.frame.plugins.statistics.NumericValidationUtils

/**
 * Contains all statistics that are computed in a single pass over the data. All statistics are in their weighted form.
 *
 * Floating point values that are running combinations over all of the data are represented as BigDecimal, whereas
 * minimum, mode and maximum are Doubles since they are simply single data points.
 *
 * Data values that are NaNs or infinite or whose weights are Nans or infinite or <=0 are skipped and logged.
 *
 * @param mean The weighted mean of the data.
 * @param weightedSumOfSquares Weighted mean of the data values squared.
 * @param weightedSumOfSquaredDistancesFromMean Weighted sum of squared distances from the weighted mean.
 * @param weightedSumOfLogs Weighted sum of logarithms of the data.
 * @param minimum The minimum data value of finite weight > 0.
 * @param maximum The minimum data value of finite weight > 0.
 * @param totalWeight The total weight in the column, excepting data pairs whose data is not a finite number, or whose
 *                    weight is either not a finite number or <= 0.
 * @param positiveWeightCount Number of entries whose weight is a finite number > 0.
 * @param nonPositiveWeightCount Number of entries whose weight is a finite number <= 0.
 * @param badRowCount The number of entries that contain a data value or a weight that is a not a finite number.
 * @param goodRowCount The number of entries that whose data value and weight are both finite numbers.
 */
private[numericalstatistics] case class FirstPassStatistics(mean: BigDecimal,
                                                            weightedSumOfSquares: BigDecimal,
                                                            weightedSumOfSquaredDistancesFromMean: BigDecimal,
                                                            weightedSumOfLogs: Option[BigDecimal],
                                                            minimum: Double,
                                                            maximum: Double,
                                                            totalWeight: BigDecimal,
                                                            positiveWeightCount: Long,
                                                            nonPositiveWeightCount: Long,
                                                            badRowCount: Long,
                                                            goodRowCount: Long)
