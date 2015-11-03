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

package org.trustedanalytics.atk.domain.frame

import spray.json.JsValue

/**
 * Command for calculating statistics for a (possibly weighted) dataframe column.
 * @param frame Identifier for the input dataframe.
 * @param dataColumn Name of the column to statistically summarize. Must contain numerical data.
 * @param weightsColumn Optional. Name of the column that provides weights (frequencies).
 */
case class ColumnFullStatisticsArgs(frame: FrameReference, dataColumn: String, weightsColumn: Option[String]) {

  require(frame != null, "frame is required but not provided")
  require(dataColumn != null, "data column is required but not provided")
}

/**
 * Statistics for a dataframe column. All values are optionally weighted by a second column. If no weights are
 * provided, all elements receive a uniform weight of 1. If any element receives a weight <= 0, that element is thrown
 * out of the calculation. If a row contains a NaN or infinite value in either the data or weights column, that row is
 * skipped and a count of bad rows is incremented.
 *
 * @param mean Arithmetic mean of the data.
 * @param geometricMean Geometric mean of the data. NaN when there is a non-positive data element, 1 if there are no
 *                      data elements.
 * @param variance Variance of the data where weighted sum of squared distance from the mean is divided by the number of
 *                 data elements minus 1. NaN when the number of data elements is < 2.
 * @param standardDeviation Standard deviation of the data. NaN when the number of data elements is < 2.
 * @param skewness The skewness of the data. NaN when the number of data elements is < 3
 *                 or if the distribution is uniform.
 * @param kurtosis The kurtosis of the data. NaN when the number of data elements is < 4
 *                 or if the distribution is uniform.
 * @param totalWeight Sum of all weights that are finite numbers > 0.
 * @param minimum Minimum value in the data. None when there are no data elements of positive
 *                weight.
 * @param maximum Maximum value in the data. None when there are no data elements of positive
 *                weight.
 * @param meanConfidenceLower: Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                           NaN when there are <= 1 data elements of positive weight.
 * @param meanConfidenceUpper: Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                           NaN when there are <= 1 data elements of positive weight.
 * @param positiveWeightCount The number valid data elements with weights > 0.
 *                            This is the number of entries used for the calculation of the statistics.
 * @param nonPositiveWeightCount The number valid data elements with weight <= 0.
 * @param badRowCount The number of rows containing a NaN or infinite value in either the data or weights column.
 * @param goodRowCount The number of rows not containing a NaN or infinite value in either the data or weights
 *                                 column.
 */
case class ColumnFullStatisticsReturn(mean: Double,
                                      geometricMean: Double,
                                      variance: Double,
                                      standardDeviation: Double,
                                      skewness: Double,
                                      kurtosis: Double,
                                      totalWeight: Double,
                                      minimum: Double,
                                      maximum: Double,
                                      meanConfidenceLower: Double,
                                      meanConfidenceUpper: Double,
                                      positiveWeightCount: Long,
                                      nonPositiveWeightCount: Long,
                                      badRowCount: Long,
                                      goodRowCount: Long)
