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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for calculating summary statistics for a dataframe column.
 */
case class ColumnSummaryStatisticsArgs(@ArgDoc("""Identifier for the input
dataframe.""") frame: FrameReference,
                                       @ArgDoc("""The column to be statistically summarized.
Must contain numerical data; all NaNs and infinite values are excluded
from the calculation.""") dataColumn: String,
                                       @ArgDoc("""Name of column holding weights of
column values.""") weightsColumn: Option[String],
                                       @ArgDoc("""If true, the variance is calculated
as the population variance.
If false, the variance calculated as the sample variance.
Because this option affects the variance, it affects the standard
deviation and the confidence intervals as well.
Default is false.""") usePopulationVariance: Option[Boolean]) {

  require(frame != null, "frame is required but not provided")
  require(dataColumn != null, "data column is required but not provided")
}

/**
 * Summary statistics for a dataframe column. All values are optionally weighted by a second column. If no weights are
 * provided, all elements receive a uniform weight of 1. If any element receives a weight <= 0, that element is thrown
 * out of the calculation. If a row contains a NaN or infinite value in either the data or weights column, that row is
 * skipped and a count of bad rows is incremented.
 *
 *
 * @param mean Arithmetic mean of the data.
 * @param geometricMean Geometric mean of the data. None when there is a non-positive data element, 1 if there are no
 *                       data elements.
 * @param variance If sample variance is used,  the variance  is the weighted sum of squared distances from the mean is
 *                 divided by the sum of weights minus 1 (NaN if the sum of the weights is <= 1).
 *                 If population variance is used, the variance is the weighted sum of squared distances from the mean
 *                 divided by the sum of weights.
 * @param standardDeviation Square root of the variance. None when sample variance is used and the sum of the weights
 *                          is <= 1.
 * @param totalWeight The sum of all weights over valid input rows. (Ie. neither data nor weight is NaN, or infinity,
 *                    and weight is > 0).
 * @param minimum Minimum value in the data. None when there are no data elements of positive weight.
 * @param maximum Maximum value in the data. None when there are no data elements of positive weight.
 * @param meanConfidenceLower: Lower limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             None when there are no elements of positive weight.
 * @param meanConfidenceUpper: Upper limit of the 95% confidence interval about the mean. Assumes a Gaussian RV.
 *                             None when there are no elements of positive weight.
 * @param badRowCount The number of rows containing a NaN or infinite value in either the data or weights column.
 * @param goodRowCount The number of rows containing a NaN or infinite value in either the data or weight
 * @param positiveWeightCount  The number valid data elements with weights > 0.
 *                             This is the number of entries used for the calculation of the statistics.
 * @param nonPositiveWeightCount The number valid data elements with weight <= 0.
 */
case class ColumnSummaryStatisticsReturn(mean: Double,
                                         geometricMean: Double,
                                         variance: Double,
                                         standardDeviation: Double,
                                         totalWeight: Double,
                                         minimum: Double,
                                         maximum: Double,
                                         meanConfidenceLower: Double,
                                         meanConfidenceUpper: Double,
                                         badRowCount: Long,
                                         goodRowCount: Long,
                                         positiveWeightCount: Long,
                                         nonPositiveWeightCount: Long)
