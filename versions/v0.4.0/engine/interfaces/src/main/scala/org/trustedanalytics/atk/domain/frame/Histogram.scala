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

/**
 * a dictionary containing the results of the HistogramPlugin
 * @param cutoffs a list containing the edges of each bin
 * @param hist a list containing count of the weighted observations found in each bin
 * @param density a list containing a decimal containing the percentage of observations found in the total set per bin
 */
case class Histogram(cutoffs: Seq[Double], hist: Seq[Double], density: Seq[Double])
