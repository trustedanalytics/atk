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

package org.trustedanalytics.atk.domain.graph.construction

/**
 * TrustedAnalytics V1 API Graph Loader rule for creating property graph values from tabular data.
 * @param source Is the value constant or varying?
 * @param value If a constant, the value taken. If a varying, the name of the column from which the data is parsed.
 */
case class ValueRule(source: String, value: String) {
  require(source.equals(GBValueSourcing.CONSTANT) || source.equals(GBValueSourcing.VARYING),
    s"source must be one of (${GBValueSourcing.CONSTANT}, ${GBValueSourcing.VARYING})")
  require(value != null, "value must not be null")
}
