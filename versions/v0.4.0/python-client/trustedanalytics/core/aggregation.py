#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
trustedanalytics frame aggregation functions
"""
import json

from trustedanalytics.core.atktypes import valid_data_types

class AggregationFunctions(object):

    """
    Defines supported aggregation functions, maps them to keyword strings
    """
    avg = 'AVG'
    count = 'COUNT'
    count_distinct = 'COUNT_DISTINCT'
    max = 'MAX'
    min = 'MIN'
    sum = 'SUM'
    var = 'VAR'
    stdev = 'STDEV'

    def histogram(self, cutoffs, include_lowest=True, strict_binning=False):
        return repr(GroupByHistogram(cutoffs, include_lowest, strict_binning))

    def __repr__(self):
        return ", ".join([k for k in AggregationFunctions.__dict__.keys()
                          if isinstance(k, basestring) and not k.startswith("__")])

    def __contains__(self, item):
        return item in AggregationFunctions.__dict__.values()

agg = AggregationFunctions()


class GroupByHistogram:
    """
    Class for histogram aggregation function that uses cutoffs to compute histograms
    """
    def __init__(self, cutoffs, include_lowest=True, strict_binning=False):
        for c in cutoffs:
            if not isinstance(c, (int, long, float, complex)):
                raise ValueError("Bad value %s in cutoffs, expected a number")
        self.cutoffs = cutoffs
        self.include_lowest = include_lowest
        self.strict_binning = strict_binning

    def __repr__(self):
        return 'HISTOGRAM=' + json.dumps(self.__dict__)
