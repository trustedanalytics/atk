# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Post Processing of histogram result
"""
class Histogram:
    """
    Class containing the results of a histogram operation.

     Members
     --------
     cutoffs: list of doubles
        cutoff points of each bin. There are n-1 bins in the result
     hist: list of doubles
        number of weighted observations per bin
     density: list of doubles
        percentage of observations found per bin
    """
    def __init__(self, cutoffs, hist, density):
        self.hist = hist
        self.cutoffs = cutoffs
        self.density = density

    def __repr__(self):
        return """Histogram:
cutoffs: %s,
hist: %s,
density: %s""" % (self.cutoffs, self.hist, self.density)
