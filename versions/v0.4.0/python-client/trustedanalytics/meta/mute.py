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
Mute definitions from the server
"""

# straightforward approach for now
muted_commands = ['frame/load',
                  'frame/count_where',
                  'frame:/load',
                  'frame:vertex/load',
                  'frame:edge/load',
                  'frame:/load',
                  'frame:/project',
                  'frame/rename',
                  'frame:/rename',
                  'frame:vertex/rename',
                  'frame:edge/rename',
                  #'graph/load',  # TODO - fix ref to graph.load in TitanGraph
                  #'graph:/load',
                  #'graph:titan/load',
                  'graph/rename',
                  'graph:/rename',
                  'graph:titan/rename',
                  'model/rename',
                  ]
