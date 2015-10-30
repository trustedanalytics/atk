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

# Frame
def get_frame_backend():
    global _frame_backend
    if _frame_backend is None:
        from trustedanalytics.rest.frame import FrameBackendRest
        _frame_backend = FrameBackendRest()
    return _frame_backend
_frame_backend = None

# Graph
def get_graph_backend():
    global _graph_backend
    if _graph_backend is None:
        from trustedanalytics.rest.graph import GraphBackendRest
        _graph_backend = GraphBackendRest()
    return _graph_backend
_graph_backend = None
