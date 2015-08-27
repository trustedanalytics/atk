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

from trustedanalytics.core.atktypes import valid_data_types


class Column(object):
    """Column in a Frame"""
    def __init__(self, frame, name, data_type):
        self.name = name
        self.data_type = valid_data_types.get_from_type(data_type)
        self._frame = frame

    def __repr__(self):
        return '{ "name" : "%s", "data_type" : "%s" }' % (self.name, valid_data_types.to_string(self.data_type))

    def _as_json_obj(self):
        return { "name": self.name,
                 "data_type": valid_data_types.to_string(self.data_type),
                 "frame": None if not self.frame else self.frame.uri}

    @property
    def frame(self):
        return self._frame
