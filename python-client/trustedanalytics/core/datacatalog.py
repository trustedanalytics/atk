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

from trustedanalytics.rest.atkserver import server
from trustedanalytics.core.ui import RowsInspection
from trustedanalytics.core.row import Row
from trustedanalytics.core import ui
from trustedanalytics.rest.frame import FrameSchema

class DataCatalog(object):

    def __init__(self):
        self.refresh()
        print self.show()

    def refresh(self):
        self.uri = "datacatalog"
        self.catalog = server.get(self.uri).json()
        self.schema = FrameSchema.from_strings_to_types(self.catalog["metadata"]['columns'])
        self.data = self.catalog["data"]
        self.rows = map(lambda row: Row(self.schema, row), self.data)
        self.default_format_setting = ui.InspectSettings(wrap=len(self.schema), truncate=10, width=200)

    def show(self, format_settings=None):
        format_settings = self.default_format_setting if format_settings is None else format_settings
        return RowsInspection(self.data, self.schema, 0, format_settings)

    def __getitem__(self, slice):
        return self.rows[slice]
