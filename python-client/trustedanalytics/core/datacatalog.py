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

from trustedanalytics.rest.atkserver import server
from trustedanalytics.core.ui import RowsInspection
from trustedanalytics.core.row import Row
from trustedanalytics.core import ui
from trustedanalytics.rest.frame import FrameSchema

class DataCatalog(object):

    def __init__(self):
        self.uri = "datacatalog"
        self.rows = []

    def inspect(self, format_settings=None):
        catalog = server.get(self.uri).json()
        schema = FrameSchema.from_strings_to_types(catalog["metadata"]['columns'])
        data = catalog["data"]
        self.rows = map(lambda row: Row(schema, row), data)
        default_format_setting = ui.InspectSettings(wrap=len(schema), truncate=10, width=200)
        format_settings = default_format_setting if format_settings is None else format_settings
        return RowsInspection(data, schema, 0, format_settings)

    def __getitem__(self, slice):
        return self.rows[slice]

    def publish(self, catalog_metadata):
        return server.post(self.uri, catalog_metadata).json()


data_catalog = DataCatalog()