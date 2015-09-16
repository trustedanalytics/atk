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
config file for rest client
"""


# default connection config
class server_defaults:
    uri="localhost:9099"
    scheme = 'http'
    headers = {'Content-type': 'application/json',
               'Accept': 'application/json,text/plain'}
    api_version = 'v1'
    user = 'test_api_key_1'
    uaa_scheme = "https"
    uaa_headers = {"Accept": "application/json"}

class upload_defaults:
    rows = 10000


class requests_defaults:
    ping_timeout_secs = 10
    request_timeout_secs = None  # None means no timeout


class polling_defaults:
    start_interval_secs = 1
    max_interval_secs = 20
    backoff_factor = 1.02
