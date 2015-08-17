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
Admin commands, not part of public API
"""

from trustedanalytics.rest.command import execute_command


def _explicit_garbage_collection(age_to_delete_data = None, age_to_delete_meta_data = None):
    """
    Execute garbage collection out of cycle age ranges specified using the typesafe config duration format.
    :param age_to_delete_data: Minimum age for data deletion. Defaults to server config.
    :param age_to_delete_meta_data: Minimum age for meta data deletion. Defaults to server config.
    """
    execute_command("_admin:/_explicit_garbage_collection", None,
                    age_to_delete_data=age_to_delete_data,
                    age_to_delete_meta_data=age_to_delete_meta_data)
