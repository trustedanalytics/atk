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


def drop_stale(stale_age=None):
    """
    Execute garbage collection out of cycle to drop stale entities
    :param stale_age: Minimum age to qualify as "stale".  Defaults to server config.  Age specified using the typesafe config duration format.
    :type stale_age: str
    """
    execute_command("_admin:/_drop_stale", None, stale_age=stale_age)


def finalize_dropped():
    """
    Execute garbage collection out of cycle to finalize all dropped entities (i.e. erase their data)
    """
    execute_command("_admin:/_finalize_dropped", None, bogus=0)
