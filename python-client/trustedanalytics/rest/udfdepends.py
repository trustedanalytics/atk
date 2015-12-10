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

# todo: delete this file once API deprecation period is over.

import trustedanalytics.rest.udfzip
from trustedanalytics.core.decorators import deprecated


class udf(object):
    """
    An object for containing  and managing all the UDF dependencies.

    """
    @staticmethod
    @deprecated("Use udf_dependencies list.")
    def install(dependencies):
        """
        (deprecated) pls. see ta.udf_dependencies
        """
        if dependencies is not None:
            trustedanalytics.udf_dependencies = dependencies
        else:
            raise ValueError ("The dependencies list to be installed on the cluster cannot be empty")

    @staticmethod
    @deprecated("Use udf_dependencies list.")
    def list():
        """
        (deprecated) pls. see ta.udf_dependencies
        """
        return trustedanalytics.udf_dependencies
