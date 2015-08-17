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

import os
import sys
import trustedanalytics.rest.spark


class udf(object):
    """
    An object for containing  and managing all the UDF dependencies.

    """
    @staticmethod
    def install(dependencies):
        """
        installs the specified files on the cluster

        Notes
        -----
        The files to install can be:
        either local python scripts without any packaging structure
        e.g.: ta.Udf.install(['my_script.py']) where my_script.py is in the same local folder
        or
        absolute path to a valid python package/module which includes the intended python file and all its
        dependencies.
        e,g: ta.Udf.install(['testcases/auto_tests/my_script.py'] where it is essential to install the whole 'testcases'
        module on the worker nodes as 'my_script.py' has other dependencies in the 'testcases' module. There are certain
        pitfalls associated with this approach:
        In this case all the folders, subfolders and files within 'testcases' directory get zipped, serialized and
        copied over to each of the worker nodes every time that the user calls a function that uses UDFs.

        So, to keep the overhead low, it is strongly advised that the users make a separate 'utils' folder and keep all
        their python libraries in that folder and simply install that folder to the workers.
        Also, when you do not need the dependencies for future function calls, you can prevent them from getting copied
        over every time by doing ta.Udf.install([]), to empty out the install list.
        This approach does not work for imports that use relative paths.
        
        :param dependencies: the file dependencies to be serialized to the cluster
        :return: nothing
        """
        if dependencies is not None:
            trustedanalytics.rest.spark.UdfDependencies = dependencies
        else:
            raise ValueError ("The dependencies list to be installed on the cluster cannot be empty")

    @staticmethod
    def list():
        """
        lists all the user files getting copied to the cluster
        :return: list of files to be copied to the cluster
        """
        return trustedanalytics.rest.spark.UdfDependencies
