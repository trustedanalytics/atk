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


from setuptools import setup
import os
import time

setup(
    # Application name:
    name="trustedanalytics",

    # Version number (initial):
    version=u"VERSION-POSTTAG",

    # Application author details:
    author="trustedanalytics",


    # Packages
    packages=["trustedanalytics","trustedanalytics/core","trustedanalytics/rest","trustedanalytics/tests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="http://trustedanalytics.github.io/atk",

    #
    license="Apache 2",

    description="Trusted Analytics Toolkit",

    keywords="analytics big data yarn cloudera hadoop hdfs spark",

    long_description=open("README").read(),

    # Dependent packages (distributions)
    install_requires=[
        'bottle >= 0.12',
        'numpy >= 1.8.1',
        'requests >= 2.4.0',
        'ordereddict >= 1.1',
        'decorator >= 3.4.0',
        'pandas >= 0.15.0',
        'pymongo >= 3.0',
    ],
)
