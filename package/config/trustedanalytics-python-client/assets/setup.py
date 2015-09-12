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


install_requires = [
    'bottle >= 0.12',
    'requests >= 2.4.0',
    'ordereddict >= 1.1',
    'decorator >= 3.4.0',
    ]
system_spec_requires = {'posix' : ['numpy >= 1.8.1', 'pandas >= 0.15.0', 'pymongo >= 3.0',]}

install_requires.extend(system_spec_requires.get(os.name, []))

setup(
    # Application name:
    name="trustedanalytics",

    # Version number (initial):
    version=u"VERSION-POSTTAG",

    # Application author details:
    author="Intel",
    author_email="atksupport@trustedanalytics.org",

    # Packages
    packages=["trustedanalytics","trustedanalytics/core","trustedanalytics/rest","trustedanalytics/tests"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="https://trustedanalytics.org",

    #
    license="LICENSE.txt",
    description="trusted analytics Toolkit build ID #BUILD_NUMBER#",

    long_description=open("README").read(),

    # Dependent packages (distributions)
    install_requires=install_requires,

)
