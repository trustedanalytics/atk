#!/bin/bash
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

#
# Generate code coverage numbers for Scala
#
# Scoverage does NOT aggregate reports for multi-module projects so we aggregate ourselves here.
#

echo "assuming combine.sh is being ran from source code"

# list of modules we want coverage for
scala_coverage_modules="engine/interfaces engine/meta-store engine/engine-core engine/graphbuilder engine-plugins/frame-plugins engine-plugins/graph-plugins engine-plugins/model-plugins engine-plugins/giraph-plugins rest-server misc/launcher"

# target directory to generate report
report_target=misc/scala-coverage/target/scala-coverage-report

# make sure old folder is gone
rm -rf ${report_target}

# re-create target folder
mkdir -p ${report_target}

# copy resources into report
cp -r misc/scala-coverage/src/main/resources/* ${report_target}

for module in `echo $scala_coverage_modules`
do
  if [ -e $module ]
  then
    # only one module at a time can be ran with scoverage otherwise you get bad numbers
    cd $module

    # fix issue with links in overview.html
    sed -i 's:a href=".*com/trustedanalytics/:a href="com/trustedanalytics/:g' target/site/scoverage/overview.html

    # save coverage report to code-coverage project
    cp -r target/site/scoverage ../${report_target}/${module}-scoverage-report

    cd ..
  fi
done
