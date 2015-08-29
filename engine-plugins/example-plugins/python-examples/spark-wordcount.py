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

#
# Word count example - create a frame and count words
#
# Depends on a movie.csv file
#
# Usage:
#
#   Copy data to HDFS
#       hadoop fs -copyFromLocal movie.csv {fsRoot in HDFS}
#
#   At Python prompt
#       import os
#       execfile('/path/to/spark-wordcount.py')
#

import trustedanalytics as atk

# show full stack traces
atk.errors.show_details = True

atk.connect()()

#atk.loggers.set_http()

print("server ping")
atk.server.ping()

print("define csv file")
csv = atk.CsvFile("/movie.csv", schema= [('user', atk.int32),
                                        ('vertexType', str),
                                        ('movie', atk.int32),
                                        ('rating', str),
                                        ('splits', str)])

print("create frame")
frame = atk.Frame(csv)

errors = frame.get_error_frame()

print("inspect frame")
print frame.inspect(10)
print("frame row count " + str(frame.row_count))

print("inspect frame errors")
print errors.inspect(10)
print("frame row count " + str(errors.row_count))

print("count words in frame")
print frame.wordcount()
