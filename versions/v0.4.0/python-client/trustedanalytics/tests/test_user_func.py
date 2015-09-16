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

import iatest
from trustedanalytics.meta.udf import has_udf_arg

iatest.init()

import unittest

sample_worker_message = """Job aborted due to stage failure: Task 0.0:87 failed 4 times, most recent failure: Exception failure in TID 181 on host gao-wsb.jf.trustedanalytics.com: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/pyspark/worker.py\", line 77, in main
    serializer.dump_stream(func(split_index, iterator), outfile)
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/trustedanalytics/rest/spark.py\", line 138, in dump_stream
    self.dump_stream_as_json(self._batched(iterator), stream)
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/trustedanalytics/rest/spark.py\", line 141, in dump_stream_as_json
    for obj in iterator:
  File \"/opt/cloudera/parcels/CDH-5.1.0-1.cdh5.1.0.p0.53/lib/spark/python/trustedanalytics/rest/serializers.py\", line 180, in _batched
    for item in iterator:
  File \"trustedanalytics/rest/spark.py\", line 106, in row_func
    return row_function(row_wrapper)
  File \"trustedanalytics/rest/spark.py\", line 51, in add_one_column
    result = row_function(row)
  File \"<ipython-input-8-e6a5608355ef>\", line 1, in <lambda>
ZeroDivisionError: integer division or modulo by zero

        org.apache.spark.api.python.PythonRDD$$anon$1.read(PythonRDD.scala:118)
        org.apache.spark.api.python.PythonRDD$$anon$1.<init>(PythonRDD.scala:148)
        org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:81)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.FlatMappedRDD.compute(FlatMappedRDD.scala:33)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        org.trustedanalytics.atk.engine.frame.FrameRDD.compute(FrameRDD.scala:15)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:35)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.rdd.MappedRDD.compute(MappedRDD.scala:31)
        org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:262)
        org.apache.spark.rdd.RDD.iterator(RDD.scala:229)
        org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:111)
        org.apache.spark.scheduler.Task.run(Task.scala:51)
        org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:187)
        java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
        java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
        java.lang.Thread.run(Thread.java:724)
Driver stacktrace:
"""

class TestUserFunc(unittest.TestCase):


    def test_trim_spark_worker_trace_from_exception(self):
        e = Exception(sample_worker_message)
        filter = "        org.apache.spark.api.python"
        message = "ZeroDivisionError: integer division or modulo by zero"
        self.assertTrue(e.args[0].find(filter) >= 0)

        @has_udf_arg
        def func():
            raise e

        try:
            func()
        except Exception as ex:
            # verify that the spark worker stacktrace is removed from the message
            self.assertTrue(ex.args[0].find(filter) < 0)
            self.assertTrue(ex.args[0].find(message) >= 0)
        else:
            self.fail()
