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
from trustedanalytics import examples

def run(path=r"datasets/cities.csv", ta=None):
    """
    Loads cities.csv into a frame and runs some simple frame operations. We will be dropping columns and adding new ones with python lambdas.
    We are not required to use cities.csv but rather it's schema. Any other csv file with the correct schema and delimeter will work.

    Parameters
    ----------
    path : str
        The HDFS path to the cities.csv dataset. If a path is not given the default is datasets/cities.csv. The dataset is
        available in the examples/datasets directory and in `github<https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples/datasets>`__.
        Must be a valid HDFS path either fully qualified hdfs://some/path or relative the ATK rest servers HDFS home directory.

    ta : trusted analytics python import
        Can be safely ignored when running examples. It is only used during integration testing to pass pre-configured
        python client reference.


    Returns
    -------
        A dictionary with the final built frame


    Datasets
    --------
      All the datasets can be found in the examples/datasets directory of the python client or in `github<https://github.com/trustedanalytics/atk/tree/master/python-client/trustedanalytics/examples/datasets>`__.


    Dataset
    -------
      Name : cities.csv

      schema:
        rank(int32) | city(str) | population_2013(int32) | population_2010(int32) | change(str) | county(str)

        sample

        .. code::

          1|Portland|609456|583776|4.40%|Multnomah
          2|Salem|160614|154637|3.87%|Marion
          3|Eugene|159190|156185|1.92%|Lane

      delimeter: |


    Example
    -------
        To run the frame example first import the example.

        .. code::

          >>>import trustedanalytics.examples.frame as frame

        After importing you can execute run method with the path to the dataset

        .. code::

          >>>frame.run("hdfs://FULL_HDFS_PATH")



    """
    NAME = "TEST"

    if ta is None:
        ta = examples.connect()

    #csv schema definition
    schema = [('rank', ta.int32),
              ('city', str),
              ('population_2013', ta.int32),
              ('population_2010', ta.int32),
              ('change', str),
              ('county', str)]

    #import csv file
    csv = ta.CsvFile(path, schema, skip_header_lines=1, delimiter='|')

    frames = ta.get_frame_names()
    if NAME in frames:
        print "Deleting old '{0}' frame.".format(NAME)
        ta.drop_frames(NAME)

    print "Building frame '{0}'.".format(NAME)

    frame = ta.Frame(csv, NAME)

    print "Inspecting frame '{0}'.".format(NAME)

    print frame.inspect()

    print "Drop Change column."

    frame.drop_columns("change")

    print frame.inspect()

    print "Add Change column."
    frame.add_columns(lambda row: ((row.population_2013 - row.population_2010)/float(row.population_2010)) * 100,
                      ("change", ta.float32))

    print frame.inspect()

    print "Drop Change column."

    frame.drop_columns("change")

    print frame.inspect()

    print "Add Change columns."

    frame.add_columns(lambda row: [row.population_2013 - row.population_2010, ((row.population_2013 - row.population_2010)/float(row.population_2010)) * 100 ], [("difference", ta.int32 ), ("change", ta.float32 )])

    print "Format inspection."
    print frame.inspect(10, wrap=10, columns=["city", "population_2013", "population_2010", "change", "difference"], round=2)


    return {"frame": frame}
