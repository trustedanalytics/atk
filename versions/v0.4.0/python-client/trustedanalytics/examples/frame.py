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
    The default home directory is hdfs://user/atkuser all the sample data sets are saved to
    hdfs://user/atkuser/datasets when installing through the rpm
    you will need to copy the data sets to hdfs manually otherwise and adjust the data set location path accordingly
    :param path: data set hdfs path can be full and relative path
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
