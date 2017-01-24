.. _ad_scoring_pipeline:
Scoring Pipeline
================
Scoring Pipeline is a python app that can be used to perform ETL transformations followed by scoring on a deployed model, on a stream of records. The result is then either sent back to the client
or is queued up on the kafka sink topic, depending upon the mode that the app was configured in at the time of initialization.

Scoring pipeline can be initialized in 2 modes: Kafka streaming mode or REST endpoint streaming mode.

This section covers instantiating a scoring pipeline in TAP.


Create a Scoring Pipeline Instance from a broker in TAP
-------------------------------------------------------

In the TAP web site:

1) Navigate to **Services -> Marketplace**.
2) Select **scoring_pipeline** => **Create new isntance**.
3) Fill in an instance name of your choice *(given below as **etlScoring**)*.
4) You will be able to see your scoring pipeline under the Applications page and obtain its URL.
5) Now call its REST endpoint to load the tar archive (advscore.tar) containing the **configuration file** (config.json) and the **python script** (test_script.py)to be executed.
    curl -i -X POST -F file=@advscore.tar  "http://etlScoring.demotrustedanalytics.com"
6) The configuration file needs the following fields in there:
    "file_name" -- python script that needs to be executed on every streaming record
    "func_name" -- name of the function in the python script that needs to be invoked
    "input_schema" -- schema of the incoming streaming record
    "src_topic" -- kafka topic from where to start consuming the records (in case of Kafka streaming) else this field should be empty
    "sink_topic" -- kafka topic to which the app starts writing the predictions (in case of Kafka streaming) else this field should be empty

    Note: For Kafka Streaming, both source and sink topics need to be specified

Scoring Pipeline Config file template
-------------------------------------

Below is a sample json to configure scoring pipeline in kafka streaming mode:

.. code::

    { "file_name":"test_script.py", "func_name":"evaluate", "input_schema": {
            "columns": [
              {
                "name":"field_0",
                "type":"str"
              },
              {
                "name":"field_1",
                "type":"str"
              },
              .....

              {
                "name":"field_162",
                "type":"str"
              }
            ]}, "src_topic":"input", "sink_topic":"output"}

Below is a sample json to configure scoring pipeline in REST endpoint streaming mode:

.. code::

    { "file_name":"test_script.py", "func_name":"evaluate", "input_schema": {
            "columns": [
              {
                "name":"field_0",
                "type":"str"
              },
              {
                "name":"field_1",
                "type":"str"
              },
              .....

              {
                "name":"field_162",
                "type":"str"
              }
            ]}, "src_topic":"", "sink_topic":""}



Scoring Pipeline Python Script Example
--------------------------------------


from record import Record
import atktypes as atk
import numpy as np


def add_numeric_time(row):
    try:
        x = row['field_0'].split(" ")
        date = x[0]
        time = x[1]
        t = time.split(":")
        numeric_time = float(t[0]) + float(t[1])/60.0+float(t[2])/3600.0
        return [date, numeric_time]
    except:
        return ["None", None]

def string_to_numeric(column_list):
    def string_to_numeric2(row):
        result = []
        for column in column_list:
            try:
                result.append(float(row[column]))
            except:
                result.append(None)
        return result
    return string_to_numeric2

def drop_null(column_list):
    def drop_null2(row):
        result = False
        for col in column_list:
            result = True if row[col] == None else result
        return result
    return drop_null2

column_list = ['field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]
new_columns_schema = [('num_' + x, atk.float64) for x in column_list]

PCA_column_list = ['num_field_'+ str(x) for x in range(19,136) if np.mod(x,4)==3]

y= ['field_'+str(i) for i in range(0, 163)]
y.extend(['Venus'])
y.extend(['Mercury'])

def evaluate(record):
    record.add_columns(add_numeric_time, [('date', str), ('numeric_time', atk.float64)])
    record.add_columns(string_to_numeric(column_list), new_columns_schema)
    record.rename_columns({'date':'Venus', 'numeric_time':'Mercury'})
    record.drop_columns(y)
    result = record.filter(drop_null(PCA_column_list))
    print("result is %s" %result)
    if not result:
    	r = record.score("formosascoringengine2.demotrustedanalytics.com")
    	return r


6) If the scoring pipeline was configured to work with Kafka messaging queues then start streaming records to the source-topic.
7) If the scoring pipeline was configured to use the REST endpoints, then you can post requests using curl command as follows:
    curl -H "Content-type: application/json" -X POST -d '{"message": "4/3/2016 10:32, P0001,1,0.0001,....., 192,-4.1158,192,3.8264"}' http://etlscoring.demotrustedanalytics.com/v2/score

