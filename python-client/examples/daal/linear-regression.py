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

import trustedanalytics as ta
import matplotlib.pyplot as plt

ta.connect()


print("define csv file")
schema = [('col_0', ta.float64),
          ('col_1', ta.float64),
          ('col_2', ta.float64),
          ('col_3', ta.float64),
          ('col_4', ta.float64),
          ('col_5', ta.float64),
          ('col_6', ta.float64),
          ('col_7', ta.float64),
          ('col_8', ta.float64),
          ('col_9', ta.float64),
          ('col_10', ta.float64),
          ('col_11', ta.float64)]
print("create big frame")
train_frame = ta.Frame(ta.CsvFile("/linear_regression_train.csv", schema= schema))
test_frame = ta.Frame(ta.CsvFile("/linear_regression_test.csv", schema= schema))
print("train frame row count " + str(train_frame.row_count))
print("test frame row count " + str(test_frame.row_count))

model = ta.DaalLinearRegressionModel()
feature_column_names = ['col_0', 'col_1','col_2','col_3','col_4','col_5','col_6','col_7','col_8','col_9']
label_column_names = ['col_10', 'col_11']

model_betas = model.train(train_frame, feature_column_names, label_column_names)
print ("linear regression model coefficients:" + str(model_betas))

predict_frame = model.predict(test_frame, feature_column_names, label_column_names)
df = predict_frame.download(test_frame.row_count)
plt.scatter(df['col_10'], df['predict_col_10'])
plt.plot(df['col_10'], df['predict_col_10'], color='r', linewidth=2.0, linestyle='--')
plt.xlabel('col_10')
plt.ylabel('predicted col_10')
plt.show()