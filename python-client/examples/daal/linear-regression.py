##############################################################################
# INTEL CONFIDENTIAL
#
# Copyright 2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related to
# the source code (Material) are owned by Intel Corporation or its suppliers
# or licensors. Title to the Material remains with Intel Corporation or its
# suppliers and licensors. The Material may contain trade secrets and
# proprietary and confidential information of Intel Corporation and its
# suppliers and licensors, and is protected by worldwide copyright and trade
# secret laws and treaty provisions. No part of the Material may be used,
# copied, reproduced, modified, published, uploaded, posted, transmitted,
# distributed, or disclosed in any way without Intel's prior express written
# permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or
# delivery of the Materials, either expressly, by implication, inducement,
# estoppel or otherwise. Any license under such intellectual  property rights
# must be express and approved by Intel in writing.
##############################################################################

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