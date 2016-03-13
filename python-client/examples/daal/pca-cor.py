# vim: set encoding=utf-8
#  Copyright (c) 2016 Intel Corporation 
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


ta.connect()


print("define csv file")
csv = ta.CsvFile("/PcaCor_Normalized.csv", schema= [('col_0', ta.float64),
                                                    ('col_1', ta.float64),
                                                    ('col_2', ta.float64),
                                                    ('col_3', ta.float64),
                                                    ('col_4', ta.float64),
                                                    ('col_5', ta.float64),
                                                    ('col_6', ta.float64),
                                                    ('col_7', ta.float64),
                                                    ('col_8', ta.float64),
                                                    ('col_9', ta.float64)])
print("create big frame")
frame = ta.Frame(csv)

print("inspect frame")
frame.inspect(20)
print("frame row count " + str(frame.row_count))

print("DAAL Principal Components Analysis (PCA)")
column_names = ['col_0', 'col_1','col_2','col_3','col_4','col_5','col_6','col_7','col_8','col_9']
pca_results_frame = frame.daal_pca_cor(column_names)

pca_results_frame.inspect(20)