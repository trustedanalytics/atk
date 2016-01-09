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
# estoppel or otherwise. Any license under such intellectual property rights
# must be express and approved by Intel in writing.
##############################################################################

import trustedanalytics as ta

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