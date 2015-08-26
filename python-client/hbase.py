# coding: utf-8
import trustedanalytics as tp
tp.connect()
h = tp.HBaseTable("levis", [("pants", "aisle", unicode), ("pants", "row", int), ("shirts", "aisle", unicode), ("shirts", "row", unicode)])
tp.loggers.set_http()
f = tp.Frame(h)
#f = tp.Frame()

#f.loadhbase("levis", [{"column_family" : "pants", "column_name": "aisle", "data_type" : "unicode"}, {"column_family" : "pants", "column_name" : "row", "data_type" : "int"},{"column_family" : #"shirts", "column_name" : "aisle", "data_type" : "unicode"},{"column_family" : "shirts", "column_name" : "row",  "data_type" : "unicode"}])
print f
print f.inspect()
