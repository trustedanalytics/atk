# coding: utf-8

import trustedanalytics as ta
ta.connect()
c = ta.get_frame('copy')

print c.inspect(20)

def agg2(acc, row):
    acc.rank_sum = acc.rank_sum + row.rank

ta.errors.show_details = True
x = c.aggregate_by_key('county', [('rank_sum', ta.int64)], agg2)
print str(x)
print x.inspect()
