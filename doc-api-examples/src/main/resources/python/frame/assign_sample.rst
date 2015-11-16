<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
<connect>

>>> frame = ta.Frame(ta.UploadRows([["abc", 0],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["bcd", 9],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["def", 1],
...                                 ["ghi", 2],
...                                 ["jkl", 3],
...                                 ["mno", 4],
...                                 ["pqr", 5],
...                                 ["stu", 6],
...                                 ["vwx", 7],
...                                 ["yza", 8],
...                                 ["bcd", 9]],
...                                [("blip", str), ("id", ta.int32)]))
<progress>

</hide>

Consider this simple frame.

>>> frame.inspect()
[#]  blip  id
=============
[0]  abc    0
[1]  def    1
[2]  ghi    2
[3]  jkl    3
[4]  mno    4
[5]  pqr    5
[6]  stu    6
[7]  vwx    7
[8]  yza    8
[9]  bcd    9

We'll assign labels to each row according to a rough 40-30-30 split, for
"train", "test", and "validate".

>>> frame.assign_sample([0.4, 0.3, 0.3])
<progress>

>>> frame.inspect()
[#]  blip  id  sample_bin
=========================
[0]  abc    0  VA
[1]  def    1  TR
[2]  ghi    2  TE
[3]  jkl    3  TE
[4]  mno    4  TE
[5]  pqr    5  TR
[6]  stu    6  TR
[7]  vwx    7  VA
[8]  yza    8  VA
[9]  bcd    9  VA

<hide>
# If the inspect proves to be unweildy (i.e. non-deterministic assign_sample),
# then try testing output with these lines...
#
# >>> frame.column_names[2]
# u'sample_bin'
#
# >>> sample = frame.take(n=10, columns=['sample_bin'])
# >>> len([x for x in map(lambda s: str(s[0]), sample) if x in ['VA', 'TR', 'TE']])
# 10
#
</hide>

Now the frame  has a new column named "sample_bin" with a string label.
Values in the other columns are unaffected.

Here it is again, this time specifying labels, output column and random seed

>>> frame.assign_sample([0.2, 0.2, 0.3, 0.3],
...                     ["cat1", "cat2", "cat3", "cat4"],
...                     output_column="cat",
...                     random_seed=12)
<progress>

>>> frame.inspect()
[#]  blip  id  sample_bin  cat
===============================
[0]  abc    0  VA          cat4
[1]  def    1  TR          cat2
[2]  ghi    2  TE          cat3
[3]  jkl    3  TE          cat4
[4]  mno    4  TE          cat1
[5]  pqr    5  TR          cat3
[6]  stu    6  TR          cat2
[7]  vwx    7  VA          cat3
[8]  yza    8  VA          cat3
[9]  bcd    9  VA          cat4

