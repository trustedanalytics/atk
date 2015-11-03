# vim: set encoding=utf-8

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

import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 9099:
    ta.server.port = 9099
ta.connect()


class FrameFlattenColumnTest(unittest.TestCase):
    """
    Test frame.flatten_column()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        csv = ta.CsvFile("flattenable.csv", schema= [('number', ta.int32),
                                                             ('abc', str),
                                                             ('food', str)], delimiter=',')

        print "create frame"
        self.frame = ta.Frame(csv)

    def test_flatten_column_abc(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter='|')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 16)

    def test_flatten_column_food(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('food', delimiter='+')
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 17)

    def test_flatten_column_abc_and_food(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter='|')
        self.frame.flatten_column('food', delimiter='+')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 29)

    def test_flatten_column_does_nothing_with_wrong_delimiter(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_column('abc', delimiter=',')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

    def test_flatten_column_with_wrong_column_name(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # expect an exception when we call the method under test with a non-existent column name
        with self.assertRaises(Exception):
            self.frame.flatten_column('hamburger')

    def test_flatten_columns_with_a_single_column(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_columns('abc','|')

        # expected data after flattening
        expected_data = [
            [1,"a","biscuits+gravy"],
            [2,"a","ham+cheese"],
            [3,"a","grilled+tuna"],
            [4,"a","mac+cheese"],
            [4,"b","mac+cheese"],
            [4,"c","mac+cheese"],
            [5,"a","salad"],
            [5,"e","salad"],
            [6,"d","coke"],
            [7,"a","spinach+artichoke+dip"],
            [7,"b","spinach+artichoke+dip"],
            [7,"d","spinach+artichoke+dip"],
            [8,"","big+mac"],
            [9,"c","fries"],
            [9,"d","fries"],
            [10,"e",""]
        ]

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 16)
        self.assertEqual(self.frame.take(self.frame.row_count), expected_data)


    def test_flatten_columns_with_multiple_columns(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # call method under test
        self.frame.flatten_columns(['abc', 'food'],['|','+'])

        # expected data after flattening
        expected_data = [
            [1,"a","biscuits"],
            [1,None,"gravy"],
            [2,"a","ham"],
            [2,None,"cheese"],
            [3,"a","grilled"],
            [3,None,"tuna"],
            [4,"a","mac"],
            [4,"b","cheese"],
            [4,"c",None],
            [5,"a","salad"],
            [5,"e",None],
            [6,"d","coke"],
            [7,"a","spinach"],
            [7,"b","artichoke"],
            [7,"d","dip"],
            [8,"","big"],
            [8,None,"mac"],
            [9,"c","fries"],
            [9,"d",None],
            [10,"e",""]
        ]

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 20)
        self.assertEqual(self.frame.take(self.frame.row_count), expected_data)

    def test_flatten_columns_does_nothing_with_wrong_delimiter(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        #store original data
        original_data = self.frame.take(self.frame.row_count)

        # call method under test
        self.frame.flatten_columns('abc', ',')

        # validate
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)
        self.assertEqual(self.frame.take(self.frame.row_count), original_data)

    def test_flatten_columns_with_wrong_column_name(self):
        # validate expected pre-conditions
        self.assertEqual(self.frame.column_names, ['number', 'abc', 'food'])
        self.assertEqual(self.frame.row_count, 10)

        # expect an exception when we call the method under test with a non-existent column name
        with self.assertRaises(Exception):
            self.frame.flatten_columns('hamburger')

    def test_flatten_columns_with_mismatch_delimiter_count(self):
        # we need a frame with more than three columns for this test
        data = [[1,"solo,mono,single","a,b,c","1+2+3"],[2,"duo,double","d,e","4+5"]]
        schema = [('a',ta.int32), ('b', str), ('c', str), ('d', str)]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        # when providing more than one delimiter, count must match column count
        # too few delimiters should throw an exception
        with self.assertRaises(Exception):
            test_frame.flatten_columns(['b','c','d'],[',',','])

        # too many delimiters should also throw an exception
        with self.assertRaises(Exception):
            test_frame.flatten_columns(['b','c','d'],[',',',','+','|'])

        # giving just one delimiter means that the same delimiter is used for all columns
        test_frame.flatten_columns(['b','c'], ',')
        self.assertEqual(test_frame.row_count, 5)

    def test_flatten_columns_with_single_vector(self):
        data = [[1,[1,2]],[2,[3,4]],[3,[5,6]],[4,[7,8]]]
        schema = [('a', ta.int32), ('b', ta.vector(2))]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        test_frame.flatten_columns('b')

        # expected data after flattening
        expected_data = [
            [1,1.0],
            [1,2.0],
            [2,3.0],
            [2,4.0],
            [3,5.0],
            [3,6.0],
            [4,7.0],
            [4,8.0]
        ]

        self.assertEqual(test_frame.row_count, 8)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)

    def test_flatten_columns_with_multiple_vectors(self):
        data = [[1,[1,2],[8,7]],[2,[3,4],[6,5]],[3,[5,6],[4,3]],[4,[7,8],[2,1]]]
        schema = [('a', ta.int32), ('b', ta.vector(2)), ('c', ta.vector(2))]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        test_frame.flatten_columns(['b','c'])

        # expected data after flattening
        expected_data = [
            [1,1.0,8.0],
            [1,2.0,7.0],
            [2,3.0,6.0],
            [2,4.0,5.0],
            [3,5.0,4.0],
            [3,6.0,3.0],
            [4,7.0,2.0],
            [4,8.0,1.0]
        ]

        self.assertEqual(test_frame.row_count, 8)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)


    def test_flatten_column_with_differing_size_vectors(self):
        data = [[1,[1,2,3],[8,7]],[2,[4,5,6],[6,5]],[3,[7,8,9],[4,3]],[4,[10,11,12],[2,1]]]
        schema = [('a', ta.int32), ('b', ta.vector(3)), ('c', ta.vector(2))]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        test_frame.flatten_columns(['b','c'])

        # expected data after flattening
        expected_data = [
            [1,1.0,8.0],
            [1,2.0,7.0],
            [1,3.0,0.0],
            [2,4.0,6.0],
            [2,5.0,5.0],
            [2,6.0,0.0],
            [3,7.0,4.0],
            [3,8.0,3.0],
            [3,9.0,0.0],
            [4,10.0,2.0],
            [4,11.0,1.0],
            [4,12.0,0.0]
        ]

        self.assertEqual(test_frame.row_count, 12)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)

    def test_flatten_columns_with_strings_and_vectors(self):
        data = [[1,"1:2",[1,2],"a|b"],[2,"3:4",[3,4],"c|d"],[3,"5:6",[5,6],"e|f"],[4,"7:8",[7,8],"g|h"]]
        schema = [('a', ta.int32),('b', str), ('c', ta.vector(2)), ('d', str)]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        test_frame.flatten_columns(['b', 'c', 'd'], [':','|'])

        # expected data after flattening
        expected_data = [
            [1,"1",1.0,"a"],
            [1,"2",2.0,"b"],
            [2,"3",3.0,"c"],
            [2,"4",4.0,"d"],
            [3,"5",5.0,"e"],
            [3,"6",6.0,"f"],
            [4,"7",7.0,"g"],
            [4,"8",8.0,"h"]
        ]

        self.assertEqual(test_frame.row_count, 8)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)

    def test_flatten_columns_with_strings_and_vectors_with_one_delimiter(self):
        data = [[1,"1:2",[1,2],"a:b"],[2,"3:4",[3,4],"c:d"],[3,"5:6",[5,6],"e:f"],[4,"7:8",[7,8],"g:h"]]
        schema = [('a', ta.int32),('b', str), ('c', ta.vector(2)), ('d', str)]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        test_frame.flatten_columns(['b', 'c', 'd'], ':')

        # expected data after flattening
        expected_data = [
            [1,"1",1.0,"a"],
            [1,"2",2.0,"b"],
            [2,"3",3.0,"c"],
            [2,"4",4.0,"d"],
            [3,"5",5.0,"e"],
            [3,"6",6.0,"f"],
            [4,"7",7.0,"g"],
            [4,"8",8.0,"h"]
        ]

        self.assertEqual(test_frame.row_count, 8)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)

    def test_flatten_columns_with_strings_and_vectors_with_default_delimiter(self):
        data = [[1,"1,2",[1,2],"a,b"],[2,"3,4",[3,4],"c,d"],[3,"5,6",[5,6],"e,f"],[4,"7,8",[7,8],"g,h"]]
        schema = [('a', ta.int32),('b', str), ('c', ta.vector(2)), ('d', str)]
        test_frame = ta.Frame(ta.UploadRows(data,schema))

        # there are only 2 string columns.  giving 3 delimiters should give an exception.
        with self.assertRaises(Exception):
            test_frame.flatten_columns(['b', 'c', 'd'], [',',',',','])

        test_frame.flatten_columns(['b', 'c', 'd'])

        # expected data after flattening
        expected_data = [
            [1,"1",1.0,"a"],
            [1,"2",2.0,"b"],
            [2,"3",3.0,"c"],
            [2,"4",4.0,"d"],
            [3,"5",5.0,"e"],
            [3,"6",6.0,"f"],
            [4,"7",7.0,"g"],
            [4,"8",8.0,"h"]
        ]

        self.assertEqual(test_frame.row_count, 8)
        self.assertEqual(test_frame.take(test_frame.row_count), expected_data)



if __name__ == "__main__":
    unittest.main()
