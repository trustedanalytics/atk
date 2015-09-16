/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.graphbuilder.io;

import org.trustedanalytics.atk.graphbuilder.titan.io.HBaseUniformSplitter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class HBaseUniformSplitterTest {

    @Test
    public void testCreate2InputSplits() throws Exception {
        byte[] startRow = Bytes.toBytes(0L);
        byte[] middle = Bytes.toBytes(3L);
        byte[] endRow = Bytes.toBytes(5L);

        // Test split into 2 parts
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, endRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(2);

        assertEquals(2, uniformSplits.size());
        // 0L to 3L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle));
        // 3L to 5L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), endRow));
    }


    @Test
    public void testCreateInput4Splits() throws Exception {
        byte[] startRow = Bytes.toBytes(10L);
        byte[] middle1 = Bytes.toBytes(35L);
        byte[] middle2 = Bytes.toBytes(60L);
        byte[] middle3 = Bytes.toBytes(85L);
        byte[] endRow = Bytes.toBytes(110L);

        // Test split into 2 pars
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow, middle2, "location");
        TableSplit tableSplit2 = new TableSplit(TableName.valueOf("table"), middle2, endRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);
        inputSplits.add(tableSplit2);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(4);

        // Test split into four parts.
        assertEquals(4, uniformSplits.size());
        // 10L to 35L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getStartRow(), startRow));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(0)).getEndRow(), middle1));
        // 35L to 60L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getStartRow(), middle1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(1)).getEndRow(), middle2));
        // 60L to 85L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getStartRow(), middle2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(2)).getEndRow(), middle3));
        // 85L to 110L
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(3)).getStartRow(), middle3));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits.get(3)).getEndRow(), endRow));
    }

    @Test
    public void testCreateInputSplitsEmptyEndRow() throws Exception {
        // If endRow is empty (i.e., last region in table), use max key to split region

        // Test split when start row is empty
        byte[] startRow1 = HConstants.EMPTY_BYTE_ARRAY;
        byte[] middle1 = Bytes.toBytes(1L << 31);
        byte[] endRow = HConstants.EMPTY_BYTE_ARRAY;
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), startRow1, HConstants.EMPTY_BYTE_ARRAY, "location");
        List<InputSplit> inputSplits1 = new ArrayList<>();
        inputSplits1.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter1 = new HBaseUniformSplitter(inputSplits1);
        List<InputSplit> uniformSplits1 = uniformSplitter1.createInputSplits(2);

        assertEquals(2, uniformSplits1.size());
        assertTrue(Bytes.equals(((TableSplit) uniformSplits1.get(0)).getStartRow(), startRow1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits1.get(0)).getEndRow(), middle1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits1.get(1)).getStartRow(), middle1));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits1.get(1)).getEndRow(), endRow));

        // Test split when start row is not empty
        byte[] startRow2 = new byte[2];
        byte[] middle2 = new byte[2];

        Arrays.fill(startRow2, (byte)0xBB);
        Arrays.fill(middle2, (byte)0xDD);
        TableSplit tableSplit2 = new TableSplit(TableName.valueOf("table"), startRow2, HConstants.EMPTY_BYTE_ARRAY, "location");
        List<InputSplit> inputSplits2 = new ArrayList<>();
        inputSplits2.add(tableSplit2);

        HBaseUniformSplitter uniformSplitter2 = new HBaseUniformSplitter(inputSplits2);
        List<InputSplit> uniformSplits2 = uniformSplitter2.createInputSplits(2);

        assertEquals(2, uniformSplits2.size());
        assertTrue(Bytes.equals(((TableSplit) uniformSplits2.get(0)).getStartRow(), startRow2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits2.get(0)).getEndRow(), middle2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits2.get(1)).getStartRow(), middle2));
        assertTrue(Bytes.equals(((TableSplit) uniformSplits2.get(1)).getEndRow(), endRow));
    }

    @Test
    public void testCreateInvalidSplits() throws Exception {
        byte[] startRow = Bytes.toBytes("AAA");
        byte[] middle = Bytes.toBytes("CCC");
        byte[] endRow = Bytes.toBytes("EEE");

        // If endRow > startRow, do not split table (return input splits)
        TableSplit tableSplit1 = new TableSplit(TableName.valueOf("table"), endRow, startRow, "location");
        List<InputSplit> inputSplits = new ArrayList<>();
        inputSplits.add(tableSplit1);

        HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(inputSplits);
        List<InputSplit> uniformSplits = uniformSplitter.createInputSplits(4);

        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));

        // If splits per region == 0, do not split table
        uniformSplits = uniformSplitter.createInputSplits(0);
        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));


        // If splits per region < 0, do not split table
        uniformSplits = uniformSplitter.createInputSplits(-3);
        assertEquals(1, uniformSplits.size());
        assertTrue(inputSplits.equals(uniformSplits));


        // If startrow == endrow, do not split table
        TableSplit tableSplit2 = new TableSplit(TableName.valueOf("table"), startRow, startRow, "location");
        List<InputSplit> inputSplits2 = new ArrayList<>();
        inputSplits2.add(tableSplit2);

        HBaseUniformSplitter uniformSplitter2 = new HBaseUniformSplitter(inputSplits2);
        List<InputSplit> uniformSplits2 = uniformSplitter2.createInputSplits(5);
        assertEquals(1, uniformSplits2.size());
        assertTrue(inputSplits2.equals(uniformSplits2));

    }


}
