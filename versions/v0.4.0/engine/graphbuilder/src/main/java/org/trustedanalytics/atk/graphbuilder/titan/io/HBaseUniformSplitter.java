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

package org.trustedanalytics.atk.graphbuilder.titan.io;

import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Split HBase regions uniformly.
 * <p/>
 * HBase's default TableInputFormat class assigns a single mapper per region. This class uniformly
 * splits HBase regions based on a user-defined split count.
 */
public class HBaseUniformSplitter {

    public static final byte xFF = (byte) 0xFF;

    // Uses a similar formula to com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore
    public static final byte[] maxRowKey = Bytes.toBytes(((1L << 32) - 1L));
    private final List<InputSplit> initialSplits;

    public HBaseUniformSplitter(List<InputSplit> initialSplits) {
        this.initialSplits = initialSplits;
    }


    /**
     * Create uniform input splits.
     *
     * @param requestedRegionCount Requested region count.
     * @return List of uniformly split regions
     */
    public List<InputSplit> createInputSplits(int requestedRegionCount) {
        int initialRegionCount = initialSplits.size();
        int splitsPerRegion = calculateSplitsPerRegion(requestedRegionCount, initialRegionCount);

        List<InputSplit> newSplits = new ArrayList<>();

        for (InputSplit split : initialSplits) {
            List<InputSplit> regionSplits = splitRegion((TableSplit) split, splitsPerRegion);
            newSplits.addAll(regionSplits);
        }

        return newSplits;
    }

    /**
     * Split a single region into multiple input splits that server as inputs to mappers
     *
     * @param inputSplit      Input region split
     * @param splitsPerRegion Number of splits per region
     * @return New region split
     */
    private List<InputSplit> splitRegion(TableSplit inputSplit, int splitsPerRegion) {

        byte[] startKey = inputSplit.getStartRow();
        byte[] endKey = inputSplit.getEndRow();

        List<InputSplit> regionSplits;

        if (!Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)) {
            regionSplits = createUniformSplits(inputSplit, startKey, endKey, splitsPerRegion);
        } else {
            byte[] lastRegionKey = getLastRegionKey(inputSplit, startKey);
            regionSplits = createUniformSplits(inputSplit, startKey, lastRegionKey, splitsPerRegion);

            if (regionSplits.size() > 0) {
                int lastSplitIdx = regionSplits.size() - 1;
                TableSplit lastSplit = (TableSplit) regionSplits.get(lastSplitIdx);
                TableSplit newLastSplit = new TableSplit(lastSplit.getTable(), lastSplit.getStartRow(),
                        HConstants.EMPTY_BYTE_ARRAY, lastSplit.getRegionLocation(), lastSplit.getLength());
                regionSplits.set(lastSplitIdx, newLastSplit);
            }

        }

        return regionSplits;
    }

    /**
     * Uniformly splits region based on start and end keys.
     *
     * @param initialSplit    Initial region split
     * @param startKey        Start key of split
     * @param endKey          End key of splits
     * @param splitsPerRegion Number of splits to create
     * @return Uniform splits for region
     */
    private List<InputSplit> createUniformSplits(TableSplit initialSplit, byte[] startKey, byte[] endKey,
                                                 int splitsPerRegion) {
        List<InputSplit> splits = new ArrayList<>();

        byte[][] splitKeys = getSplitKeys(startKey, endKey, splitsPerRegion);

        if (splitKeys != null) {
            for (int i = 0; i < splitKeys.length - 1; i++) {
                TableSplit tableSplit = new TableSplit(initialSplit.getTable(), splitKeys[i],
                        splitKeys[i + 1], initialSplit.getRegionLocation(), initialSplit.getLength());
                splits.add(tableSplit);
            }
        } else {
            Log.warn("Unable to create " + splitsPerRegion + " HBase splits/region for: "
                    + initialSplit.getTable() + "/" + initialSplit +
                    ". Will use default split.");
            splits.add(initialSplit);
        }

        return (splits);
    }


    /**
     * Calculates the number of splits per region based on the requested number of region splits.
     *
     * @param requestedRegionCount Requested region count
     * @param initialRegionCount   Initial region count
     * @return Number of splits/region = max(requestedRegionCount/initialRegionCount, 1)
     */
    private int calculateSplitsPerRegion(int requestedRegionCount, int initialRegionCount) {

        int splitsPerRegion = 1;

        if (initialRegionCount > 0 && requestedRegionCount > initialRegionCount) {
            splitsPerRegion = (int) Math.ceil(requestedRegionCount / initialRegionCount);
        }

        return splitsPerRegion;
    }

    /**
     * Get split keys based on start and end keys, and requested number of splits.
     *
     * @param startKey        Start key of split
     * @param endKey          End key of splits
     * @param splitsPerRegion Number of splits per region
     * @return Split keys for range
     */
    private byte[][] getSplitKeys(byte[] startKey, byte[] endKey, int splitsPerRegion) {
        byte[][] splitKeys = null;
        if (splitsPerRegion > 1) {
            try {
                //Bytes.split() creates X+1 splits. If you want to split the range into Y, specify Y-1.
                splitKeys = Bytes.split(startKey, endKey, true, (splitsPerRegion - 1));
            } catch (IllegalArgumentException e) {
                Log.warn("Exception while getting split keys:" + e);
            }
        }
        return splitKeys;
    }

    /**
     * Estimate the end key for the last region.
     * <p/>
     * The end key of the last region is empty in a HBase table.
     *
     * @param inputSplit Input region split
     * @param startKey   Start key for the region
     * @return Estimated end key
     */
    public byte[] getLastRegionKey(TableSplit inputSplit, byte[] startKey) {

        byte[] endKey = maxRowKey;

        if (startKey != null) {
            int keyLength = startKey.length;
            if (keyLength > 0) {
                endKey = new byte[keyLength];
                Arrays.fill(endKey, xFF);
            }
        }

        Log.info("Setting last key for HBase region to:" + endKey);

        return (endKey);
    }

}
