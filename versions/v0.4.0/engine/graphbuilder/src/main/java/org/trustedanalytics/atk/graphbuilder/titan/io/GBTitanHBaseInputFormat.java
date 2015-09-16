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

import com.thinkaurelius.titan.hadoop.formats.titan_054.hbase.CachedTitanHBaseInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * HBase's default TableInputFormat class assigns a single mapper per region,
 * This splitting policy degrades performance when the size of individual regions
 * is large. This class increases the number of splits as follows:
 * <p/>
 * Total splits = max(hbase.mapreduce.regions.splits, existing number of regions)
 *
 * @see org.apache.hadoop.hbase.mapreduce.TableInputFormat
 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
 */
public class GBTitanHBaseInputFormat extends CachedTitanHBaseInputFormat {


    public static final String NUM_REGION_SPLITS = "hbase.mapreduce.regions.splits";

    private final Log LOG = LogFactory.getLog(TableInputFormat.class);

    /**
     * Gets the input splits using a uniform-splitting policy.
     *
     * @param context Current job context.
     * @return List of input splits.
     * @throws java.io.IOException when creating the list of splits fails.
     * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {

        List<InputSplit> splits = getInitialRegionSplits(context);
        int requestedSplitCount = getRequestedSplitCount(context, splits);

        if (splits != null) {

            HBaseUniformSplitter uniformSplitter = new HBaseUniformSplitter(splits);
            splits = uniformSplitter.createInputSplits(requestedSplitCount);

            LOG.info("Generated " + splits.size() + " input splits for HBase table");
        }


        return splits;
    }

    /**
     * Get initial region splits. Default policy is one split per HBase region.
     *
     * @param context Job Context
     * @return Initial input splits for HBase table
     * @throws IOException
     */
    protected List<InputSplit> getInitialRegionSplits(JobContext context) throws IOException, InterruptedException {
        // This method helps initialize the mocks for unit tests
        return (super.getSplits(context));
    }

    /**
     * Get the requested input split count from the job configuration
     *
     * @param context Current job context which contains configuration.
     * @param splits  List of input splits.
     * @return Requested split count
     */
    protected int getRequestedSplitCount(JobContext context, List<InputSplit> splits) {
        Configuration config = context.getConfiguration();

        int initialSplitCount = splits.size();
        int requestedSplitCount = config.getInt(NUM_REGION_SPLITS, initialSplitCount);

        LOG.info("Set requested input splits for HBase table to: " + requestedSplitCount);
        return requestedSplitCount;
    }
}
