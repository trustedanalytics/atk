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

package org.trustedanalytics.atk.giraph.io.titan.formats;

import org.trustedanalytics.atk.giraph.io.titan.GiraphToTitanGraphFactory;
import org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.hadoop.formats.util.TitanInputFormat;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

/**
 * Abstract class that uses TitanHBaseInputFormat to read Titan/Hadoop (i.e., Faunus) vertices from HBase.
 * <p/>
 * Subclasses can configure TitanHBaseInputFormat by using conf.set
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public abstract class TitanVertexInputFormat<I extends WritableComparable, V extends Writable, E extends Writable>
        extends VertexInputFormat<I, V, E> {

    protected TitanInputFormat titanInputFormat = null;


    /**
     * Check that input configuration is valid.
     *
     * @param conf Configuration
     */
    public void checkInputSpecs(Configuration conf) {
    }


    /**
     * Set up Titan/HBase configuration for Giraph
     *
     * @param conf :Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        super.setConf(conf);
        GiraphTitanUtils.sanityCheckInputParameters(conf);
        GiraphToTitanGraphFactory.addFaunusInputConfiguration(conf);

        this.titanInputFormat = TitanInputFormatFactory.getTitanInputFormat(conf);
        this.titanInputFormat.setConf(conf);
    }

    /**
     * Get input splits from Titan/HBase input format
     *
     * @param context           task context
     * @param minSplitCountHint minimal split count
     * @return List<InputSplit> list of input splits
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException,
            InterruptedException {

        return this.titanInputFormat.getSplits(context);
    }

}
