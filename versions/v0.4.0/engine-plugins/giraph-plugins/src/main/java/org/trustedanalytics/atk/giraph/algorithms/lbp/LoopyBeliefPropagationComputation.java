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

package org.trustedanalytics.atk.giraph.algorithms.lbp;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.counters.GiraphStats;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.trustedanalytics.atk.giraph.config.lbp.LoopyBeliefPropagationConfig;
import org.trustedanalytics.atk.giraph.config.lbp.LoopyBeliefPropagationConfiguration;
import org.trustedanalytics.atk.giraph.io.IdWithVectorMessage;
import org.trustedanalytics.atk.giraph.io.VertexData4LBPWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4LBPWritable.VertexType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Loopy belief propagation on MRF
 */
@Algorithm(
    name = "Loopy belief propagation on MRF"
)
public class LoopyBeliefPropagationComputation extends BasicComputation<LongWritable, VertexData4LBPWritable,
    DoubleWritable, IdWithVectorMessage> {

    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "lbp.maxSupersteps";

    /** Custom argument for the Ising smoothing parameter */
    public static final String SMOOTHING = "lbp.smoothing";

    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "lbp.convergenceThreshold";

    /**
     * Custom argument for the anchor threshold [0, 1]
     * the vertices whose normalized prior values are greater than
     * this threshold will not be updated.
     */
    public static final String ANCHOR_THRESHOLD = "lbp.anchorThreshold";

    /**
     * Custom argument for ignoring vertex type or not (default: false)
     * If true, all vertex will be treated as training data
     */
    public static final String IGNORE_VERTEX_TYPE = "lbp.ignoreVertexType";

    /** Custom argument for using max-product or not (default: false) */
    public static final String MAX_PRODUCT = "lbp.maxProduct";

    /** Custom argument for power coefficient for power edge potential (default: 0) */
    public static final String POWER = "lbp.power";

    /** Constant value for minimum prior value */
    public static final double MIN_PRIOR_VALUE = 0.001d;

    /** Aggregator name for sum of delta on training data */
    private static final String SUM_TRAIN_DELTA = "train_delta";

    /** Aggregator name for sum of delta on validation data */
    private static final String SUM_VALIDATE_DELTA = "validate_delta";

    /** Aggregator name for sum of delta on test data */
    private static final String SUM_TEST_DELTA = "test_delta";

    /** Number of training vertices */
    private static final String SUM_TRAIN_VERTICES = "num_train_vertices";

    /** Number of validation vertices */
    private static final String SUM_VALIDATE_VERTICES = "num_validate_vertices";

    /** Number of test vertices */
    private static final String SUM_TEST_VERTICES = "num_test_vertices";

    /** Average delta value on validation data of previous super step for convergence monitoring */
    private static final String PREV_AVG_DELTA = "prev_avg_delta";

    /** Number of super steps */
    private int maxSupersteps = 10;

    /** The Ising smoothing parameter */
    private float smoothing = 2f;

    /** The anchor threshold controlling if update a vertex */
    private float anchorThreshold = 1f;

    /** Whether ignore vertex type or not */
    private boolean ignoreVertexType = false;

    /** Whether use max-product or not */
    private boolean maxProduct = false;

    /** Power coefficient of the power edge potential */
    private float power = 0f;

    @Override
    public void preSuperstep() {
        LoopyBeliefPropagationConfig config = new LoopyBeliefPropagationConfiguration(getConf()).getConfig();

        maxSupersteps = config.maxIterations();
        smoothing = config.smoothing();
        anchorThreshold = (float) Math.log(config.anchorThreshold());
        ignoreVertexType = config.ignoreVertexType();
        maxProduct = config.maxProduct();
        power = config.power();
    }

    /**
     * Edge potential function
     *
     * @param delta of type double
     * @param weight of type double
     * @return potential of type double
     */
    private double edgePotential(double delta, double weight) {
        double potential = 0d;
        if (delta > 1e-6 || power > 0) {
            potential = - Math.pow(delta, power) * weight * smoothing;
        }
        return potential;
    }

    /**
     * Initialize vertex
     *
     * @param vertex of the graph
     */
    private void initializeVertex(Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex) {
        // normalize prior and posterior
        Vector prior = vertex.getValue().getPriorVector();
        Vector posterior = vertex.getValue().getPosteriorVector();
        int nStates = prior.size();
        double sum = 0d;
        for (int i = 0; i < nStates; i++) {
            double v = prior.getQuick(i);
            if (v < 0d) {
                throw new IllegalArgumentException("Vertex ID: " + vertex.getId() + " has negative prior value.");
            } else if (v < MIN_PRIOR_VALUE) {
                v = MIN_PRIOR_VALUE;
                prior.setQuick(i, v);
            }
            sum += v;
        }
        for (int i = 0; i < nStates; i++) {
            posterior.setQuick(i, prior.getQuick(i) / sum);
            prior.setQuick(i, Math.log(posterior.getQuick(i)));
        }
        // collect graph statistics
        VertexType vt = vertex.getValue().getType();
        vt = ignoreVertexType ? VertexType.TRAIN : vt;
        switch (vt) {
        case TRAIN:
            aggregate(SUM_TRAIN_VERTICES, new LongWritable(1));
            break;
        case VALIDATE:
            aggregate(SUM_VALIDATE_VERTICES, new LongWritable(1));
            break;
        case TEST:
            aggregate(SUM_TEST_VERTICES, new LongWritable(1));
            break;
        default:
            throw new IllegalArgumentException("Unknown vertex type: " + vt.toString());
        }
        // if it's not a training vertex, use uniform posterior and don't send out messages
        if (vt != VertexType.TRAIN) {
            posterior.assign(1.0 / nStates);
            return;
        }
        // calculate messages
        IdWithVectorMessage newMessage = new IdWithVectorMessage();
        newMessage.setData(vertex.getId().get());
        // calculate initial belief
        Vector belief = prior.clone();
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            double weight = edge.getValue().get();
            if (weight <= 0d) {
                throw new IllegalArgumentException("Vertex ID: " + vertex.getId() +
                    " has an edge with negative or zero weight value " + weight);
            }
            for (int i = 0; i < nStates; i++) {
                sum = 0d;
                for (int j = 0; j < nStates; j++) {
                    double msg = Math.exp(prior.getQuick(j) + edgePotential(Math.abs(i - j) /
                        (double) (nStates - 1), weight));
                    if (maxProduct) {
                        sum = sum > msg ? sum : msg;
                    } else {
                        sum += msg;
                    }
                }
                belief.setQuick(i, sum > 0d ? Math.log(sum) : Double.MIN_VALUE);
            }
            belief = belief.plus(-belief.maxValue());
            // send out messages
            newMessage.setVector(belief);
            sendMessage(edge.getTargetVertexId(), newMessage);
        }
    }

    @Override
    public void compute(Vertex<LongWritable, VertexData4LBPWritable, DoubleWritable> vertex,
        Iterable<IdWithVectorMessage> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initializeVertex(vertex);
            return;
        }

        // collect messages sent to this vertex
        HashMap<Long, Vector> map = new HashMap<Long, Vector>();
        for (IdWithVectorMessage message : messages) {
            map.put(message.getData(), message.getVector());
        }

        // update posterior according to prior and messages
        VertexData4LBPWritable vertexValue = vertex.getValue();
        VertexType vt = vertexValue.getType();
        vt = ignoreVertexType ? VertexType.TRAIN : vt;
        Vector prior = vertexValue.getPriorVector();
        double nStates = prior.size();
        if (vt != VertexType.TRAIN) {
            // assign a uniform prior for validate/test vertex
            prior = prior.clone().assign(Math.log(1.0 / nStates));
        }
        // sum of prior and messages
        Vector sumPosterior = prior;
        for (IdWithVectorMessage message : messages) {
            sumPosterior = sumPosterior.plus(message.getVector());
        }
        sumPosterior = sumPosterior.plus(-sumPosterior.maxValue());
        // update posterior if this isn't an anchor vertex
        if (prior.maxValue() < anchorThreshold) {
            // normalize posterior
            Vector posterior = sumPosterior.clone().assign(Functions.EXP);
            posterior = posterior.normalize(1d);
            Vector oldPosterior = vertexValue.getPosteriorVector();
            double delta = posterior.minus(oldPosterior).norm(1d);
            // aggregate deltas
            switch(vt) {
            case TRAIN:
                aggregate(SUM_TRAIN_DELTA, new DoubleWritable(delta));
                break;
            case VALIDATE:
                aggregate(SUM_VALIDATE_DELTA, new DoubleWritable(delta));
                break;
            case TEST:
                aggregate(SUM_TEST_DELTA, new DoubleWritable(delta));
                break;
            default:
                throw new IllegalArgumentException("Unknown vertex type: " + vt.toString());
            }
            // update posterior
            vertexValue.setPosteriorVector(posterior);
        }

        if (step < maxSupersteps) {
            // if it's not a training vertex, don't send out messages
            if (vt != VertexType.TRAIN) {
                return;
            }
            IdWithVectorMessage newMessage = new IdWithVectorMessage();
            newMessage.setData(vertex.getId().get());
            // update belief
            Vector belief = prior.clone();
            for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                double weight = edge.getValue().get();
                long id = edge.getTargetVertexId().get();
                Vector tempVector = sumPosterior;
                if (map.containsKey(id)) {
                    tempVector = sumPosterior.minus(map.get(id));
                }
                for (int i = 0; i < nStates; i++) {
                    double sum = 0d;
                    for (int j = 0; j < nStates; j++) {
                        double msg = Math.exp(tempVector.getQuick(j) + edgePotential(Math.abs(i - j) / (nStates - 1), weight));
                        if (maxProduct) {
                            sum = sum > msg ? sum : msg;
                        } else {
                            sum += msg;
                        }
                    }
                    belief.setQuick(i, sum > 0d ? Math.log(sum) : Double.MIN_VALUE);
                }
                belief = belief.plus(-belief.maxValue());
                newMessage.setVector(belief);
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        } else {
            // convert prior back to regular scale before output
            prior = vertexValue.getPriorVector();
            prior = prior.assign(Functions.EXP);
            vertexValue.setPriorVector(prior);
            vertex.voteToHalt();
        }
    }

    /**
     * Master compute associated with {@link LoopyBeliefPropagationComputation}. It registers required aggregators.
     */
    public static class LoopyBeliefPropagationMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerPersistentAggregator(SUM_TRAIN_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_VALIDATE_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TEST_VERTICES, LongSumAggregator.class);
            registerAggregator(SUM_TRAIN_DELTA, DoubleSumAggregator.class);
            registerAggregator(SUM_VALIDATE_DELTA, DoubleSumAggregator.class);
            registerAggregator(SUM_TEST_DELTA, DoubleSumAggregator.class);
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            if (step <= 0) {
                return;
            }

            if (step != 1) {
                // calculate average delta on training data
                DoubleWritable sumTrainDelta = getAggregatedValue(SUM_TRAIN_DELTA);
                long numTrainVertices = this.<LongWritable>getAggregatedValue(SUM_TRAIN_VERTICES).get();
                double avgTrainDelta = 0d;
                if (numTrainVertices > 0) {
                    avgTrainDelta = sumTrainDelta.get() / numTrainVertices;
                }
                sumTrainDelta.set(avgTrainDelta);
                // calculate average delta on test data
                DoubleWritable sumTestDelta = getAggregatedValue(SUM_TEST_DELTA);
                long numTestVertices = this.<LongWritable>getAggregatedValue(SUM_TEST_VERTICES).get();
                double avgTestDelta = 0d;
                if (numTestVertices > 0) {
                    avgTestDelta = sumTestDelta.get() / numTestVertices;
                }
                sumTestDelta.set(avgTestDelta);
                // calculate average delta on validation data
                DoubleWritable sumValidateDelta = getAggregatedValue(SUM_VALIDATE_DELTA);
                long numValidateVertices = this.<LongWritable>getAggregatedValue(SUM_VALIDATE_VERTICES).get();
                double avgValidateDelta = 0d;
                if (numValidateVertices > 0) {
                    avgValidateDelta = sumValidateDelta.get() / numValidateVertices;
                }
                sumValidateDelta.set(avgValidateDelta);
                // evaluate convergence condition
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float prevAvgDelta = getConf().getFloat(PREV_AVG_DELTA, 0f);
                if (Math.abs(prevAvgDelta - avgValidateDelta) < threshold) {
                    getConf().setInt(MAX_SUPERSTEPS, (int) step);
                }
                getConf().setFloat(PREV_AVG_DELTA, (float) avgValidateDelta);
            }
        }
    }

    /**
     * This is an aggregator writer for lbp, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class LoopyBeliefPropagationAggregatorWriter implements AggregatorWriter {

        /** Configuration */
        private ImmutableClassesGiraphConfiguration conf;

        @Override
        public void setConf(ImmutableClassesGiraphConfiguration conf) {
            this.conf = conf;
        }

        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }

        /** Name of the file we wrote to */
        private static String FILENAME;

        /** Saved output stream to write to */
        private FSDataOutputStream output;

        /** Last superstep number */
        private long lastStep = -1L;

        public static String getFilename() {
            return FILENAME;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void initialize(Context context, long applicationAttempt) throws IOException {
            setFilename(applicationAttempt);
            String outputDir = context.getConfiguration().get("mapred.output.dir");
            Path p = new Path(outputDir + "/" + FILENAME);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
            output = fs.create(p, true);
        }

        /**
         * Set filename written to
         *
         * @param applicationAttempt of type long
         */
        private static void setFilename(long applicationAttempt) {
            FILENAME = "lbp-learning-report";
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            long realStep = lastStep;

            // collect aggregator data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            if (realStep == 0) {
                // output graph statistics
                long numTrainVertices = Long.parseLong(map.get(SUM_TRAIN_VERTICES));
                long numValidateVertices = Long.parseLong(map.get(SUM_VALIDATE_VERTICES));
                long numTestVertices = Long.parseLong(map.get(SUM_TEST_VERTICES));
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d (train: %d, validate: %d, test: %d)%n",
                    numTrainVertices + numValidateVertices + numTestVertices,
                    numTrainVertices, numValidateVertices, numTestVertices));
                output.writeBytes(String.format("Number of edges: %d%n",
                    GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");
                // output LBP configuration
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 10);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                float anchorThreshold = getConf().getFloat(ANCHOR_THRESHOLD, 1f);
                float smoothing = getConf().getFloat(SMOOTHING, 2f);
                boolean ignoreVertexType = getConf().getBoolean(IGNORE_VERTEX_TYPE, false);
                boolean maxProduct = getConf().getBoolean(MAX_PRODUCT, false);
                float power = getConf().getFloat(POWER, 0f);
                output.writeBytes("======LBP Configuration======\n");
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("anchorThreshold: %f%n", anchorThreshold));
                output.writeBytes(String.format("smoothing: %f%n", smoothing));
                output.writeBytes(String.format("ignoreVertexType: %b%n", ignoreVertexType));
                output.writeBytes(String.format("maxProduct: %b%n", maxProduct));
                output.writeBytes(String.format("power: %f%n", power));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (realStep > 0) {
                // output learning progress
                double avgTrainDelta = Double.parseDouble(map.get(SUM_TRAIN_DELTA));
                double avgValidateDelta = Double.parseDouble(map.get(SUM_VALIDATE_DELTA));
                double avgTestDelta = Double.parseDouble(map.get(SUM_TEST_DELTA));
                output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                output.writeBytes(String.format("avgTrainDelta = %f%c", avgTrainDelta, '\t'));
                output.writeBytes(String.format("avgValidateDelta = %f%c", avgValidateDelta, '\t'));
                output.writeBytes(String.format("avgTestDelta = %f%n", avgTestDelta));
            }
            output.flush();
            lastStep =  superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

}
