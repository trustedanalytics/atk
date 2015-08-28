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

package org.trustedanalytics.atk.giraph.algorithms.lda;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;
import org.trustedanalytics.atk.giraph.aggregators.VectorSumAggregator;
import org.trustedanalytics.atk.giraph.config.lda.LdaConfig;
import org.trustedanalytics.atk.giraph.config.lda.LdaConfiguration;
import org.trustedanalytics.atk.giraph.io.LdaEdgeData;
import org.trustedanalytics.atk.giraph.io.LdaMessage;
import org.trustedanalytics.atk.giraph.io.LdaVertexData;
import org.trustedanalytics.atk.giraph.io.LdaVertexId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

/**
 * CVB0 Latent Dirichlet Allocation for Topic Modelling
 * The algorithm presented in
 * Y.W. Teh, D. Newman, and M. Welling, A Collapsed Variational Bayesian
 * Inference Algorithm for Latent Dirichlet Allocation, NIPS 19, 2007.
 */
@Algorithm(
        name = "CVB0 Latent Dirichlet Allocation"
)
public class CVB0LDAComputation extends BasicComputation<LdaVertexId, LdaVertexData,
        LdaEdgeData, LdaMessage> {

    // TODO: looks like SUM_OCCURRENCE_COUNT might cause divide by zero error if word_count is zero or you had an unconnected vertex

    private LdaConfig config = null;

    /**
     * This value gets changed on convergence
     */
    private static final String CURRENT_MAX_SUPERSTEPS = "lda.maxSupersteps";

    /**
     * Aggregator name for number of document-vertices
     */
    private static String SUM_DOC_VERTEX_COUNT = "num_doc_vertices";
    /**
     * Aggregator name for number of word-vertices
     */
    private static String SUM_WORD_VERTEX_COUNT = "num_word_vertices";
    /**
     * Aggregator name for number of word occurrences
     */
    private static String SUM_OCCURRENCE_COUNT = "num_occurrences";
    /**
     * Aggregator name for sum of word-vertex values, the nk in LDA
     */
    private static String SUM_WORD_VERTEX_VALUE = "nk";
    /**
     * Aggregator name for max of delta at each super step
     */
    private static String SUM_COST = "sum_cost";
    /**
     * Aggregator name for max of delta at each super step
     */
    private static String MAX_DELTA = "max_delta";
    /**
     * Max delta value of previous super step for convergence monitoring
     */
    private static String PREV_MAX_DELTA = "prev_max_delta";
    /**
     * Map of conditional probability of topics given word
     */
    private Map<String, DenseVector> topicWordMap = new HashMap<>();

    /**
     * Number of words in vocabulary
     */
    private long numWords = 0;
    /**
     * Sum of word-vertex values
     */
    private Vector nk = null;


    @Override
    public void preSuperstep() {
        config = new LdaConfiguration(getConf()).ldaConfig();
        getConf().setLong(CURRENT_MAX_SUPERSTEPS, config.maxIterations());
        // Set custom parameters
        numWords = this.<LongWritable>getAggregatedValue(SUM_WORD_VERTEX_COUNT).get();
        nk = this.<VectorWritable>getAggregatedValue(SUM_WORD_VERTEX_VALUE).get().clone();
    }


    @Override
    public void compute(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex,
                        Iterable<LdaMessage> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initialize(vertex);
            vertex.voteToHalt();
            return;
        }

        // collect messages
        HashMap<LdaVertexId, Vector> map = new HashMap<LdaVertexId, Vector>();
        for (LdaMessage message : messages) {
            map.put(message.getVid().copy(), message.getVector());
        }

        // evaluate cost
        if (config.evaluationCost()) {
            evaluateCost(vertex, messages, map);
        }
        updateEdge(vertex, map);
        updateVertex(vertex);

        if (step < getConf().getLong(CURRENT_MAX_SUPERSTEPS, config.maxIterations())) {
            // send out messages
            LdaMessage newMessage = new LdaMessage(vertex.getId().copy(),
                    vertex.getValue().getLdaResult());
            sendMessageToAllEdges(vertex, newMessage);
        } else {
            // set conditional probability of topic given word
            setTopicGivenWord(vertex);

            // normalize vertex value, i.e., theta and phi in LDA, for final output
            normalizeVertex(vertex);
        }

        vertex.voteToHalt();
    }

    /**
     * Initialize vertex/edges, collect graph statistics and send out messages
     *
     * @param vertex of the graph
     */
    private void initialize(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex) {

        // initialize vertex vector, i.e., the theta for doc and phi for word in LDA
        double[] vertexValues = new double[config.numTopics()];
        vertex.getValue().setLdaResult(new DenseVector(vertexValues));

        // initialize edge vector, i.e., the gamma in LDA
        Random rand1 = new Random(vertex.getId().seed());
        long seed1 = rand1.nextInt();
        double maxDelta = 0d;
        double sumWeights = 0d;
        for (Edge<LdaVertexId, LdaEdgeData> edge : vertex.getMutableEdges()) {
            double weight = edge.getValue().getWordCount();

            // generate the random seed for this edge
            Random rand2 = new Random(edge.getTargetVertexId().seed());
            long seed2 = rand2.nextInt();
            long seed = seed1 + seed2;
            Random rand = new Random(seed);
            double[] edgeValues = new double[config.numTopics()];
            for (int i = 0; i < config.numTopics(); i++) {
                edgeValues[i] = rand.nextDouble();
            }
            Vector vector = new DenseVector(edgeValues);
            vector = vector.normalize(1d);
            edge.getValue().setVector(vector);
            // find the max delta among all edges
            double delta = vector.norm(1d) / config.numTopics();
            if (delta > maxDelta) {
                maxDelta = delta;
            }
            // the sum of weights from all edges
            sumWeights += weight;
        }
        // update vertex value
        updateVertex(vertex);
        // aggregate max delta value
        aggregate(MAX_DELTA, new DoubleWritable(maxDelta));

        // collect graph statistics
        if (vertex.getId().isDocument()) {
            aggregate(SUM_DOC_VERTEX_COUNT, new LongWritable(1));
        } else {
            aggregate(SUM_OCCURRENCE_COUNT, new DoubleWritable(sumWeights));
            aggregate(SUM_WORD_VERTEX_COUNT, new LongWritable(1));
        }

        // send out messages
        LdaMessage newMessage = new LdaMessage(vertex.getId().copy(), vertex.getValue().getLdaResult());
        sendMessageToAllEdges(vertex, newMessage);
    }

    /**
     * Update vertex value according to edge value
     *
     * @param vertex of the graph
     */
    private void updateVertex(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex) {
        Vector vector = vertex.getValue().getLdaResult().clone().assign(0d);
        for (Edge<LdaVertexId, LdaEdgeData> edge : vertex.getEdges()) {
            double weight = edge.getValue().getWordCount();
            Vector gamma = edge.getValue().getVector();
            vector = vector.plus(gamma.times(weight));
        }
        vertex.getValue().setLdaResult(vector);

        if (vertex.getId().isWord()) {
            aggregate(SUM_WORD_VERTEX_VALUE, new VectorWritable(vector));
        }
    }

    /**
     * Update edge value according to vertex and messages
     *
     * @param vertex of the graph
     * @param map    of type HashMap
     */
    private void updateEdge(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex, HashMap<LdaVertexId, Vector> map) {
        Vector vector = vertex.getValue().getLdaResult();

        double maxDelta = 0d;
        for (Edge<LdaVertexId, LdaEdgeData> edge : vertex.getMutableEdges()) {
            Vector gamma = edge.getValue().getVector();
            LdaVertexId id = edge.getTargetVertexId();
            if (map.containsKey(id)) {
                Vector otherVector = map.get(id);
                Vector newGamma = null;
                if (vertex.getId().isDocument()) {
                    newGamma = vector.minus(gamma).plus(config.alpha()).times(otherVector.minus(gamma).plus(config.beta()))
                            .times(nk.minus(gamma).plus(numWords * config.beta()).assign(Functions.INV));
                } else {
                    newGamma = vector.minus(gamma).plus(config.beta()).times(otherVector.minus(gamma).plus(config.alpha()))
                            .times(nk.minus(gamma).plus(numWords * config.beta()).assign(Functions.INV));
                }
                newGamma = newGamma.normalize(1d);
                double delta = gamma.minus(newGamma).norm(1d) / config.numTopics();
                if (delta > maxDelta) {
                    maxDelta = delta;
                }
                // update edge vector
                edge.getValue().setVector(newGamma);
            } else {
                // this happens when you don't have your Vertex Id's being setup correctly
                throw new IllegalArgumentException(String.format("Vertex ID %s: A message is mis-matched.", vertex.getId()));
            }
        }
        aggregate(MAX_DELTA, new DoubleWritable(maxDelta));
    }

    /**
     * Normalize vertex value
     *
     * @param vertex of the graph
     */
    private void normalizeVertex(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex) {
        Vector vector = vertex.getValue().getLdaResult();
        if (vertex.getId().isDocument()) {
            vector = vector.plus(config.alpha()).normalize(1d);
        } else {
            vector = vector.plus(config.beta()).times(nk.plus(numWords * config.beta()).assign(Functions.INV));
        }
        // update vertex value
        vertex.getValue().setLdaResult(vector);
    }

    /**
     * Evaluate cost according to vertex and messages
     *
     * @param vertex   of the graph
     * @param messages of type iterable
     * @param map      of type HashMap
     */
    private void evaluateCost(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex,
                              Iterable<LdaMessage> messages, HashMap<LdaVertexId, Vector> map) {

        if (vertex.getId().isDocument()) {
            return;
        }
        Vector vector = vertex.getValue().getLdaResult();
        vector = vector.plus(config.beta()).times(nk.plus(numWords * config.beta()).assign(Functions.INV));

        double cost = 0d;
        for (Edge<LdaVertexId, LdaEdgeData> edge : vertex.getEdges()) {
            double weight = edge.getValue().getWordCount();
            LdaVertexId id = edge.getTargetVertexId();
            if (map.containsKey(id)) {
                Vector otherVector = map.get(id);
                otherVector = otherVector.plus(config.alpha()).normalize(1d);
                cost -= weight * Math.log(vector.dot(otherVector));
            } else {
                throw new IllegalArgumentException(String.format("Vertex ID %s: A message is mis-matched",
                        vertex.getId().getValue()));
            }
        }
        aggregate(SUM_COST, new DoubleWritable(cost));
    }

    /**
     * Compute the conditional probability of topics given word
     *
     * Each element of the vector contains the conditional probability of topic k given word
     * @param vertex of the graph
     */
    private void setTopicGivenWord(Vertex<LdaVertexId, LdaVertexData, LdaEdgeData> vertex) {
        if (vertex.getId().isDocument()) {
            return;
        }

        double wordCount = 0d;
        Vector vector = vertex.getValue().getLdaResult().clone();
        for (Edge<LdaVertexId, LdaEdgeData> edge : vertex.getMutableEdges()) {
            wordCount += edge.getValue().getWordCount();
        }

        if (wordCount > 0d) {
            vector = vector.divide(wordCount);
        }
        else {
            vector = vector.assign(0d);
        }

        vertex.getValue().setTopicGivenWord(vector);
    }

    /**
     * Master compute associated with {@link CVB0LDAComputation}. It registers required aggregators.
     */
    public static class CVB0LDAMasterCompute extends DefaultMasterCompute {

        private LdaConfig config = null;

        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            config = new LdaConfiguration(getConf()).ldaConfig();
            registerPersistentAggregator(SUM_DOC_VERTEX_COUNT, LongSumAggregator.class);
            registerPersistentAggregator(SUM_WORD_VERTEX_COUNT, LongSumAggregator.class);
            registerPersistentAggregator(SUM_OCCURRENCE_COUNT, DoubleSumAggregator.class);
            registerAggregator(SUM_WORD_VERTEX_VALUE, VectorSumAggregator.class);
            registerAggregator(MAX_DELTA, DoubleMaxAggregator.class);
            if (config.evaluationCost()) {
                registerAggregator(SUM_COST, DoubleSumAggregator.class);
            }
        }

        @Override
        public void compute() {
            long step = getSuperstep();
            if (step <= 0) {
                return;
            }

            // store number of edges for graph statistics
            if (step != 1) {
                // evaluate convergence condition
                float prevMaxDelta = getConf().getFloat(PREV_MAX_DELTA, 0f);
                if (config.evaluationCost()) {
                    DoubleWritable sumCost = getAggregatedValue(SUM_COST);
                    double numOccurrances = this.<DoubleWritable>getAggregatedValue(SUM_OCCURRENCE_COUNT).get();
                    double cost = sumCost.get() / numOccurrances;
                    sumCost.set(cost);
                }
                double maxDelta = this.<DoubleWritable>getAggregatedValue(MAX_DELTA).get();
                if (Math.abs(prevMaxDelta - maxDelta) < config.convergenceThreshold()) {
                    getConf().setLong(CURRENT_MAX_SUPERSTEPS, step);
                }
                getConf().setFloat(PREV_MAX_DELTA, (float) maxDelta);
            }
        }
    }

    /**
     * This is an aggregator writer for LDA, which after each super step will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class CVB0LDAAggregatorWriter implements AggregatorWriter {
        /**
         * Name of the file we wrote to
         */
        private static String FILENAME;
        /**
         * Saved output stream to write to
         */
        private FSDataOutputStream output;
        /**
         * Last superstep number
         */
        private long lastStep = -1L;

        /**
         * Configuration
         */
        private ImmutableClassesGiraphConfiguration conf;

        @Override
        public void setConf(ImmutableClassesGiraphConfiguration conf) {
            this.conf = conf;
        }

        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }


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
            FILENAME = "lda-learning-report_" + applicationAttempt;
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

            LdaConfig ldaConfig = new LdaConfiguration(getConf()).ldaConfig();

            if (realStep == 0) {
                // output graph statistics
                long numDocVertices = Long.parseLong(map.get(SUM_DOC_VERTEX_COUNT));
                long numWordVertices = Long.parseLong(map.get(SUM_WORD_VERTEX_COUNT));
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d (doc: %d, word: %d)%n",
                        numDocVertices + numWordVertices, numDocVertices, numWordVertices));
                output.writeBytes(String.format("Number of edges: %d%n",
                        GiraphStats.getInstance().getEdges().getValue()));
                output.writeBytes("\n");
                // output LDA configuration
                output.writeBytes("======LDA Configuration======\n");
                output.writeBytes(String.format("numTopics: %d%n", ldaConfig.numTopics()));
                output.writeBytes(String.format("alpha: %f%n", ldaConfig.alpha()));
                output.writeBytes(String.format("beta: %f%n", ldaConfig.beta()));
                output.writeBytes(String.format("convergenceThreshold: %f%n", ldaConfig.convergenceThreshold()));
                output.writeBytes(String.format("maxIterations: %d%n", ldaConfig.maxIterations()));
                output.writeBytes(String.format("evaluateCost: %b%n", ldaConfig.evaluationCost()));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (realStep > 0) {
                // output learning progress
                output.writeBytes(String.format("iteration = %d%c", realStep, '\t'));
                if (ldaConfig.evaluationCost()) {
                    double cost = Double.parseDouble(map.get(SUM_COST));
                    output.writeBytes(String.format("cost = %f%c", cost, '\t'));
                }
                double maxDelta = Double.parseDouble(map.get(MAX_DELTA));
                output.writeBytes(String.format("maxDelta = %f%n", maxDelta));
            }
            output.flush();
            lastStep = superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

}
