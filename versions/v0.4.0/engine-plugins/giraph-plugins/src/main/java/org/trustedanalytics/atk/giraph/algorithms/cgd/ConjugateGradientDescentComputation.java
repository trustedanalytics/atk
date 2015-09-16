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

package org.trustedanalytics.atk.giraph.algorithms.cgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
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
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfig;
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfiguration;
import org.trustedanalytics.atk.giraph.io.CFVertexId;
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable;
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable.EdgeType;
import org.trustedanalytics.atk.giraph.io.MessageData4CFWritable;
import org.trustedanalytics.atk.giraph.io.VertexData4CFWritable.VertexType;
import org.trustedanalytics.atk.giraph.io.VertexData4CGDWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

/**
 * Conjugate Gradient Descent (CGD) with Bias for collaborative filtering
 * CGD implementation of the algorithm presented in
 * Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
 * Filtering Model. In ACM KDD 2008. (Equation 5)
 */
@Algorithm(
    name = "Conjugate Gradient Descent (CGD) with Bias"
)
public class ConjugateGradientDescentComputation extends BasicComputation<CFVertexId, VertexData4CGDWritable,
    EdgeData4CFWritable, MessageData4CFWritable> {
    /** Custom argument for number of super steps */
    public static final String MAX_SUPERSTEPS = "cgd.maxSupersteps";
    /** Custom argument for number of CGD iterations in each super step */
    public static final String NUM_CGD_ITERS = "cgd.numCGDIters";
    /** Custom argument for feature dimension */
    public static final String FEATURE_DIMENSION = "cgd.featureDimension";
    /**
     * Custom argument for regularization parameter: lambda
     * f = L2_error + lambda*Tikhonov_regularization
     */
    public static final String LAMBDA = "cgd.lambda";
    /** Custom argument to turn on/off bias */
    public static final String BIAS_ON = "cgd.biasOn";
    /** Custom argument for the convergence threshold */
    public static final String CONVERGENCE_THRESHOLD = "cgd.convergenceThreshold";
    /** Custom argument for checking bi-directional edge or not (default: false) */
    public static final String BIDIRECTIONAL_CHECK = "cgd.bidirectionalCheck";
    /** Custom argument for maximum edge weight value */
    public static final String MAX_VAL = "cgd.maxVal";
    /** Custom argument for minimum edge weight value */
    public static final String MIN_VAL = "cgd.minVal";
    /**
     * Custom argument for learning curve output interval (default: every iteration)
     * Since each CGD iteration is composed by 2 super steps, one iteration
     * means two super steps.
     * */
    public static final String LEARNING_CURVE_OUTPUT_INTERVAL = "cgd.learningCurveOutputInterval";

    /** Aggregator name for sum of cost on training data */
    private static String SUM_TRAIN_COST = "train_cost";
    /** Aggregator name for sum of L2 error on validate data */
    private static String SUM_VALIDATE_ERROR = "validate_rmse";
    /** Aggregator name for sum of L2 error on test data */
    private static String SUM_TEST_ERROR = "test_rmse";
    /** Aggregator name for number of left vertices */
    private static String SUM_LEFT_VERTICES = "num_left_vertices";
    /** Aggregator name for number of right vertices */
    private static String SUM_RIGHT_VERTICES = "num_right_vertices";
    /** Aggregator name for number of validate edges */
    private static String SUM_TRAIN_EDGES = "num_train_edges";
    /** Aggregator name for number of validate edges */
    private static String SUM_VALIDATE_EDGES = "num_validate_edges";
    /** Aggregator name for number of test edges */
    private static String SUM_TEST_EDGES = "num_test_edges";
    /** RMSE value of previous iteration for convergence monitoring */
    private static String PREV_RMSE = "prev_rmse";

    /** Number of super steps */
    private int maxSupersteps = 20;
    /** Number of CGD iterations in each super step */
    private int numCGDIters = 5;
    /** Feature dimension */
    private int featureDimension = 20;
    /** The regularization parameter */
    private float lambda = 0f;
    /** Bias on/off switch */
    private boolean biasOn = false;
    /** Maximum edge weight value */
    private float maxVal = Float.POSITIVE_INFINITY;
    /** Minimum edge weight value */
    private float minVal = Float.NEGATIVE_INFINITY;
    /** Iteration interval to output learning curve */
    private int learningCurveOutputInterval = 1;

    @Override
    public void preSuperstep() {

        CollaborativeFilteringConfig config = new CollaborativeFilteringConfiguration(getConf()).getConfig();
        maxSupersteps = config.maxIterations();
        featureDimension = config.numFactors();
        lambda = config.lambda();
        biasOn = config.biasOn();
        maxVal = config.maxValue();
        minVal = config.minValue();
        learningCurveOutputInterval = config.learningCurveInterval();
        numCGDIters = config.cgdIterations();
    }

    /**
     * Initialize vertex, collect graph statistics and send out messages
     *
     * @param vertex of the graph
     */
    private void initialize(Vertex<CFVertexId, VertexData4CGDWritable, EdgeData4CFWritable> vertex) {
        // initialize vertex data: bias, vector, gradient, conjugate
        vertex.getValue().setBias(0d);
        vertex.getValue().setType(vertex.getId().isUser() ? VertexType.User: VertexType.Item);

        double sum = 0d;
        int numTrain = 0;
        for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
            EdgeType et = edge.getValue().getType();
            if (et == EdgeType.TRAIN) {
                double weight = edge.getValue().getWeight();
                if (weight < minVal || weight > maxVal) {
                    throw new IllegalArgumentException(String.format("Vertex ID: %s has an edge with weight value " +
                        "out of the range of [%f, %f].", vertex.getId().getValue(), minVal, maxVal));
                }
                sum += weight;
                numTrain++;
            }
        }
        Random rand = new Random(vertex.getId().seed());
        double[] values = new double[featureDimension];
        values[0] = 0d;
        if (numTrain > 0) {
            values[0] = sum / numTrain;
        }
        for (int i = 1; i < featureDimension; i++) {
            values[i] = rand.nextDouble() * values[0];
        }
        Vector value = new DenseVector(values);
        vertex.getValue().setVector(value);
        vertex.getValue().setGradient(value.clone().assign(0d));
        vertex.getValue().setConjugate(value.clone().assign(0d));

        // collect graph statistics and send out messages
        VertexType vt = vertex.getValue().getType();
        switch (vt) {
        case User:
            aggregate(SUM_LEFT_VERTICES, new LongWritable(1));
            break;
        case Item:
            aggregate(SUM_RIGHT_VERTICES, new LongWritable(1));
            long numTrainEdges = 0L;
            long numValidateEdges = 0L;
            long numTestEdges = 0L;
            for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
                EdgeType et = edge.getValue().getType();
                switch (et) {
                case TRAIN:
                    numTrainEdges++;
                    break;
                case VALIDATE:
                    numValidateEdges++;
                    break;
                case TEST:
                    numTestEdges++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown recognized edge type: " + et.toString());
                }
                // send out messages
                MessageData4CFWritable newMessage = new MessageData4CFWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
            if (numTrainEdges > 0) {
                aggregate(SUM_TRAIN_EDGES, new LongWritable(numTrainEdges));
            }
            if (numValidateEdges > 0) {
                aggregate(SUM_VALIDATE_EDGES, new LongWritable(numValidateEdges));
            }
            if (numTestEdges > 0) {
                aggregate(SUM_TEST_EDGES, new LongWritable(numTestEdges));
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown recognized vertex type: " + vt.toString());
        }
    }

    /**
     * Compute gradient
     *
     * @param bias of type double
     * @param value of type Vector
     * @param messages of type Iterable
     * @return gradient of type Vector
     */
    private Vector computeGradient(double bias, Vector value, Iterable<MessageData4CFWritable> messages) {
        Vector xr = value.clone().assign(0d);
        int numTrain = 0;
        for (MessageData4CFWritable message : messages) {
            EdgeType et = message.getType();
            if (et == EdgeType.TRAIN) {
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = bias + otherBias + value.dot(vector);
                double e = predict - weight;
                xr = xr.plus(vector.times(e));
                numTrain++;
            }
        }
        Vector gradient = value.clone().assign(0d);
        if (numTrain > 0) {
            gradient = xr.divide(numTrain).plus(value.times(lambda));
        }
        return gradient;
    }

    /**
     * Compute alpha
     *
     * @param gradient of type Vector
     * @param conjugate of type Vector
     * @param messages of type Iterable
     * @return alpha of type double
     */
    private double computeAlpha(Vector gradient, Vector conjugate,
        Iterable<MessageData4CFWritable> messages) {
        double alpha = 0d;
        if (conjugate.norm(1d) == 0d) {
            return alpha;
        }
        double predictSquared = 0d;
        int numTrain = 0;
        for (MessageData4CFWritable message : messages) {
            EdgeType et = message.getType();
            if (et == EdgeType.TRAIN) {
                Vector vector = message.getVector();
                double predict = conjugate.dot(vector);
                predictSquared += predict * predict;
                numTrain++;
            }
        }
        if (numTrain > 0) {
            alpha = - gradient.dot(conjugate) / (predictSquared / numTrain + lambda * conjugate.dot(conjugate));
        }
        return alpha;
    }

    /**
     * Compute beta according to Hestenes-Stiefel formula
     *
     * @param gradient of type Vector
     * @param conjugate of type Vector
     * @param gradientNext of type Vector
     * @return beta of type double
     */
    private double computeBeta(Vector gradient, Vector conjugate, Vector gradientNext) {
        double beta = 0d;
        if (conjugate.norm(1d) == 0d) {
            return beta;
        }
        Vector deltaVector = gradientNext.minus(gradient);
        beta = - gradientNext.dot(deltaVector) / conjugate.dot(deltaVector);
        return beta;
    }

    /**
     * Compute bias
     *
     * @param value of type Vector
     * @param messages of type Iterable
     * @return bias of type double
     */
    private double computeBias(Vector value, Iterable<MessageData4CFWritable> messages) {
        double errorOnTrain = 0d;
        int numTrain = 0;
        for (MessageData4CFWritable message : messages) {
            EdgeType et = message.getType();
            if (et == EdgeType.TRAIN) {
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = otherBias + value.dot(vector);
                double e = weight - predict;
                errorOnTrain += e;
                numTrain++;
            }
        }
        double bias = 0d;
        if (numTrain > 0) {
            bias = errorOnTrain / ((1 + lambda) * numTrain);
        }
        return bias;
    }

    @Override
    public void compute(Vertex<CFVertexId, VertexData4CGDWritable, EdgeData4CFWritable> vertex,
        Iterable<MessageData4CFWritable> messages) throws IOException {
        long step = getSuperstep();
        if (step == 0) {
            initialize(vertex);
            vertex.voteToHalt();
            return;
        }

        Vector currentValue = vertex.getValue().getVector();
        double currentBias = vertex.getValue().getBias();
        // update aggregators every (2 * interval) super steps
        if ((step % (2 * learningCurveOutputInterval)) == 0) {
            double errorOnTrain = 0d;
            double errorOnValidate = 0d;
            double errorOnTest = 0d;
            int numTrain = 0;
            for (MessageData4CFWritable message : messages) {
                EdgeType et = message.getType();
                double weight = message.getWeight();
                Vector vector = message.getVector();
                double otherBias = message.getBias();
                double predict = currentBias + otherBias + currentValue.dot(vector);
                double e = weight - predict;
                switch (et) {
                case TRAIN:
                    errorOnTrain += e * e;
                    numTrain++;
                    break;
                case VALIDATE:
                    errorOnValidate += e * e;
                    break;
                case TEST:
                    errorOnTest += e * e;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown recognized edge type: " + et.toString());
                }
            }
            double costOnTrain = 0d;
            if (numTrain > 0) {
                costOnTrain = errorOnTrain / numTrain + lambda * (currentBias * currentBias +
                    currentValue.dot(currentValue));
            }
            aggregate(SUM_TRAIN_COST, new DoubleWritable(costOnTrain));
            aggregate(SUM_VALIDATE_ERROR, new DoubleWritable(errorOnValidate));
            aggregate(SUM_TEST_ERROR, new DoubleWritable(errorOnTest));
        }

        if (step < maxSupersteps) {
            // implement CGD iterations
            Vector value0 = vertex.getValue().getVector();
            Vector gradient0 = vertex.getValue().getGradient();
            Vector conjugate0 = vertex.getValue().getConjugate();
            double bias0 = vertex.getValue().getBias();
            for (int i = 0; i < numCGDIters; i++) {
                double alpha = computeAlpha(gradient0, conjugate0, messages);
                Vector value = value0.plus(conjugate0.times(alpha));
                Vector gradient = computeGradient(bias0, value, messages);
                double beta = computeBeta(gradient0, conjugate0, gradient);
                Vector conjugate = conjugate0.times(beta).minus(gradient);
                value0 = value;
                gradient0 = gradient;
                conjugate0 = conjugate;
            }
            // update vertex values
            vertex.getValue().setVector(value0);
            vertex.getValue().setConjugate(conjugate0);
            vertex.getValue().setGradient(gradient0);

            // update vertex bias
            if (biasOn) {
                double bias = computeBias(value0, messages);
                vertex.getValue().setBias(bias);
            }

            // send out messages
            for (Edge<CFVertexId, EdgeData4CFWritable> edge : vertex.getEdges()) {
                MessageData4CFWritable newMessage = new MessageData4CFWritable(vertex.getValue(), edge.getValue());
                sendMessage(edge.getTargetVertexId(), newMessage);
            }
        }

        vertex.voteToHalt();
    }

    /**
     * Master compute associated with {@link ConjugateGradientDescentComputation}. It registers required aggregators.
     */
    public static class ConjugateGradientDescentMasterCompute extends DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException, IllegalAccessException {
            registerAggregator(SUM_TRAIN_COST, DoubleSumAggregator.class);
            registerAggregator(SUM_VALIDATE_ERROR, DoubleSumAggregator.class);
            registerAggregator(SUM_TEST_ERROR, DoubleSumAggregator.class);
            registerPersistentAggregator(SUM_LEFT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_RIGHT_VERTICES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TRAIN_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_VALIDATE_EDGES, LongSumAggregator.class);
            registerPersistentAggregator(SUM_TEST_EDGES, LongSumAggregator.class);
        }

        @Override
        public void compute() {
            // check convergence at every (2 * interval) super steps
            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);
            long step = getSuperstep();
            if (step < 3 || (step % (2 * learningCurveOutputInterval)) == 0) {
                return;
            }
            // calculate rmse on validate data
            DoubleWritable sumValidateError = getAggregatedValue(SUM_VALIDATE_ERROR);
            LongWritable numValidateEdges = getAggregatedValue(SUM_VALIDATE_EDGES);
            double validateRmse = 0d;
            if (numValidateEdges.get() > 0) {
                validateRmse = sumValidateError.get() / numValidateEdges.get();
                validateRmse = Math.sqrt(validateRmse);
            }
            sumValidateError.set(validateRmse);
            // calculate rmse on test data
            DoubleWritable sumTestError = getAggregatedValue(SUM_TEST_ERROR);
            LongWritable numTestEdges = getAggregatedValue(SUM_TEST_EDGES);
            double testRmse = 0d;
            if (numTestEdges.get() > 0) {
                testRmse = sumTestError.get() / numTestEdges.get();
                testRmse = Math.sqrt(testRmse);
            }
            sumTestError.set(testRmse);
            // evaluate convergence condition
            if (step >= 3 + (2 * learningCurveOutputInterval)) {
                float prevRmse = getConf().getFloat(PREV_RMSE, 0f);
                float threshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                if (Math.abs(prevRmse - validateRmse) < threshold) {
                    haltComputation();
                }
            }
            getConf().setFloat(PREV_RMSE, (float) validateRmse);
        }
    }

    /**
     * This is an aggregator writer for CGD, which after each superstep will persist the
     * aggregator values to disk, by use of the Writable interface.
     */
    public static class ConjugateGradientDescentAggregatorWriter implements AggregatorWriter {
        /** Name of the file we wrote to */
        private static String FILENAME;
        /** Saved output stream to write to */
        private FSDataOutputStream output;
        /**super step number*/
        private long lastStep = -1L;

        private ImmutableClassesGiraphConfiguration conf;

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
            FILENAME = "cf-learning-report";
        }

        @Override
        public void writeAggregator(Iterable<Entry<String, Writable>> aggregatorMap, long superstep)
            throws IOException {
            long realStep = lastStep;
            int learningCurveOutputInterval = getConf().getInt(LEARNING_CURVE_OUTPUT_INTERVAL, 1);

            // collect aggregated data
            HashMap<String, String> map = new HashMap<String, String>();
            for (Entry<String, Writable> entry : aggregatorMap) {
                map.put(entry.getKey(), entry.getValue().toString());
            }

            if (realStep == 0) {
                // output graph statistics
                long leftVertices = Long.parseLong(map.get(SUM_LEFT_VERTICES));
                long rightVertices = Long.parseLong(map.get(SUM_RIGHT_VERTICES));
                long trainEdges = Long.parseLong(map.get(SUM_TRAIN_EDGES)) * 2;
                long validateEdges = Long.parseLong(map.get(SUM_VALIDATE_EDGES)) * 2;
                long testEdges = Long.parseLong(map.get(SUM_TEST_EDGES)) * 2;
                output.writeBytes("======Graph Statistics======\n");
                output.writeBytes(String.format("Number of vertices: %d (left: %d, right: %d)%n",
                    leftVertices + rightVertices, leftVertices, rightVertices));
                output.writeBytes(String.format("Number of edges: %d (train: %d, validate: %d, test: %d)%n",
                    trainEdges + validateEdges + testEdges, trainEdges, validateEdges, testEdges));
                output.writeBytes("\n");
                // output cgd configuration
                int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS, 20);
                int featureDimension = getConf().getInt(FEATURE_DIMENSION, 20);
                float lambda = getConf().getFloat(LAMBDA, 0f);
                boolean biasOn = getConf().getBoolean(BIAS_ON, false);
                float convergenceThreshold = getConf().getFloat(CONVERGENCE_THRESHOLD, 0.001f);
                boolean bidirectionalCheck = getConf().getBoolean(BIDIRECTIONAL_CHECK, false);
                int numCGDIters = getConf().getInt(NUM_CGD_ITERS, 5);
                float maxVal = getConf().getFloat(MAX_VAL, Float.POSITIVE_INFINITY);
                float minVal = getConf().getFloat(MIN_VAL, Float.NEGATIVE_INFINITY);
                output.writeBytes("======CGD Configuration======\n");
                output.writeBytes(String.format("maxSupersteps: %d%n", maxSupersteps));
                output.writeBytes(String.format("featureDimension: %d%n", featureDimension));
                output.writeBytes(String.format("lambda: %f%n", lambda));
                output.writeBytes(String.format("biasOn: %b%n", biasOn));
                output.writeBytes(String.format("convergenceThreshold: %f%n", convergenceThreshold));
                output.writeBytes(String.format("bidirectionalCheck: %b%n", bidirectionalCheck));
                output.writeBytes(String.format("numCGDIters: %d%n", numCGDIters));
                output.writeBytes(String.format("maxVal: %f%n", maxVal));
                output.writeBytes(String.format("minVal: %f%n", minVal));
                output.writeBytes(String.format("learningCurveOutputInterval: %d%n", learningCurveOutputInterval));
                output.writeBytes("\n");
                output.writeBytes("======Learning Progress======\n");
            } else if (realStep > 0 && (realStep % (2 * learningCurveOutputInterval)) == 0) {
                // output learning progress
                double trainCost = Double.parseDouble(map.get(SUM_TRAIN_COST));
                double validateRmse = Double.parseDouble(map.get(SUM_VALIDATE_ERROR));
                double testRmse = Double.parseDouble(map.get(SUM_TEST_ERROR));
                output.writeBytes(String.format("superstep = %d%c", realStep, '\t'));
                output.writeBytes(String.format("cost(train) = %f%c", trainCost, '\t'));
                output.writeBytes(String.format("rmse(validate) = %f%c", validateRmse, '\t'));
                output.writeBytes(String.format("rmse(test) = %f%n", testRmse));
            }
            output.flush();
            lastStep = (int) superstep;
        }

        @Override
        public void close() throws IOException {
            output.close();
        }

        /**
         * Set the configuration to be used by this object.
         *
         * @param configuration Set configuration
         */
        @Override
        public void setConf(ImmutableClassesGiraphConfiguration configuration) {
            this.conf = configuration;
        }

        /**
         * Return the configuration used by this object.
         *
         * @return Set configuration
         */
        @Override
        public ImmutableClassesGiraphConfiguration getConf() {
            return conf;
        }
    }

}
