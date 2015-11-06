/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin;

/**
 * Original file part of Apache Mahout.
 * https://github.com/apache/mahout/blob/ad84344e4055b1e6adff5779339a33fa29e1265d/examples/src/main/java/org/apache/mahout/classifier/bayes/XmlInputFormat.java
 */

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class MultiLineTaggedInputFormat extends TextInputFormat {

    private static final Logger log = LoggerFactory.getLogger(MultiLineTaggedInputFormat.class);

    public static final String START_TAG_KEY = "taggedinput.start";
    public static final String END_TAG_KEY = "taggedinput.end";
    public static final String IS_XML_KEY = "taggedinput.isxml";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new TaggedRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            log.warn("Error while creating TaggedRecordReader", ioe);
            return null;
        }
    }

    /**
     * TaggedRecordReader class to read through a given xml document to output xml blocks as records as specified
     * by the start tag and end tag
     *
     */
    public static class TaggedRecordReader extends RecordReader<LongWritable, Text> {

        private final byte[][] startTags;
        private final byte[][] endTags;
        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable currentKey;
        private Text currentValue;
        private final String[] startTagStrings;
        private String latestStartTag = "";
        private boolean isXML;
        byte[][] quoteChars = getTagArray(new String[] { "\"", "'"});
        byte[][] escapeChars = getTagArray(new String[]{"\\"});
        byte[][] xmlNodeEnd = getTagArray(new String[]{">"});
        byte[][] xmlAltEnd = getTagArray(new String[]{"/>"});
        byte[][] commentStartChars = getTagArray(new String[]{"<!--"});
        byte[][] commentEndChars = getTagArray(new String[]{"-->"});

        public TaggedRecordReader(FileSplit split, Configuration conf) throws IOException {
            startTagStrings = conf.getStrings(START_TAG_KEY);
            String[] endTagStrings = conf.getStrings(END_TAG_KEY);
            startTags = getTagArray(startTagStrings);
            endTags = getTagArray(endTagStrings);
            isXML = conf.getBoolean(IS_XML_KEY, false);

            for(String endTag : endTagStrings){
                for(String startTag : startTagStrings){
                    if(endTag.contains(startTag)){
                        throw new IOException("An end tag cannot contain any start tag as a substring");
                    }
                }
            }

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }

        /**
         * Convert a String array into a list of byte arrays
         * @param values array of strings
         */
        private byte [][] getTagArray(String[] values){
            byte [][] tags = new byte[values.length][];
            for(int i = 0; i < values.length; i++){
                tags[i] = values[i].getBytes(Charsets.UTF_8);
            }
            return tags;
        }

        /**
         * Search for the next key/value pair in the hadoop file
         * @param key
         * @param value
         * @return
         * @throws IOException
         */
        private boolean next(LongWritable key, Text value) throws IOException {
            if (fsin.getPos() < end && readUntilMatch(startTags, false)) {
                try {
                    if (readUntilMatchNested(endTags, true, startTags)) {
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            Closeables.close(fsin, true);
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }


        /**
         * Parse the file byte by byte until we find a match or the file ends this does not track nested values
         * @param match a list of possible matches
         * @param withinBlock true if we are inside of a tag and are looking for an end tag
         * @return true if we find a match
         * @throws IOException
         */
        private boolean readUntilMatch(byte[][] match, boolean withinBlock) throws IOException {
            return readUntilMatchNested(match, withinBlock, null);
        }

        private enum ParseState {
            ESCAPE_CHAR,
            IN_COMMENT,
            IN_QUOTE,
            NORMAL
        }


        /**
         * Parse the file byte by byte until we find a match or the file ends
         * @param match a list of possible matches
         * @param withinBlock true if we are inside of a tag and are looking for an end tag
         * @param startTag list of possible startTags. only needed if we are within a Block
         * @return true if we find a match
         * @throws IOException
         */
        private boolean readUntilMatchNested(byte[][] match, boolean withinBlock, byte[][] startTag) throws IOException {
            return new NestedMatchFinder(match, withinBlock, startTag, this.isXML).invoke();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }

        /**
         * Reads the file byte by byte until we find either a desired match or the end of the block.
         */
        private class NestedMatchFinder {
            private byte[][] match;
            private boolean withinBlock;
            private byte[][] startTag;
            private boolean checkXML;
            ParseState parseState = ParseState.NORMAL;
            int currentQuote = 0;
            int currentQuoteIndex = -1;
            boolean inStartTag;
            int depth = 0;

            public NestedMatchFinder(byte[][] match, boolean withinBlock, byte[][] startTag, boolean checkXML) {
                this.match = match;
                this.withinBlock = withinBlock;
                this.startTag = startTag;
                this.checkXML = checkXML;
                inStartTag = checkXML && latestStartTag.endsWith(" ");
            }

            /**
             * execute the method object
             * @return true if we find a match, false if we do not.
             * @throws IOException
             */
            public boolean invoke() throws IOException {
                int[] matchIndices = new int[match.length];
                int[] startTagIndices = startTag != null ? new int[startTag.length] : null;
                int[] escapeCharIndices = new int[escapeChars.length];
                int[] quoteIndices = new int[quoteChars.length];
                int[] xmlNodeEndIndices = new int[xmlNodeEnd.length];
                int[] xmlAltEndIndices = new int[xmlAltEnd.length];
                int[] xmlCommentStartIndices = new int[commentStartChars.length];
                int[] xmlCommentEndIndices = new int[commentEndChars.length];


                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1) {
                        return false;
                    }
                    // save to buffer:
                    if (withinBlock) {
                        buffer.write(b);
                    }


                    checkQuotes(escapeCharIndices, quoteIndices, b);
                    checkComments(xmlCommentStartIndices, xmlCommentEndIndices, b);

                    if(parseState == ParseState.NORMAL) {
                        if (checkEmptyElementNode(xmlNodeEnd, xmlNodeEndIndices, xmlAltEnd, xmlAltEndIndices, b))
                            return true;
                        checkStartTagForDepth(startTagIndices, b);
                        if (checkForMatch(matchIndices, b))
                            return true;
                    }

                    // see if we've passed the stop point:
                    if (noLongerInBlock(matchIndices)) {
                        return false;
                    }
                }
            }

            /**
             * Check if the value is still within the block. If not it should go to the next block
             * @return
             * @throws IOException
             */
            private boolean noLongerInBlock(int[] matchIndices) throws IOException {
                return !withinBlock && allValuesAreZero(matchIndices) && fsin.getPos() >= end;
            }

            /**
             * Check
             */
            private boolean checkForMatch(int[] matchIndices, int b) throws IOException {
                int matchIndex;// check if we're matching:
                if ((matchIndex = checkMatches(b, matchIndices, match)) != -1) {
                    if (depth == 0) {
                        if (!withinBlock) {
                            byte[] foundBytes = match[matchIndex];
                            latestStartTag = new String(foundBytes, Charsets.UTF_8);
                            buffer.write(foundBytes);
                        }
                        return true;
                    } else {
                        depth--;
                        initIntArray(matchIndices);
                    }
                }
                return false;
            }

            /**
             * Check if you have re-encountered the start tag. if you have you are a level deeper into the nested structure.
             */
            private void checkStartTagForDepth(int[] startTagIndices, int b) {
                int matchIndex;
                if (startTag != null) {
                    // Check if we match the start tag
                    if ((matchIndex = checkMatches(b, startTagIndices, startTag)) != -1) {
                        depth++;
                        //If this is an xml file we have to check for matching empty element nodes
                        if(checkXML) {
                            byte[] foundBytes = startTag[matchIndex];
                            latestStartTag = new String(foundBytes, Charsets.UTF_8);
                            inStartTag = latestStartTag.endsWith(" ");
                        }
                        initIntArray(startTagIndices);
                    }
                }
            }

            /**
             * Check for XML comments
             */
            private void checkComments(int[] xmlCommentStartIndices, int[] xmlCommentEndIndices, int b) {
                if(checkXML){
                    if(this.parseState == ParseState.NORMAL){
                        //if this is in a normal state check for start of a comment
                        if(checkMatches(b, xmlCommentStartIndices, commentStartChars) != -1){
                            this.parseState = ParseState.IN_COMMENT;
                            initIntArray(xmlCommentStartIndices);
                        }
                    }else if(this.parseState == ParseState.IN_COMMENT){
                        //if this is inside of a comment check for the end
                        if(checkMatches(b, xmlCommentEndIndices, commentEndChars) != -1){
                            this.parseState = ParseState.NORMAL;
                            initIntArray(xmlCommentEndIndices);
                        }
                    }
                }
            }

            /**
             * Check for the alternate ending of an xml node. The empty element node is a single tag ending in a />
             * @return
             */
            private boolean checkEmptyElementNode(byte[][] xmlNodeEnd, int[] xmlNodeEndIndices, byte[][] xmlAltEnd, int[] xmlAltEndIndices, int b) {
                if(checkXML && inStartTag){
                    //if this is xml we need to make sure that when the tag ends it doesn't end the node.
                    if(checkMatches(b, xmlAltEndIndices, xmlAltEnd) != -1){
                        //This ends the node because it is an Empty-Element tag
                        if (depth == 0){
                            return true;
                        } else {
                            depth--;
                            inStartTag = false;
                        }
                    }else if(checkMatches(b, xmlNodeEndIndices, xmlNodeEnd) != -1){
                        inStartTag = false;
                    }
                }
                return false;
            }

            /**
             * If you are within a quoted attribute or json value any tag that you find is to be ignored.
             */
            private void checkQuotes(int[] escapeCharIndices, int[] quoteIndices, int b) {
                if(isXML && !inStartTag)
                    return;
                // This section checks for quoted attributes. If a tag appears in an attribute than it should not count as a match
                if (parseState == ParseState.ESCAPE_CHAR) {
                    //if we are in an escape char we want to check for a quote. If we wait the length of the quote we will know that we are back in the string.
                    currentQuoteIndex++;
                    if (currentQuoteIndex >= quoteChars[currentQuote].length) {
                        parseState = ParseState.IN_QUOTE;
                    }
                } else {
                    if (parseState == ParseState.IN_QUOTE) {
                        // if we are inside of a quote we need to verify that the next quote we find is not escaped
                        if (checkMatches(b, escapeCharIndices, escapeChars) != -1)
                            parseState = ParseState.ESCAPE_CHAR;
                    }
                    if (parseState != ParseState.ESCAPE_CHAR) {
                        int quoteIndex;
                        if ((quoteIndex = checkMatches(b, quoteIndices, quoteChars)) != -1) {
                            if (parseState == ParseState.NORMAL) {
                                parseState = ParseState.IN_QUOTE;
                                currentQuote = quoteIndex;
                                currentQuoteIndex = 0;
                            } else {
                                if (quoteIndex == currentQuote) {
                                    parseState = ParseState.NORMAL;
                                }
                            }
                        }
                    }
                }
            }

            /**
             * update the byte array index flags
             * @param b current byte
             * @param indices an array of indices related to the corresponding tags
             * @param tags Tag List we are checking for
             * @return -1 if no match, if a match it is the index we are looking for
             */
            private int checkMatches(int b, int[] indices, byte [][] tags) {
                for(int i = 0; i < indices.length; i++) {
                    if (b == tags[i][indices[i]]) {
                        indices[i] = indices[i] + 1;
                        if (indices[i] >= tags[i].length) {
                            indices[i] = 0;
                            return i;
                        }
                    } else {
                        indices[i] = 0;
                    }
                }
                return -1;
            }

            /**
             * takes an array of integers and set all values to 0
             */
            private void initIntArray(int arr[]){
                for(int i = 0; i < arr.length; i++){
                    arr[i] = 0;
                }
            }

            /**
             * Check if all of the values of an array are equal to 0
             * @param arr int array to verify
             * @return true if all values in the array are equal to 0
             */
            private boolean allValuesAreZero(int[] arr) {
                for (int i = 0; i < arr.length; i++) {
                    if (arr[i] != 0)
                        return false;
                }
                return true;
            }
        }
    }
}
