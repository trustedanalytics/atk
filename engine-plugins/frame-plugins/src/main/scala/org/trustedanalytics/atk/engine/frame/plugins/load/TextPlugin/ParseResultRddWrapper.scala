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

package org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin

import org.apache.spark.frame.FrameRdd

/**
 * RDD results of loading a dataframe including both successfully parsed lines and errors
 * @param parsedLines lines that were successfully parsed
 * @param errorLines lines that were NOT successfully parsed including error messages
 */
case class ParseResultRddWrapper(parsedLines: FrameRdd, errorLines: FrameRdd)
