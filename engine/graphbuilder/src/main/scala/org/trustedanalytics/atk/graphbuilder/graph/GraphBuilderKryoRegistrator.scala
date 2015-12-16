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

package org.trustedanalytics.atk.graphbuilder.graph

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.spark.serializer.KryoRegistrator
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, GbIdToPhysicalId, _ }
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ CompoundValue, ConstantValue, EdgeRule, EdgeRuleParser, ParsedValue, PropertyRuleParser, SingleEdgeRuleParser, SinglePropertyRuleParser, SingleVertexRuleParser, VertexRule, VertexRuleParser, _ }
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, CombinedParser, InputSchema, _ }
import org.trustedanalytics.atk.graphbuilder.schema.{ EdgeLabelDef, InferSchemaFromData, PropertyDef, SchemaAccumulableParam, _ }

/**
 * Register GraphBuilder classes that are going to be serialized by Kryo.
 * If you miss a class here, it will likely still work, but registering
 * helps Kryo to go faster.
 * <p>
 * Kryo is 2x to 10x faster than Java Serialization.  In one experiment,
 * Kryo was 2 hours faster with 23GB of Netflix data.
 * </p>
 * <p>
 *  Usage:
 *   conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 *   conf.set("spark.kryo.registrator", "org.trustedanalytics.atk.graphbuilder.graph.GraphBuilderKryoRegistrator")
 * </p>
 */
class GraphBuilderKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {

    // elements package
    kryo.register(classOf[GBEdge])
    kryo.register(classOf[GbIdToPhysicalId])
    kryo.register(classOf[GraphElement])
    kryo.register(classOf[Property])
    kryo.register(classOf[GBVertex])

    // parser.rule package
    kryo.register(classOf[DataTypeResolver])
    kryo.register(classOf[EdgeRuleParser])
    kryo.register(classOf[SingleEdgeRuleParser])
    kryo.register(classOf[ParseRule])
    kryo.register(classOf[EdgeRule])
    kryo.register(classOf[VertexRule])
    kryo.register(classOf[PropertyRule])
    kryo.register(classOf[PropertyRuleParser])
    kryo.register(classOf[SinglePropertyRuleParser])
    kryo.register(classOf[Value])
    kryo.register(classOf[CompoundValue])
    kryo.register(classOf[ConstantValue])
    kryo.register(classOf[ParsedValue])
    kryo.register(classOf[VertexRuleParser])
    kryo.register(classOf[SingleVertexRuleParser])

    // parser package
    kryo.register(classOf[CombinedParser])
    kryo.register(classOf[InputRow])
    kryo.register(classOf[InputSchema])
    kryo.register(classOf[ColumnDef])

    // reader package
    kryo.register(classOf[ImmutableBytesWritable])
    kryo.register(classOf[Result])

    // schema package
    kryo.register(classOf[EdgeLabelDef])
    kryo.register(classOf[PropertyDef])
    kryo.register(classOf[PropertyType.Value])
    kryo.register(classOf[GraphSchema])
    kryo.register(classOf[InferSchemaFromData])
    kryo.register(classOf[SchemaAccumulableParam])
    kryo.register(classOf[NullWritable])
  }
}
