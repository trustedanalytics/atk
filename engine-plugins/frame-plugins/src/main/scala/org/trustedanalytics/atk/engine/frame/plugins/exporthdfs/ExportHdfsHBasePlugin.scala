
package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, HBaseConfiguration }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{ Job }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.{ ExportHdfsHBaseArgs }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.{ RowWrapper, SparkFrame }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, Put }
import org.trustedanalytics.atk.domain.schema.Schema
import org.apache.commons.lang.StringUtils

// Implicits needed for JSON conversion 
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to hive table
 */
@PluginDoc(oneLine = "Write current frame to HBase table.",
  extended = """Table must exist in HBase.
Export of Vectors is not currently supported.""")
class ExportHdfsHBasePlugin extends SparkCommandPlugin[ExportHdfsHBaseArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_hbase"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsHBaseArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsHBaseArgs)(implicit invocation: Invocation): UnitReturn = {

    val frame: SparkFrame = arguments.frame
    val conf = createConfig(arguments.tableName)
    val familyName = arguments.familyName.getOrElse("familyColumn")

    val pairRdd = ExportHBaseImpl.convertToPairRDD(frame.rdd,
      familyName,
      arguments.keyColumnName.getOrElse(StringUtils.EMPTY))

    val hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create())
    if (!hBaseAdmin.tableExists(arguments.tableName)) {
      val desc = new HTableDescriptor(arguments.tableName)
      desc.addFamily(new HColumnDescriptor(familyName))

      hBaseAdmin.createTable(desc)
    }

    pairRdd.saveAsNewAPIHadoopDataset(conf)

  }

  /**
   * Create initial configuration for hbase writer
   * @param tableName name of hBase table
   * @return hBase configuration
   */
  private def createConfig(tableName: String): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = new Job(conf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    job.getConfiguration
  }

}

/**
 * Helper class for hbase implementation
 */
object ExportHBaseImpl extends Serializable {

  /**
   * Creates pair rdd to save to hbase
   * @param rdd initial frame rdd
   * @param familyColumnName family column name for hbase
   * @param keyColumnName key column name for hbase
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @return pair rdd
   */
  def convertToPairRDD(rdd: FrameRdd,
                       familyColumnName: String,
                       keyColumnName: String)(implicit invocation: Invocation) = {

    rdd.mapRows(_.valuesAsArray()).zipWithUniqueId().map {
      case (row, index) => buildRow((row, index), rdd.frameSchema, familyColumnName, keyColumnName)
    }
  }

  /**
   * Builds a row
   * @param row row of the original frame
   * @param schema original schema
   * @param familyColumnName family column name for hbase
   * @param keyColumnName key column name for hbase
   * @return hbase row
   */
  private def buildRow(row: (Array[Any], Long), schema: Schema, familyColumnName: String, keyColumnName: String) = {
    val columnTypes = schema.columns.map(_.dataType)
    val columnNames = schema.columns.map(_.name)
    val familyColumnAsByteArray = Bytes.toBytes(familyColumnName)

    val valuesAsDataTypes = DataTypes.parseMany(columnTypes.toArray)(row._1)
    val valuesAsByteArray = valuesAsDataTypes.map(value => {
      if (null == value) null else Bytes.toBytes(value.toString)
    })

    val keyColumnValue = Bytes.toBytes(keyColumnName + row._2)
    val put = new Put(keyColumnValue)
    for (index <- 0 to valuesAsByteArray.length - 1) {
      if (valuesAsByteArray(index) != null) {
        put.add(familyColumnAsByteArray, Bytes.toBytes(columnNames(index)), valuesAsByteArray(index))
      }
    }

    (new ImmutableBytesWritable(keyColumnValue), put)
  }

}