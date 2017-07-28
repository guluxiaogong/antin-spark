package com.antin.spark.oracle2hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.KeyValue
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/17.
  *
  * 未完成//TODO
  */
object Oracle2Hbase {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Oracle2Hbase")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("partitionColumn", "sex")
      .option("lowerBound", 1)
      .option("upperBound", 2)
      .option("numPartitions", 2)
      .option("fetchSize", 1000)
      .option("url", "jdbc:oracle:thin:@192.168.0.91:1521:xmhealth")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "SEHR_XMAN_TEST")
      .option("user", "sehr_zoe")
      .option("password", "sehr_zoe")
      .load()

    // jdbcDF.write.option("hbase", 1)


    //    val conf = HBaseConfiguration.create()
    //    conf.set(TableInputFormat.INPUT_TABLE, "data1")
    //    val table = new HTable(conf, "data1")
    //
    //    val put = new Put(Bytes.toBytes("rowKey"))
    //    for (i <- 1 to 5) {
    //      var put = new Put(Bytes.toBytes("row" + i))
    //      put.add(Bytes.toBytes("v"), Bytes.toBytes("value"), Bytes.toBytes("value" + i))
    //      table.put(put)
    //    }


    val conf = HBaseConfiguration.create()
    val tableName = "data1"
    val table = new HTable(conf, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    lazy val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat.configureIncrementalLoad(job, table)

    //val rdd = sc.textFile("/data/produce/2015/2015-03-01.log").map(_.split("@")).map{x => (DigestUtils.md5Hex(x(0)+x(1)).substring(0,3)+x(0)+x(1),x(2))}.sortBy(x =>x._1).map{x=>{val kv:KeyValue = new KeyValue(Bytes.toBytes(x._1),Bytes.toBytes("v"),Bytes.toBytes("value"),Bytes.toBytes(x._2+""));(new ImmutableBytesWritable(kv.getKey),kv)}}


    //
    //    rdd.saveAsNewAPIHadoopFile("/tmp/data1",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat],job.getConfiguration())
    //    val bulkLoader = new LoadIncrementalHFiles(conf)
    //    bulkLoader.doBulkLoad(new Path("/tmp/data1"),table)

  }

}
