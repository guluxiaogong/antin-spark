package com.zoe.spark.oracle2hbase.rdd2

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

/**
  * Created by Administrator on 2017/7/18.
  * 批量导数据到Hbase又可以分为两种：（1）、生成Hfiles，然后批量导数据；
  * （2）、直接将数据批量导入到Hbase中。
  * 批量将Hfiles导入Hbase
  * 现在我们来介绍如何批量将数据写入到Hbase中，主要分为两步：
  * （1）、先生成Hfiles；
  * （2）、使用 org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles 将事先生成Hfiles导入到Hbase中。
  */
object Demo2 {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Demo2"))

    method1(sc)
  }


  def method1(sc: SparkContext): Unit = {
    val conf = HBaseConfiguration.create()
    val tableName = "jcj_word"
    val table = new HTable(conf, tableName)
    // val hbaseConn = ConnectionFactory.createConnection(conf)
    // val table = hbaseConn.getTable(TableName.valueOf("jcj_word"))

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    HFileOutputFormat.configureIncrementalLoad(job, table)

    // Generate 10 sample data:
    val num = sc.parallelize(1 until 10)
    val rdd = num.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "f1".getBytes(), "c1".getBytes(), "value_xxx".getBytes())
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })

    // Save Hfiles on HDFS
    rdd.saveAsNewAPIHadoopFile("/tmp/jcj_word", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf)

    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("/tmp/jcj_word"), table)
  }
}
