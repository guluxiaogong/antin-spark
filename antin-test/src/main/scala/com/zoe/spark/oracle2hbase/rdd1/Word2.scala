package com.zoe.spark.oracle2hbase.rdd1

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/18.
  * 2.第二种方法
  * 批量写入Hbase，使用的API：
  */
object Word2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("hbase"))
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 until 1000)
    rdd.map(value => {
      val put = new Put(Bytes.toBytes(value.toString))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(value.toString))
      put
    }).foreachPartition(iterator => {
      val jobConf = new JobConf(HBaseConfiguration.create())
      //jobConf.set("hbase.zookeeper.quorum", "192.168.14.83,192.168.14.84,192.168.14.85")
      //jobConf.set("zookeeper.znode.parent", "/hbase-unsecure")
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      val table = new HTable(jobConf, TableName.valueOf("jcj_word"))
      import scala.collection.JavaConversions._
      table.put(seqAsJavaList(iterator.toSeq))
    })
  }
}
