package com.antin.spark.oracle2hbase.rdd1

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/18.
  * 1. 第一种方法
  * 每次写进一条，调用API
  */
object Word1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Word1"))
    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 until 10)
    rdd.foreachPartition(x => {
      val hbaseConf = HBaseConfiguration.create()
//      hbaseConf.set("hbase.zookeeper.quorum", "192.168.14.83,192.168.14.84,192.168.14.85")
//      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//      hbaseConf.set("hbase.defaults.for.version.skip", "true")
      val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
      val table = hbaseConn.getTable(TableName.valueOf("jcj_word"))
      x.foreach(value => {
        val put = new Put(Bytes.toBytes(value.toString))
        put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(value.toString))
        table.put(put)
      })
    })
  }
}
