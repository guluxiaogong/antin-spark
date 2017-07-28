package com.antin.spark.counter

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/7/22.
  */
object RowCounter {
  private val log = LoggerFactory.getLogger(RowCounter.getClass)

  def main(args: Array[String]) {
    testCounter
    //    if (args.length < 0) {
    //      log.warn("need hbase tableName!!!")
    //      System.exit(-1)
    //    }
    //    myHBaseCounter(args(0))
    //myHBaseCounter("EHR_R")

  }

  def myHBaseCounter(tableName: String): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val conf = new SparkConf()
      .setAppName("myHBaseCounter")
      .setMaster("yarn")
    val sc = new SparkContext(conf)

    val accumulator = new MyHBaseAccumulatorV2()
    sc.register(accumulator, "My HBase Accumulator")

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rs.foreach(x => {
      accumulator.add(1)
    })
    val total = accumulator.value
    println(s"总共 ===> $total 行")
  }

  def testCounter(): Unit = {
    val hConf = HBaseConfiguration.create()
    val tableName = "performance_test:sehr_xman"//EHR_R"
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val conf = new SparkConf()
      .setAppName("ReadFromHbase")
      .setMaster("local[20]")
    val sc = new SparkContext(conf)


    val accumulator = new MyHBaseAccumulatorV2()
    sc.register(accumulator, "My HBase Accumulator")

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rs.foreach(x => {
      accumulator.add(1)
    })
    val total = accumulator.value
    println(s"总共 ===> $total 行")
    val totalRdd = sc.parallelize(Array(total),1)
    totalRdd.saveAsTextFile("hdfs://zoe-cluster/demo-data/output/counter")

  }

}
