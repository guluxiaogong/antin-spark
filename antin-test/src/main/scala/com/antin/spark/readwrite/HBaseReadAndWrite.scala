package com.antin.spark.readwrite

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/22.
  */
object HBaseReadAndWrite {

  def main(args: Array[String]) {
    //readFromHbase()
    //writeToHbase
    //writeToHbase2

  }

  def readFromHbase(): Unit = {
    val hConf = HBaseConfiguration.create()
    //    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    //    hConf.set("hbase.zookeeper.quorum", "zoe-002,zoe-003,zoe-004")
    //    hConf.set("hbase.master", "zoe-002:16010")
    val tableName = "jcj:test"
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val conf = new SparkConf().setAppName("ReadFromHbase").setMaster("local")
    val sc = new SparkContext(conf)

    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("f1"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hConf.set(TableInputFormat.SCAN, ScanToString)


    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val resultRDD = rs.map(x => x._2)
    resultRDD.foreach(x => {
      println(Bytes.toString(x.getRow))
      //println(new String(x.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c1"))))
      // 通过列族和列名获取列
      // println(Bytes.toInt(x._2.getValue("f1".getBytes, "c1".getBytes)))
    })
  }

  def writeToHbase(): Unit = {
    // val conf = new SparkConf().setAppName("ReadFromHbase").setMaster("local")

    // 创建SparkSession对象
    val spark = SparkSession.builder().appName("spark sql").master("local").getOrCreate()
    // 创建sparkContext对象
    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    val tableName = "jcj:test"

    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val jobConf = new JobConf(hbaseConf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val pairs = sc.parallelize(List(("p_0000010", "12")))

    def convert(data: (String, String)) = {
      val p = new Put(Bytes.toBytes(data._1))
      p.add(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes(data._2))
      (new ImmutableBytesWritable, p)
    }

    // 保存数据到hbase数据库中
    new PairRDDFunctions(pairs.map(convert)).saveAsHadoopDataset(jobConf)
  }

  def writeToHbase2(): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("HBaseCount"))
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "jcj_test")
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 until 10)
    rdd.map(x => {
      val put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }
}
