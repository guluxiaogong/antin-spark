package com.antin.mllib.test.spark2Oracle

import org.apache.hadoop.hbase.client.{Put, Scan, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by Administrator on 2017/5/16.
  */
object Spark2Oracle {

  def main(args: Array[String]) {
    inport1()
  }

  def inport1(): Unit = {

    //System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("inport1")
      //.setJars(Array("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\target\\spark-1.0-SNAPSHOT.jar"))
      //.setMaster("spark://192.168.2.88:7077")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    //定义 HBase 的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "zoe01,zoe02,zoe03") //HConstants.ZOOKEEPER_QUORUM
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, "user_info")
    conf.set(TableInputFormat.SCAN_ROW_START, "zhangsan_20150701_0001")
    conf.set(TableInputFormat.SCAN_ROW_STOP, "zhangsan_20150701_0008")
    conf.set(TableInputFormat.SCAN_COLUMNS, "base_info:name")

    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

   // hbaseRDD.count()
   // hbaseRDD.cache()

    //遍历输出
    hbaseRDD.foreach { case (_, result) =>
      val key = Bytes.toInt(result.getRow)
      val name = Bytes.toString(result.getValue("base_info".getBytes, "name".getBytes))

      println("Row key:" + key + " Name:" + name)
    }

    //指定输出格式和输出表名
    //val jobConfig: JobConf = new JobConf(conf, this.getClass)
    //jobConfig.setOutputFormat(classOf[TableOutputFormat])
    //jobConfig.setOutputFormat(classOf[TableOutputFormat])
    //jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "t1")
    //keyStatsRDD.map{case (k,v)=>convertToPut(k,v)}.saveAsHadoopDataSet(jobConfig)

    sc.stop()
  }

  //  def convertToPut(triple: (Int, String, Int)) = {
  //    val p = new Put(Bytes.toBytes(triple._1))
  //    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
  //    p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
  //    (new ImmutableBytesWritable, p)
  //  }


  //  def inport2(): Unit = {
  //    val tableName = "sensor"
  //    val sparkConf = new SparkConf().setAppName("inport2").setMaster("local[2]")
  //    val sc = new SparkContext(sparkConf)
  //
  //    val conf = HBaseConfiguration.create()
  //    conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.2.88:2888")
  //
  //    val scan = new Scan()
  //    scan.setCaching(100)
  //    scan.setStartRow(Bytes.toBytes("rk001"))
  //    scan.setStopRow(Bytes.toBytes("rk003"))
  //
  //    val hbaseContext = new HBaseContext(sc, conf)
  //
  //    //hbaseContext的hbaseScanRDD方法，这个方法返回的是一个
  //    //(RowKey, List[(columnFamily, columnQualifier, Value)]类型的RDD
  //    val hbaseRDD = hbaseContext.hbaseScanRDD(tableName, scan)
  //
  //    val rowKeyRDD = hbaseRDD.map(tuple => tuple._1)
  //    rowKeyRDD.foreach(key => println(Bytes.toString(key)))
  //
  //    val resultRDD = hbaseRDD.map { case (tuple1, tuple2) =>
  //      val rowKey = Bytes.toString(tuple1)
  //      val scalaArraysList = JavaConversions.asScalaBuffer(tuple2)
  //      val sb = new StringBuilder
  //      scalaArraysList.foreach {
  //        array => val result = Array(Bytes.toString(array._1), Bytes.toString(array._2), Bytes.toString(array._3))
  //          if (scalaArraysList.last != array) sb.append(result.mkString("_")).append("\t")
  //          else sb.append(result.mkString("_"))
  //      }
  //      (rowKey, sb.toString())
  //
  //    }.foreach(println)
  //
  //    sc.stop()
  //  }


  def inport3(): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "zoe01") //HConstants.ZOOKEEPER_QUORUM

    val sparkConf = new SparkConf().setAppName("inport3").setMaster("local")
    val sc = new SparkContext(sparkConf)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "user_info")

    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = stuRDD.count()
    println("Students RDD Count:" + count)
    stuRDD.cache()

    //遍历输出
    stuRDD.foreach({ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("base_info".getBytes, "name".getBytes))
      println("Row key:" + key + " Name:" + name)
    })
  }
}
