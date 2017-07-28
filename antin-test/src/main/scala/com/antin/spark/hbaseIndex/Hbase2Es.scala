package com.antin.spark.hbaseIndex

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

/**
  * Created by Administrator on 2017/7/26.
  */
object Hbase2Es {

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]) {
    //demo1()
    //demo2()
    //demo3()
    demo4()
  }

  def demo4(): Unit = {
    val conf = new SparkConf()
      .set("es.nodes", "zoe-007,zoe-008,zoe-009,zoe-010")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .setMaster("local")
      .setAppName("demo4")

    val sc = new SparkContext(conf)
    //创建sqlContext
    // val sqlContext = new SQLContext(sc)

    queryNameFromHbaseColumn2(sc, "hpor:sehr_xman")


  }

  //从hbase中取数据
  def queryNameFromHbaseColumn2(sc: SparkContext, tableName: String): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()

    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("name"))
    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("sex"))
    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("birthday"))
    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("native"))
    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("address"))
    scan.addColumn(Bytes.toBytes("attribute"), Bytes.toBytes("work"))
    scan.addFamily(Bytes.toBytes("diagnose"))

    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hConf.set(TableInputFormat.SCAN, ScanToString)

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val resultRDD = rs.map(x => x._2)



    val people = resultRDD.map(x => {

      var rowM = Map("rowKey" -> Bytes.toString(x.getRow))
      val arr = x.rawCells()
      arr.foreach(cell => {
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        if (qualifier != null && !"".equals(qualifier.trim)) {
          rowM += (qualifier.trim -> Bytes.toString(CellUtil.cloneValue(cell)))
        }

      })
      rowM
    })
    //println("===========================================>" + people.toDebugString)
    people.saveToEs("hpor:name_xmanid_index/person")

  }

  //从hbase中取数据
  def queryNameFromHbaseColumn(sc: SparkContext, tableName: String): Unit = {
    val hConf = HBaseConfiguration.create()
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    //scan.addFamily(Bytes.toBytes("f1"))
    //scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hConf.set(TableInputFormat.SCAN, ScanToString)

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val resultRDD = rs.map(x => x._2)
    //println("=====================")
    //    resultRDD.map(x => {
    //      println("=====================>" + x.getRow)
    //      val keyValue = x.raw()
    //      keyValue.foreach(kv => {
    //        println("列：" + new String(kv.getFamily())
    //          + "====值:" + new String(kv.getValue()))
    //      })
    //    })
    //val resultRDD = rs.map(x => x._2)
    //        resultRDD.foreach(x => {
    //          println("行键：" + Bytes.toString(x.getRow) + "  姓名：" + new String(x.getValue(Bytes.toBytes("attribute"), Bytes.toBytes("NAME"))))
    //        })
    resultRDD.foreach(x => {
      println("行键：" + Bytes.toString(x.getRow))
      val arr = x.rawCells()
      arr.foreach(cell => {
        println("Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
          "Value : " + Bytes.toString(CellUtil.cloneValue(cell)))
      })
    })
  }

  case class Person(sid: String, name: String, age: Int)

  def demo3(): Unit = {
    val conf = new SparkConf()
      .set("es.nodes", "zoe-007,zoe-008,zoe-009,zoe-010")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .setMaster("local")
      .setAppName("demo3")

    val sc = new SparkContext(conf)
    //创建sqlContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //创建DataFrame
    val people = sc.parallelize(Array(1, 2, 3, 4, 5, 6)).map(p => Person(p + "", "lisi" + p, 18)).toDF()
    //people.show()

    people.saveToEs("spark2/people")
  }


  def demo2(): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Demo2").setMaster("local"))
    //创建sqlContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val options = Map(
      "pushdown" -> "true",
      "es.nodes" -> "zoe-007,zoe-008,zoe-009,zoe-010",
      "es.port" -> "9200"
    )

    val spark14DF = sqlContext.read.format("org.elasticsearch.spark.sql").options(options).load("spark2/people")

    spark14DF.select("name", "age").collect().foreach(println(_))

    spark14DF.registerTempTable("people")
    val results = sqlContext.sql("SELECT name FROM people")
    results.map(t => "Name:" + t(0)).collect().foreach(println)
  }

  def demo1(): Unit = {
    val conf = new SparkConf()
      .set("es.nodes", "zoe-007,zoe-008,zoe-009,zoe-010")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")
      .setMaster("local")
      .setAppName("demo1")

    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    sc.makeRDD(Seq(numbers, airports)).saveToEs("spark1/docs")
  }

}
