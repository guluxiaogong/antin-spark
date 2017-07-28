package com.antin.mllib.test.jdbc2Oracle

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2017/5/8.
  */
object TestXml2Json {

  def main(args: Array[String]) {
    //val path = "F:\\CommonDevelop\\hadoop\\project\\spark\\HelloSpark\\src\\main\\scala\\com\\antin\\test\\testXml.xml"
    //val someXml = XML.loadFile(path)
    val conf = new SparkConf().setAppName("hdfs").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("hdfs://zoe01:9000/input/test.log")
    rdd.foreach(x => {
      println(x)
    })
    //    println(rdd.collect.toBuffer)
  }

}
