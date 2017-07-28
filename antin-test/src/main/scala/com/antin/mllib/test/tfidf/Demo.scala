package com.antin.mllib.test.tfidf

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/17.
  */
object Demo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)
    val fileRDD = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\test\\test\\tfidf\\data\\test1.txt")
    println(fileRDD.count())

  }

}
