package com.antin.mllib.test.jdbc2Oracle

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/8.
  */
object WordCountTest {
  def main(args: Array[String]) {
    //非常重要，是通向Spark集群的入口
    //val conf = new SparkConf().setAppName("WC").setMaster("local")
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
    val rdd = sc.textFile("F:\\CommonDevelop\\hadoop\\testData\\words.txt").cache()
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    //产生一个RDD: mapPartitions
    //.saveAsTextFile("F:\\CommonDevelop\\hadoop\\testData\\out2")
    println(rdd.collect().toBuffer)
    sc.stop()
  }

}
