package com.zoe.demo.demo01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 远程提交
  * 1.  spark-submit --class com.zoe.demo.demo01.WordCount --master yarn-client /zoesoft/zoeJobJar/zoe-spark.jar
  * 2.  spark-submit --class com.zoe.demo.demo01.WordCount --master yarn --deploy-mode cluster /zoesoft/zoeJobJar/zoe-spark.jar
  * 3.  spark-submit --class com.zoe.demo.demo01.WordCount \
  * --master yarn \
  * --deploy-mode client \
  * --driver-memory 4g \
  * --executor-memory 2g \
  * --executor-cores 1 \
  * /zoesoft/zoeJobJar/zoe-spark.jar \
  * 10
  */
object WordCount {
  def main(args: Array[String]) {

    //屏蔽日志
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    System.setProperty("HADOOP_USER_NAME", "demo")
    //非常重要，是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
      .setMaster("local")
      //.setJars(Array("F:\\CommonDevelop\\hadoop\\project\\zoe\\zoe-spark\\zoe-demo\\target\\zoe-spark.jar"))
      //.setMaster("yarn")

    //设置运行资源参数
    //    conf.set("spark.executor.instances", "30")
    //    conf.set("spark.executor.cores", "3")
    //    conf.set("spark.executor.memory", "5G")
    //    conf.set("spark.driver.memory", "3G")
    //    conf.set("spark.driver.maxResultSize", "10G")

    val sc = new SparkContext(conf)

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
    sc.textFile("hdfs://zoe-cluster/demo-data/input/words.txt").cache()
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_ + _)
      //产生一个RDD: mapPartitions
      .saveAsTextFile("hdfs://zoe-cluster/demo-data/output/wordcount02")

    sc.stop()
  }
}
