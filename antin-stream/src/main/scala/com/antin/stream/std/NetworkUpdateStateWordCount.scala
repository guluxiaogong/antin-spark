package com.antin.stream.std

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object NetworkUpdateStateWordCount {
  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "jcj")

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkUpdateStateWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    //做checkpoint 写入共享存储中
    ssc.checkpoint("hdfs://zoe-cluster/user/jcj/study/stream/NetworkUpdateStateWordCount/")
    val lines = ssc.socketTextStream("192.168.2.147", 9999)
    //reduceByKey 结果不累加
    //val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    //updateStateByKey结果可以累加但是需要传入一个自定义的累加函数：updateFunc
    val results = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}