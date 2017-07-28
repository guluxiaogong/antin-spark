package com.antin.spark.counter

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/7/24.
  * ./bin/spark-submit --class com.antin.spark.counter.RowCounter2 --master yarn --deploy-mode cluster /zoesoft/zoeJobJar/antin-test.jar "EHR_R"
  *
  * 本地运行没问题，集群上运行出错
  */
object RowCounter2 {
  private val log = LoggerFactory.getLogger(RowCounter2.getClass)

  def main(args: Array[String]) {

    //    if (args.length < 0) {
    //      log.warn("need hbase tableName!!!")
    //      System.exit(-1)
    //    }
    val hConf = HBaseConfiguration.create()
    // val tableName = args(0) //"EHR_R" //"performance_test:sehr_xman"
    val tableName = "hpor:sehr_xman"//"hpor:sehr_test"
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val conf = new SparkConf()
      .setAppName("ReadFromHbase")
      .setMaster("local")
    //.setMaster("yarn")
    val sc = new SparkContext(conf)

    val accumulator = sc.longAccumulator("My Accumulator")

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rs.foreach(x => {
      accumulator.add(1L)
    })
    val total = accumulator.value
    println(s"总共 ===> $total 行")
    //val totalRdd = sc.parallelize(Array(total), 1)
    //totalRdd.saveAsTextFile("hdfs://zoe-cluster/demo-data/output/counter3")

  }
}

/*
17/07/24 15:09:18 INFO Client: Application report for application_1500518571519_0010 (state: RUNNING)
17/07/24 15:09:19 INFO Client: Application report for application_1500518571519_0010 (state: RUNNING)
17/07/24 15:09:20 INFO Client: Application report for application_1500518571519_0010 (state: RUNNING)
17/07/24 15:09:21 INFO Client: Application report for application_1500518571519_0010 (state: FINISHED)
17/07/24 15:09:21 INFO Client:
         client token: N/A
         diagnostics: User class threw exception: org.apache.hadoop.hbase.client.RetriesExhaustedException: Can't get the locations
         ApplicationMaster host: 192.168.14.95
         ApplicationMaster RPC port: 0
         queue: default
         start time: 1500879911371
         final status: FAILED
         tracking URL: http://zoe-004:8088/proxy/application_1500518571519_0010/
         user: jcj
Exception in thread "main" org.apache.spark.SparkException: Application application_1500518571519_0010 finished with failed status
        at org.apache.spark.deploy.yarn.Client.run(Client.scala:1244)
        at org.apache.spark.deploy.yarn.Client$.main(Client.scala:1290)
        at org.apache.spark.deploy.yarn.Client.main(Client.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:750)
        at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:187)
        at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:212)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:126)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
17/07/24 15:09:21 INFO ShutdownHookManager: Shutdown hook called
17/07/24 15:09:21 INFO ShutdownHookManager: Deleting directory /tmp/spark-24d2167f-5cbc-4543-9f35-050bce30c7af
*/