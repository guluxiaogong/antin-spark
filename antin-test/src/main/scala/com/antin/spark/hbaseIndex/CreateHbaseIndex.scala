package com.antin.spark.hbaseIndex

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{HTable, Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/25.
  * hbase=>hbase
  * /usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.antin.spark.hbaseIndex.CreateHbaseIndex --master yarn --deploy-mode client /zoesoft/zoeJobJar/antin-test.jar
  *
  * /usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.antin.spark.hbaseIndex.CreateHbaseIndex \
  * --master yarn \
  * --deploy-mode client \
  * --driver-memory 4g \
  * --executor-memory 4g \
  * --executor-cores 10 \
  * /zoesoft/zoeJobJar/antin-test.jar
  */
object CreateHbaseIndex {

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]) {

    //System.setProperty("HADOOP_USER_NAME", "hdfs")

    //从hbase中取姓名列
    val conf = new SparkConf().setAppName("CreateHbaseIndex")//.setMaster("local[6]")
    val sc = new SparkContext(conf)
    val rdd = queryNameFromHbaseColumn(sc, "hpor:sehr_xman", "attribute", "NAME")

    //将姓名+市民唯一标识做为索引行键插入索引表

    //李敏辉2731fbe3-286f-4079-891b-94712472f29d
    //val rdd = sc.parallelize(Seq("李敏辉2731fbe3-286f-4079-891b-94712472f29baa"))
    //writeToNameIdIndex(sc, "hpor:name_xmanid_index", "n", "", rdd)
    writeToNameIdIndex2(sc, "hpor:name_xmanid_index", "n", "", rdd)

    //rdd.saveAsTextFile("/jcj/output/hpor")
  }

  //从hbase中取姓名列
  def queryNameFromHbaseColumn(sc: SparkContext, tableName: String, family: String, qualifier: String): RDD[String] = {
    val hConf = HBaseConfiguration.create()
    hConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    //scan.addFamily(Bytes.toBytes(family))
    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))

    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hConf.set(TableInputFormat.SCAN, ScanToString)

    val rs = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rs.map(x => new String(x._2.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier))) + Bytes.toString(x._2.getRow))

    //    val resultRDD = rs.map(x => x._2)
    //    resultRDD.foreach(x => {
    //      println("行键：" + Bytes.toString(x.getRow) + "  姓名：" + new String(x.getValue(Bytes.toBytes("attribute"), Bytes.toBytes("NAME"))))
    //    })
  }

  //将姓名+市民唯一标识做为索引行键插入索引表
  def writeToNameIdIndex(sc: SparkContext, tableName: String, family: String, qualifier: String, rdd: RDD[String]): Unit = {
    val hdfsPath = "hdfs://zoe-cluster/tmp/sehr_xman"

    val conf = HBaseConfiguration.create()
    val table = new HTable(conf, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    HFileOutputFormat.configureIncrementalLoad(job, table)

    val hRdd = rdd.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(""))
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    })

    val fileSystem = FileSystem.get(new URI(hdfsPath), new Configuration)

    if (fileSystem.exists(new Path(hdfsPath)))
      fileSystem.delete(new Path(hdfsPath), true)

    //单独写一条是正常的
    //将查询与写入对接时，这一步会报错，
    //TODO
    // Save Hfiles on HDFS
    hRdd.saveAsNewAPIHadoopFile(hdfsPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf)

    //Bulk load Hfiles to Hbase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path(hdfsPath), table)
  }

  /**
    * 将写进Hbase转换为Mapreduce任务
    *
    * @param sc
    * @param tableName
    * @param family
    * @param qualifier
    * @param rdd
    */
  def writeToNameIdIndex2(sc: SparkContext, tableName: String, family: String, qualifier: String, rdd: RDD[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    rdd.map(x => {
      val put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }

}
