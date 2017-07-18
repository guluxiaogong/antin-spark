package com.zoe.spark.oracle2hbase.rdd1

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/7/18.
  * 3.第三种方法
  * 将写进Hbase转换为Mapreduce任务
  */
object Word3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Word3"))
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    //jobConf.set("hbase.zookeeper.quorum", "192.168.14.83,192.168.14.84,192.168.14.85")
    //jobConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "jcj_word")
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 until 1000)
    rdd.map(x => {
      val put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }
}
