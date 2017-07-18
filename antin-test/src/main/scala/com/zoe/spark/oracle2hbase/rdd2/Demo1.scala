package com.zoe.spark.oracle2hbase.rdd2

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}

/**
  * Created by Administrator on 2017/7/18.
  * 使用 org.apache.hadoop.hbase.client.Put
  * 将数据一条一条写入Hbase中，但是和Bulk加载相比效率低下，仅仅作为对比。
  *
  *
  * 这种方法api过时
  */
object Demo1 {
  def main(args: Array[String]) {
    val conf = HBaseConfiguration.create()
    val tableName = "jcj_word"
    //conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val myTable = new HTable(conf, tableName)
    val p = new Put(new String("row999").getBytes())
    p.add("f1".getBytes(), "column_name".getBytes(), new String("value999").getBytes())
    myTable.put(p)
    myTable.flushCommits()
  }

}
