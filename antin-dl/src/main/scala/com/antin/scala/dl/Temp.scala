package com.antin.scala.dl

import java.io.{BufferedInputStream, File, FileInputStream}

import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator
import org.nd4j.linalg.dataset.DataSet

/**
  * Created by Administrator on 2017/7/29.
  */
object Temp {

  def main(args: Array[String]) {
    val d = new DataSet()
    val bis = new BufferedInputStream(new FileInputStream(new File("path/to/your/file")))
    d.load(bis)
    bis.close()
    val iter = new ListDataSetIterator(d.asList(), 10)
  }

}
