package com.antin.spark.counter

import org.apache.spark.util.AccumulatorV2

/**
  * Created by Administrator on 2017/7/22.
  */
class MyHBaseAccumulatorV2 extends AccumulatorV2[Long, Long] {


  var result = 1L // 初始值

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[Long, Long] = {
    val myAccumulator = new MyHBaseAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = 0L
  }

  override def add(v: Long): Unit = {
    result = result + 1
  }

  override def merge(other: AccumulatorV2[Long, Long]) = other match {
    case map: MyHBaseAccumulatorV2 =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: Long = {
    result
  }
}

