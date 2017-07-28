package com.antin.spark.counter

import org.apache.spark.util.AccumulatorV2
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2017/7/22.
  */
class MyAccumulatorV2 extends AccumulatorV2[String, String] {

  private val log = LoggerFactory.getLogger("MyAccumulatorV2")

  var result = "user0=0|user1=0|user2=0|user3=0" // 初始值

  override def isZero: Boolean = {
    true
  }

  override def copy(): AccumulatorV2[String, String] = {
    val myAccumulator = new MyAccumulatorV2()
    myAccumulator.result = this.result
    myAccumulator
  }

  override def reset(): Unit = {
    result = "user0=0|user1=0|user2=0|user3=0"
  }

  override def add(v: String): Unit = {
    val v1 = result
    val v2 = v
    //    log.warn("v1 : " + v1 + " v2 : " + v2)
    if (v1 == null && v == null) {
      var newResult = ""
      // 从v1中，提取v2对应的值，并累加
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue = oldValue.toInt + 1
        newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
      result = newResult
    }
  }

  override def merge(other: AccumulatorV2[String, String]) = other match {
    case map: MyAccumulatorV2 =>
      result = other.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: String = {
    result
  }
}

object StringUtils {
  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    val fields = str.split(delimiter)
    var result = "0"
    for (concatField <- fields) {
      if (concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        if (fieldName == field) {
          result = fieldValue
        }
      }
    }
    result
  }

  /**
    * 从拼接的字符串中给字段设置值
    *
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
    val fields = str.split(delimiter)

    val buffer = new StringBuffer("")
    for (item <- fields) {
      val fieldName = item.split("=")(0)
      if (fieldName == field) {
        val concatField = fieldName + "=" + newFieldValue
        buffer.append(concatField).append("|")
      } else {
        buffer.append(item).append("|")
      }
    }
    buffer.substring(0, buffer.length() - 1)
  }


}