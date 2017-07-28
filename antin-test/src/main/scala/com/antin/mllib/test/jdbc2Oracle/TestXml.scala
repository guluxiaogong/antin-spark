package com.antin.mllib.test.jdbc2Oracle

import scala.xml.XML

/**
  * Created by Administrator on 2017/5/8.
  */
object TestXml {
  def main(args: Array[String]) {
    //test1
    test2
  }

  def test2() = {
    val path = "F:\\CommonDevelop\\hadoop\\project\\spark\\HelloSpark\\src\\main\\scala\\com\\antin\\test\\testXml.xml"
    val someXml = XML.loadFile(path)
    val headerField = someXml\"header"\"field"

    val fieldAttributes = (someXml\"header"\"field").map(_\"@name")

    //val fieldAttributes = someXml\"header"\"field"\\"@name"
    println(fieldAttributes)
  }

  def test1() = {
    val xmlFile =
      <symbols>
        <symbol ticker="AAPL">
          <units>200</units>
        </symbol>
        <units>300</units>
        <symbol ticker="IBM">
          <units>400</units>
        </symbol>
      </symbols>

    //    println(xmlFile)    // 直接打印xmlFragment的内容
    //    println(xmlFile.getClass()) // 打印xmlFragment的类型

    val unitsNodes = xmlFile \\ "units" // 提取出<units>元素
    // println(unitsNodes mkString "\n") // 打印提取出的结果
    //println(unitsNodes getClass) // 打印symbolNodes的类型

    /*    unitsNodes(0) match {
          case <units>{numOfUnits}</units> =>
            println("num of units : " + numOfUnits)
        }*/

    println(unitsNodes(1).text)
  }

}
