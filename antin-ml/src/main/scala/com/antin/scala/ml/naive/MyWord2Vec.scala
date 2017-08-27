/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.antin.scala.ml.naive

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession

/**
  * 将文字转成数值
  */
object MyWord2Vec {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MyWord2Vec")
      .master("local")
      .getOrCreate()

    val df = spark.read.textFile("D:\\ZHS\\智能预约\\data\\features-1.txt")
    //import spark.implicits._
    import spark.implicits._
    val featuresDF = df.map(_.split(" ")).map(Tuple1.apply).toDF("text")
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5)
      .setMinCount(0)
    val model = word2Vec.fit(featuresDF)

    val result = model.transform(featuresDF)

   val resultRdd= result.select("text", "result").map(x => {
      val arr = x.get(1).asInstanceOf[DenseVector].toArray
      val sb = new StringBuilder
      arr.foreach(a => {
        sb.append(a).append(" ")
      })
     // println(x.get(0).asInstanceOf[scala.collection.mutable.WrappedArray[Object]].array.head + " " + sb.toString())

      x.get(0).asInstanceOf[scala.collection.mutable.WrappedArray[Object]].array.head + " " + sb.toString()
    })
    //resultRdd.repartition(1)
    resultRdd.write.text("D:\\ZHS\\智能预约\\data\\features-2")
    spark.stop()
  }
}

