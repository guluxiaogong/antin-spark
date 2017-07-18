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

// scalastyle:off println
package com.zoe.ml

// $example on$
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

// $example off$
import org.apache.spark.sql.SparkSession

object TfIdfExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TfIdfExample")
      .master("local[*]")
      .getOrCreate()

    // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat"),
      (1.0, "CPU usage is Logistic regression are neat unexpectedly increasing. Application response time is spiking. 503 errors are skyrocketing. Elasticsearch indexing rate has plummeted. But you're on top of it before anyone notices because X-Pack keeps you in the know with the alerts you need.")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.select("words").show(false)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(5)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)
    // $example off$

    spark.stop()
  }
}

// scalastyle:on println
