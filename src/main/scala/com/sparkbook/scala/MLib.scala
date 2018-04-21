package com.sparkbook.scala

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
object MLib {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Book example: Scala")
    val sc = new SparkContext(conf)

    val spam = sc.textFile("file/spam.txt")
    val ham = sc.textFile("file/ham.txt")

    val tf = new HashingTF(numFeatures = 100)
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(featurs => LabeledPoint(0, featurs))

    val trainingDta= positiveExamples ++ negativeExamples;
    trainingDta.cache()

    val lrLearner = new LogisticRegressionWithSGD()
    val model = lrLearner.run(trainingDta)

    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

    sc.stop()
  }
}
