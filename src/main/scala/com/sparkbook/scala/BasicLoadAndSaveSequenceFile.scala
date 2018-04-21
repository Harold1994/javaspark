package com.sparkbook.scala

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext

object BasicLoadAndSaveSequenceFile {
  def main(args : Array[String]): Unit = {
    val master = args(0)
    val inFile = args(1)
    val outFile = args(2)
    val sc = new SparkContext(master,"BasicLoadSequenceFile", System.getenv("SPARK_HOME"))
    val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).map{ case (x,y) =>
      (x.toString, y.get())}
    println(data.collect().toList)
    val data2 = sc.parallelize(List(("Holden", 3), ("Kay", 6), ("Snail", 2)))
    data2.saveAsSequenceFile(outFile)
  }
}
