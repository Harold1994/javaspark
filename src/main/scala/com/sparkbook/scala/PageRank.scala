package com.sparkbook.scala

import org.apache.spark.{HashPartitioner, SparkContext}

object PageRank {
    def main (args: Array[String]) : Unit = {

      val sc = new SparkContext("local","BasicAvg",System.getenv("SPARK_HOME"))
      val  links = sc.objectFile[(String, Seq[String])]("links")
        .partitionBy(new HashPartitioner(100))
          .persist()

      var ranks = links.mapValues(v => 1.0)

      for(i <- 0 until 10) {
        val contributions = links.join(ranks).flatMap {
          case  (pageId,(links, rank)) =>
            links.map(dest => (dest, rank/links.size))
        }
        ranks = contributions.reduceByKey((x,y) => x+y).mapValues(v => 0.15 + 0.85*v)
      }

      ranks.saveAsTextFile("ranks");
  }
}
