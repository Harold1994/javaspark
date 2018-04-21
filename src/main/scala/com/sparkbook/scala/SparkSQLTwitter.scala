package com.sparkbook.scala


import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class HappyPerson(handle: String, favouritBeverage: String)
object SparkSQLTwitter {
  def main(args : Array[String]) {
    if (args.length < 2) println("Usage inputFile outputFile(spark.sql.inMemoryColumnarStorage.batchSize)")
    val inputFile = args(0)
    val outputFile = args(1)
    val batchSize = if (args.length == 3) {
      args(2)
    } else {
      "200"
    }
    val conf = new SparkConf()
    conf.set("spark.sql.codegen", "false")
    conf.set("spark.sql.inMemoryColumnarStorage.batchSize", batchSize)
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc);
    val input = hiveCtx.jsonFile(inputFile)
    input.printSchema()
    input.registerTempTable("tweets")
    val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")
    topTweets.collect().map(print(_))
    val topTweetText = topTweets.map(row => row.getString(0))
    val schemaString = "handle favouritBeverage"
    val schema = StructType (
      schemaString.split(" ").map(fieldName => StructField(fieldName,StringType,true))
    )
    val happypeopleRDD = sc.parallelize(List(HappyPerson("holden", "coffee")))
    val rowRDD = happypeopleRDD.map(list => Row(list(0), list(1)))
    val happypeopleDF  = hiveCtx.createDataFrame(rowRDD, schema)
    happypeopleDF.registerTempTable("happy_people")
    hiveCtx.udf.register("strLenScala", (_:String).length)
    val tweetLength = hiveCtx.sql("SELECT strlenScala('tweet') FROM tweets LIMIT 10")
    tweetLength.collect().map(println(_))
    // Two sums at once (crazy town!)
    val twoSums = hiveCtx.sql("SELECT SUM(user.favouritesCount), SUM(retweetCount), user.id FROM tweets GROUP BY user.id LIMIT 10")
    twoSums.collect().map(println(_))
    sc.stop()

  }
}
