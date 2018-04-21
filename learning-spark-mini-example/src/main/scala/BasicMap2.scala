import org.apache.spark.{SparkConf, SparkContext}

object BasicMap2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("BasicMap2").setMaster("local")
    var sc = new SparkContext(conf)
    var input = sc.parallelize(List(1,2,3,4))
    var result = input.map(x => x*x)
    println(result.collect().mkString(","))
  }
}
