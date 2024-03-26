package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object MapExample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))

    val earningByAuthor = Array(
      "Guido van Rossum,35434",
      "James Gosling,11224",
      "Brendan Eich,4621",
      "Anders Hejlsberg,33416",
      "Rasmus Lerdorf,17123"
    )

    val rddEarningByAuthor = sc.parallelize(earningByAuthor)

    val rddEarning = rddEarningByAuthor.map(author => {
      val splitInfo = author.split(",")
      (splitInfo.apply(0), splitInfo.apply(1).toInt)
    })

    rddEarning.foreach(authorInfo =>
    println(authorInfo._1 + " earn : " + authorInfo._2))








  }

}
