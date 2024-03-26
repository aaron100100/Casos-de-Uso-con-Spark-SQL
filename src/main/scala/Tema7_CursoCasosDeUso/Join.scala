package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))

    val rdd1 = sc.parallelize(Seq(
      ("math", 55),
      ("math", 56),
      ("english", 57),
      ("english", 58),
      ("science", 59),
      ("science", 54)
    ))

    val rdd2 = sc.parallelize(Seq(
      ("math", 60),
      ("math", 65),
      ("english", 61),
      ("english", 62),
      ("history", 63),
      ("history", 64)
    ))
    println("==============")
    rdd1.join(rdd2).collect().foreach(println)
    println("==============")
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    println("==============")
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)



  }


}
