package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample {
  def main(args: Array[String]): Unit = {


    //scores
    val scores = Array(
      "Math,7",
      "Physics,8",
      "laboratory,6",
      "Ethics,10"
    )
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))

    val rddScores = sc.parallelize(scores)

    val rddGrades = rddScores.map(score => {
      val valuesScore = score.split(",")(1).toFloat //aqui hubo un cambio al del video
      valuesScore
    })


    val total = rddGrades.reduce(_+_)
    println("Average: " + total/rddGrades.count())
  }
}