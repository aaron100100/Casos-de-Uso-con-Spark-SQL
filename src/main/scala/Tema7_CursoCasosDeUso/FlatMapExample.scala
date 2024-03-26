package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))
    try {
      val verseRDD = sc.textFile("C:/Users/CONSULTOR/Beca Infomedia/EspacioDeTrabajo/ProyectoPrueba/src/main/scala/Tema7_CursoCasosDeUso/TextoPrueba.txt")
      val wordsRDD = verseRDD.flatMap(x => x.split(" ")).filter(_ != "")
      wordsRDD.take(10).foreach(println)
    }



  }

}
