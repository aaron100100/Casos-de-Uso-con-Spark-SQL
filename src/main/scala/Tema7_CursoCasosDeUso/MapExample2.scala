package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object MapExample2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))

    /* Ejercicio 1 de las disapositivas
    val nums = sc.parallelize(List(1,2,3,4,5,6))
    val numsResult = nums.map(x => x + 1)

    nums.foreach(num => println(num))
    numsResult.foreach(num => println(num))*/

    //Ejercicio 2 de las diapositivas
    val listNumber = sc.parallelize(List((2,2), (1,1), (7,7)))
    val listNumberResult = listNumber.map(x => (x._1 + 1) * x._2)

    listNumber.foreach(num => println(num))
    listNumberResult.foreach(num => println(num))


  }

}
