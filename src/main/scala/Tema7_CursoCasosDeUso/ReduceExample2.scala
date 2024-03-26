package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object ReduceExample2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))

    val nums = sc.parallelize(List(1,2,3,4,5))
    val numsResult = nums.reduce((x,y) => x + y)

    nums.foreach(num => println(num))
    println(numsResult)

    val ListNumber = sc.parallelize(List((2,2), (1,1), (7,7)))
    val listNumberResult = ListNumber.reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    ListNumber.foreach(num => println(num))
    println(listNumberResult)
  }

}
