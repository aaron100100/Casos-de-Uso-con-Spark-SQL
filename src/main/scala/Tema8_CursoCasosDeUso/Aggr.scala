package Tema8_CursoCasosDeUso
//Codigo de Prueba

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count,max,min,mean}
import org.apache.spark.{SparkConf, SparkContext}


case class Product1 (pId: Long, name:String, price: Double,cost: Double)
object Aggr extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("dfrme")
    .getOrCreate();
  val sc = spark.sparkContext
  import spark.implicits._

  val products = List (Product1(1,"iPhone",600,400),
    Product1(2,"Galaxy",500,400),
    Product1(3,"iPad",400,300),
    Product1(4,"Kindle",200,100),
    Product1(5,"MacBook",1200,900),
    Product1(6,"Dell",500,400)
  )
  val productDF =sc.parallelize(products).toDF
  //val pDF = productDF.toDF()

  println(productDF.count())
  productDF.select(cols = count("name")).show()
}
