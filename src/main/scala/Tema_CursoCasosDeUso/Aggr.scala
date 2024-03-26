package Tema_CursoCasosDeUso
//Codigo de Prueba

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count,max,min,mean}
import org.apache.spark.{SparkConf, SparkContext}


case class Product (pId: Long, name:String, price: Double,cost: Double)
object Aggr extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("dfrme")
    .getOrCreate();
  val sc = spark.sparkContext
  import spark.implicits._

  val products = List (Product(1,"iPhone",600,400),
    Product(2,"Galaxy",500,400),
    Product(3,"iPad",400,300),
    Product(4,"Kindle",200,100),
    Product(5,"MacBook",1200,900),
    Product(6,"Dell",500,400)
  )
  val productDF =sc.parallelize(products).toDF
  //val pDF = productDF.toDF()

  println(productDF.count())
  productDF.select(cols = count("name")).show()
}
