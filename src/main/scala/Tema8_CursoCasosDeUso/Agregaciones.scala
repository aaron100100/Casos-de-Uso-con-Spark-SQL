package Tema8_CursoCasosDeUso
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, min, sum, avg}

case class Product(pId: Long, name: String, price: Double, cost: Double)


object Agregaciones extends App{
  val spark = SparkSession.builder().master("local[*]").appName("DataFrames").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val products = List(
    Product(1, "Iphone", 600, 400),
    Product(2, "Galaxy", 500, 400),
    Product(3, "Ipad", 400, 300),
    Product(4, "Kindle", 200, 100),
    Product(5, "MacBook", 1200, 400),
    Product(6, "Dell", 500, 400))

  val productDF = sc.parallelize(products).toDF

  /*
  select count(*) from products
  select count(*), max(price), min(price)  from products
   */
  println(productDF.count())

  productDF.select(count("*"),
    max("price"),
    min("price"),
    sum("price"),
    avg("price")).explain(true) // te explica lo que hace por detrás mi consulta

  //productDF.show()
  //productDF.printSchema() esquema de latabla



}
