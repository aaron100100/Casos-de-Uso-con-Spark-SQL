package Tema8_CursoCasosDeUso

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, concat, lit}

object UDF extends App{
  val spark = SparkSession.builder().master("local[*]").appName("DataFrames").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val data = 1 to 10
  val df = sc.parallelize(data).toDF("data")

  //df.show()
  //se crea una nueva columna llamada add1 y es el resultado de sumarle 1 a la columna data
  //df.withColumn("add1", col("data") + 1).show()

  //método add1
  def add1(data: Int): Int = {
    data + 1
  }
  //función anónima
  val add1AnonFunc = (x: Int) => x + 1

  //Creando las udf´s
  //se pueden crear mediante un metodo nativo de scala y también apoyandonos de una funcion anonima
  val udfFromMethod = udf(add1(_:Int)) //método de scala
  val udfFromAnonFunc = udf(add1AnonFunc) // función anónima

  /*df.select(
    col("*"),
    col("data") + 3,
    udfFromMethod(col("data")), //llamada de la udf
    udfFromAnonFunc(col("data")) //llamada de la udf
  ).show()*/



  /*df.select(
    col("*"),
    col("data") + 3).explain(true)
  println("===========")
  println("===========")
  println("===========")
  df.select(
    col("*"),
    udfFromMethod(col("data"))
  ).explain(true)*/

  // Otro ejemplo

  val data2: List[String] = List("hola", null, "hola")
  val df2 = sc.parallelize(data2).toDF("string") // se convierte la lista data2 a rdd y en la misma linea a dataframe con una columna llamada string

  val concatMundo = (cadena: String) => if (cadena == null) null else cadena.concat(" Mundo") // se crea una función anónima
  val concatMundoUdf = udf(concatMundo) // se crea una udf a partir de una la función anónima de arriba

  df2.select(concat(col("String"),lit(" mundo"))).show //haciendo uso de la función lit, de tipo columna constante
  df2.select(concatMundoUdf(col("String"))).show()







}
