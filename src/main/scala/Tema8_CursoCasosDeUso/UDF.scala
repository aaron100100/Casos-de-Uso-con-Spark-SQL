package Tema8_CursoCasosDeUso

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf, concat, lit}

object UDF extends App{
  val spark = SparkSession.builder().master("local[*]").appName("DataFrames").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  val data = 1 to 10
  val df = sc.parallelize(data).toDF("data")

  df.show()
  df.withColumn("add1", col("data") + 1).show()

  def add1(data: Int): Int = {
    data + 2
  }
  val add1AnonFunc = (x: Int) => x + 1

  //Creando las udf´s
  //se pueden crear mediante un metodo nativo de scala y también apoyandonos de una funcion anonima
  val udfFromMethod = udf(add1(_:Int))
  val udfFromAnonFunc = udf(add1AnonFunc)

  df.select(
    col("*"),
    col("data") + 3,
    udfFromMethod(col("data")),
    udfFromAnonFunc(col("data"))
  ).show()

  df.select(
    col("*"),
    col("data") + 3).explain(true)


  df.select(
    col("*"),
    udfFromMethod(col("data"))
  ).explain(true)

  val data2 = List("hola", null, "hola")
  val df2 = sc.parallelize(data2).toDF("string")

  val concatMundo = (cadena: String) => if (cadena == null) null else cadena.concat(" Mundo")
  val concatMundoUdf = udf(concatMundo)

  //df2.select(concat(col("String"), lit(" mundo"))).show
  df2.select(concatMundoUdf(col("String"))).show()







}
