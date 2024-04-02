package EntrenamientoSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLbasico2 extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("sqlbasico")
    .getOrCreate();
  val sc = spark.sparkContext
  import spark.implicits._
  sc.setLogLevel("ERROR")

  def loadCSV(ruta:String) :  DataFrame = {
    val tmpDF = spark.read.option("delimiter", ",").
      option("inferSchema", "true").
      option("header", "true").
      option("nullValue", "NULL").
      csv(ruta)
    return tmpDF
  }

  var q1 : String =""
  val aDF=loadCSV("datos/a.csv")
  val amDF=loadCSV("datos/am.csv")
  val mDF=loadCSV("datos/m.csv")
  val colegDF=loadCSV("datos/colegiaturas.csv")
  val conciertoDF=loadCSV("datos/concierto.csv")
  val discoDF=loadCSV("datos/disco.csv")
  val forosDF=loadCSV("datos/foros.csv")
  val autDF=loadCSV("datos/aut.csv")
  val grabDF=loadCSV("datos/grab.csv")
  val obraDF=loadCSV("datos/obra.csv")
  val preciosDF=loadCSV("datos/precios.csv")
  val vinosDF=loadCSV("datos/vinos.csv")
  val vinedoDF=loadCSV("datos/vinedo.csv")

  /*autDF.show()
  autDF.createOrReplaceTempView("aut")
  q1="select count(*) from aut where f_def is NULL"
  spark.sql(q1).show()
  println(autDF.filter(isnull($"f_def")).count())*/

  //Ejercicio 1

  /*grabDF.show()
  grabDF.groupBy(col("interprete")).agg(min(col("durac"))).show()
  grabDF.groupBy(col("Interprete")).agg(min("durac")).filter($"interprete" === "HERRERA DE LA FUENTE")show()*/

  //Ejercicio 2

  /*grabDF.show()
  grabDF.select(avg($"durac")).show()*/

  //Ejercicio 3

  /*grabDF.show()
  grabDF.select($"interprete").distinct().show()
  println(grabDF.select($"interprete").distinct().count())*/

  //Ejercicio 4
  //** aqui falta

  //Ejercicio 5
  /*vinosDF.show()
  vinosDF.groupBy($"vinedo").agg(sum($"produc")).orderBy($"vinedo")show()*/

  //Ejercicio 6
  /*vinedoDF.show()
  vinedoDF.filter($"region".isNotNull).groupBy($"region").agg(sum($"superf")).orderBy($"region").show()*/

  //Ejercicio 7
  /*amDF.show()
  amDF.groupBy($"cta").agg(floor(avg($"cal"))).orderBy($"cta")show()*/

  //Ejercicio 8

  /*autDF.show()
  autDF.groupBy($"nacion").agg(substring($"nacion",1,2).alias("DosPalabras"), substring($"nacion",1,4).alias("CuatroPalabras"))show()*/

  //Ejercicio 9
  //aqui falta

  //Ejercicio 10








}
