package EntrenamientoSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLbasico3 extends App{
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

  //Ejercicio 1

  /*autDF.show()
  autDF.filter($"f_nac".between("1789-07-14", "1910-10-20")).orderBy($"nombre").show()*/


  //Ejercicio 2

  //conciertoDF.schema.fields.foreach(field => println(s"${field.name}: ${field.dataType}"))
  /*conciertoDF.show()
  conciertoDF.filter(year($"fecha") === 1990 && month($"fecha") === 7)
    .select(date_format($"fecha", "yyyy-MM-dd" ).alias("fecha"),
      date_format($"fecha", "MM-dd-yyyy").alias("FormatoUSA"),
      date_format($"fecha", "dd.MM.yyyy").alias("FormatoEUR")
    ).show()*/


  //Ejercicio 3

  /*conciertoDF.show()
  conciertoDF.filter(year($"fecha") === 1990 && month($"fecha") === 8)
    .select($"hora",
      date_format($"hora", "hh:mm a"),
      date_format($"hora", "HH.mm"),
      date_format($"hora", "hh:mm:ss"))
    .show()*/

  //Ejercicio 4

  /*autDF.show()
  obraDF.show()
  val table1 = autDF.filter($"genero" === "NACIONALISTA")
    .select(year($"f_nac").alias("AÑO"), lit("NACIO UN MUSICO NACIONALISTA").alias("Mensaje Nacimiento o Escritura"))
  //table1.show()
  val table2 =autDF.alias("aut")
    .filter($"aut.genero" === "NACIONALISTA")
    .join(obraDF.alias("obra"), $"aut.nombre" === $"obra.autor")
    .select($"a_crea", lit("SE ESCRIBIO UNA OBRA NACIONALISTA").alias("Mensaje Nacimiento o Escritura"))
  //table2.show()

  table1.union(table2).orderBy($"AÑO").show(1000, false)*/


  //Ejercicio 5

  /*obraDF.show()
  autDF.show()
  grabDF.show()*/

  //Subconsulta
  /*val fechaRachmaninoff = autDF.filter($"nombre" === "RACHMANINOFF").select(year($"f_nac")).first().getInt(0)
  //println(fechaFiltro)

  val resultado = obraDF.alias("o").join(autDF.alias("a"), $"o.autor" === $"a.nombre")
    .join(grabDF.alias("g"), $"o.nombre" === $"g.obra")
    .filter($"o.a_crea" > fechaRachmaninoff)
    .select($"o.nombre", $"o.a_crea", $"o.autor")

  resultado.show()*/

//Ejercicio 6

  //Primera parte, se trabajan las tablas por separado
  //mDF.show()
  val table1 = mDF.select($"depto")
    .distinct()
  table1.show()

  val deptos = table1.select("depto").distinct().collect().map(_.getString(0))
  println(deptos)

  val table2 = aDF.filter(!($"depto".isin(deptos: _*)))
    .select($"depto")
    .distinct()
  table2.show()

 table1.union(table2).show()








}
