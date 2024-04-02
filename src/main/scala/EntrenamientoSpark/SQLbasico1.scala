package EntrenamientoSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLbasico1 extends App {

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
  /*
  autDF.show()
  autDF.createOrReplaceTempView("aut")
  q1="select count(*) from aut where f_def is NULL"
  spark.sql(q1).show()
  println(autDF.filter(isnull($"f_def")).count())*/


  //Ejercicio1

  /*obraDF.show()
  obraDF.select(
    "nombre", "autor", "a_crea"
  ).filter(col("autor") === "BACH").show()*/


  //Ejercicio 2

  /*grabDF.show()
  grabDF.select(
    "cat", "obra", "interprete", "durac"
  ).filter(col("durac") > 700 && col("durac") < 900).orderBy($"interprete".asc).show()

  grabDF.select(
    "cat", "obra", "interprete", "durac"
  ).filter(col("durac").between(700,900)).orderBy($"interprete".asc).show()*/

  //Ejercicio 3

  /*grabDF.show()
  val costumGrabDF = grabDF.withColumn("conversion", col("durac")/60)
    .withColumn("---", lit("MINUTOS")).filter(col("durac") > 700 && col("durac") < 900).orderBy($"interprete")
  costumGrabDF.select($"cat", $"obra", $"interprete", $"conversion", $"---").show()

  val costumGrabDF2 = grabDF.withColumn("conversion", col("durac")/60)
    .withColumn("---", lit("MINUTOS")).filter(col("durac").between(700,900)).orderBy($"interprete")
  costumGrabDF2.select($"cat", $"obra", $"interprete", $"conversion", $"---").show()*/
//aqui se puede obtener otra opción que hizo un compañero en la sesión



  //Ejercicio 4

  /*val costumGrabDF = grabDF.withColumn("conversion", $"durac"/60).withColumn("---", lit("MINUTOS"))
  costumGrabDF.select("cat", "obra", "interprete", "conversion", "---").filter($"conversion" > 15).orderBy($"conversion")show()*/

  //Ejercicio 5

 /* mDF.show()
  mDF.filter($"grupo" === "A" && ($"depto" === "SISTEMAS" || $"depto" === "FISICA")).orderBy($"descri").show()*/


  //Ejercicio 6

  /*amDF.show()
  amDF.select($"cta", $"cve", $"cal")
    .filter(($"cve" === "M2" && $"cal" < 9) || $"cal" === 10).orderBy($"cve", $"cal").show()*/


  //Ejercicio 7

  /*conciertoDF.select($"fecha",
  $"hora", $"lugar", $"interprete").filter($"lugar" === "SALA NETZA." || $"interprete" === "JUAN GABRIEL")
    .orderBy($"fecha", $"hora").show()*/

  //Ejercicio 8

  //vinedoDF.filter(($"pais" === "FRANCIA" || $"pais"!= "FRANCIA" ) && $"region".isNotNull ).orderBy($"region").show()
  //vinosDF.where()

  //Ejercicio 9

  /*val custom_vinedoDF = vinedoDF.withColumn("conversion_a_Km", $"superf"/1000)
  custom_vinedoDF.filter($"region".isNotNull).orderBy($"region").show()*/

  //Ejercicio 10

  /*vinosDF.show()
  val resultado10DF = vinosDF.filter($"Nombre".like("%C%"))
    .select($"Uva")
    .distinct()
    .orderBy($"Uva")
  resultado10DF.show()*/

  //Ejercicio 11

  /*discoDF.show()
  val resultado = discoDF.withColumn("---", lit("PRECIO DE OFERTA="))
    .withColumn("nuevo_precio", $"precio"*0.25 + $"precio")

  resultado.filter(($"a_grab" < 1986 || $"a_grab" > 1989) && ($"precio" >= 10 || $"precio" <= 15)).show()*/



  //Ejercicio 12

  /*aDF.show()
  amDF.show()
  val resultado = aDF.join(amDF, aDF.col("cta") === amDF.col("cta"))
  resultado.show()
  resultado.select("nomb", "cve", "cal").show()*/


  //Ejercicio 13

  /*grabDF.show()
  obraDF.show()
  val resultado = grabDF.join(obraDF, grabDF.col("obra") === obraDF.col("nombre"))

  //resultado.show()
  resultado.select("nombre", "autor", "interprete", "durac").filter($"autor" === "CHOPIN" || $"autor" === "BACH").orderBy($"autor")show()*/


  //Ejercicio 14

  /*//obraDF.show()
  val obraDFRenamed = obraDF.withColumnRenamed("nombre", "nombreObra")
  obraDFRenamed.show()
  autDF.show()
  val resultado = obraDFRenamed.join(autDF, obraDFRenamed.col("autor") === autDF.col("nombre"), "left")
  resultado.show()


  resultado.select("nombreObra", "autor", "a_crea").filter($"genero" === "NACIONALISTA" && ($"a_crea" < 1939 || $"a_crea" > 1945)).show()*/


  //Ejercicio 15

  /*vinosDF.show()
  val vinosDFRenamed = vinosDF.withColumnRenamed("nombre", "nombrev2")
  vinosDFRenamed.show()
  vinedoDF.show()
  val resultadoJoin = vinosDFRenamed.join(vinedoDF, vinosDFRenamed.col("vinedo") === vinedoDF.col("nombre")).withColumn("Extra", $"produc"*12)
  resultadoJoin.show()

  resultadoJoin.select("nombrev2",
  "nombre", "superf").show()
  resultadoJoin.select("nombre", "nombrev2", "superf","Extra").filter($"tipo" === "TINTO" && $"superf" >= 10000 && $"produc" >= 10000).show()*/




}
