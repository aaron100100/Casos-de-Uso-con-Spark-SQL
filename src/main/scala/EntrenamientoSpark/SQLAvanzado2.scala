package EntrenamientoSpark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import javax.annotation.meta.When

object SQLAvanzado2 extends App{
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


	// Ejercicio 1


	/*grabDF.show()
	discoDF.show()

	grabDF.alias("g").join(discoDF.alias("d"), $"g.cat" === $"d.cat")
		.filter( $"d.tipo" === "CD")
		.select($"g.interprete")
		.distinct()
		.orderBy($"g.interprete")
		.show()*/


	// Ejercicio 2


	/*autDF.show()

	autDF
		.filter($"genero" =!= "NACIONALISTA")
		.select($"nacion")
		.show()*/


	// Ejercicio 3


	/*val discoAcetatoDF = discoDF.filter($"tipo" === "ACETATO")
	val resultado3DF = discoAcetatoDF.join(grabDF, discoAcetatoDF("CAT") === grabDF("CAT"), "left_anti")
	resultado3DF.show()*/
	// en el ejercicio 3 me falta

	// Ejercicio 4


	/*obraDF.show()
	val anioFiltro = obraDF.alias("o").join(autDF.alias("a"), $"o.autor" === $"a.nombre", joinType = "inner")
		.filter($"a.nacion" === "RUSIA")
		.select($"o.a_crea").first().getInt(0)

	//println(anioFiltro)

	obraDF.alias("o").join(autDF.alias("a"), $"o.autor" === $"a.nombre", joinType = "inner")
		.filter($"o.a_crea" > anioFiltro)
		.select($"o.nombre", $"o.autor")
		.show()*/

	// Ejercicio 5


	/*preciosDF.show()

	preciosDF.withColumn("AÑO", when($"year" < 1990, $"year" - 1)
		.otherwise($"year" + 2))
		.withColumn("PRECIO", when($"year" < 1990, $"precio" - $"precio"*0.1)
		.otherwise($"precio"*2))
		.select($"nombre", $"AÑO", $"PRECIO")
		.orderBy($"nombre")
		.show()*/


	// Ejercicio 6









	// Ejercicio 7


	/*discoDF.show()
	discoDF.withColumn("DECADA", when($"a_grab".between(1970, 1979), "70s")
	.when($"a_grab".between(1980, 1989), "80s")
	.when($"a_grab".between(1990, 1999), "90s"))
		.groupBy($"DECADA")
		.agg(round(avg($"precio"), 2))
		.show()*/


	// Ejercicio 8


	/*obraDF
		.union(obraDF.select(lit("CONCIERTO EMPERADOR"), lit("BEETHOVEN"), lit(1809)))
		.distinct()
		.show()*/


	// Ejercicio 9


	/*autDF.show()
	autDF.withColumn("NACION",when($"nacion" === "RUSIA", "CEI")
	.otherwise($"nacion"))
		.select($"nombre",
			$"f_nac",
		$"NACION",
		$"f_def",
		$"genero")
		.show()*/

	// Ejercicio 10

	/*conciertoDF.withColumn("LUGAR", when($"lugar" === "SALA NETZA.", "BELLAS ARTES")
		.otherwise($"lugar"))
		.select(
		add_months($"fecha", 24).alias("FECHA"),
		expr("date_format(cast(hora as timestamp) - interval  30 minutes, 'HH:mm:ss')").alias("HORA"),
			$"LUGAR",
			$"interprete"
	)
		.filter($"interprete" === "SINFONICA NAL.")
		.union(conciertoDF)
		.show()*/































	



}
