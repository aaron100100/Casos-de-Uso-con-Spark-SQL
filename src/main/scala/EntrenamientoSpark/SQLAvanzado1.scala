package EntrenamientoSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLAvanzado1 extends App{
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

	//----- Ejercicio 1a -----

	/*vinosDF.show()
	vinedoDF.show()

	val resultadoJoin = vinosDF.alias("v")
		.join(vinedoDF.alias("vin"), $"v.vinedo" === $"vin.nombre")
		.filter($"vin.pais".like("ESPANA"))
		.orderBy($"v.nombre".desc, $"v.produc".desc)
		.select($"v.nombre", $"v.uva",$"v.tipo",$"v.produc", $"v.vinedo")
	println("Resultado: ")
	resultadoJoin.show()*/


	// ---- Ejercicio 1b -----

	//discoDF.show()
	val filtroPrecio = discoDF.select($"precio").filter($"cat" ===  "44922").first().getDouble(0)
	//print(filtroPrecio)

	//En adelante ya voy a hacer uso de la consulta de arriba
	/*discoDF.select($"cat",
	$"a_grab",
	round($"precio" ,2),
	$"tipo").filter($"precio" >= filtroPrecio).orderBy($"cat").show()*/



	// ------- Ejercicio 2 -------

	/*vinosDF.show()
	preciosDF.show()

	val resultJoin = vinosDF.alias("v")
		.join(preciosDF.alias("p"), $"v.nombre" === $"p.nombre", joinType = "full")
		.filter($"v.nombre".isNotNull)
		.select($"v.nombre", $"v.uva", $"p.precio").orderBy($"v.nombre")

	resultJoin.show()*/

	//------- Ejercicio 3 ------

	/*vinosDF.show()
	preciosDF.show()

	val resultJoin = vinosDF.alias("v")
		.join(preciosDF.alias("p"), $"v.nombre" === $"p.nombre", joinType = "rightouter")
		.filter($"v.nombre".isNotNull)
		.select($"v.nombre", $"v.uva", $"p.precio").orderBy($"v.nombre")

	resultJoin.show()*/


	// Ejercicio 4

	/*vinosDF.show()
	preciosDF.show()

	val resultJoin = vinosDF.alias("v")
		.join(preciosDF.alias("p"), $"v.nombre" === $"p.nombre", joinType = "leftouter")
		.filter($"v.nombre".isNotNull)
		.select($"v.nombre", $"v.uva", $"p.precio").orderBy($"v.nombre")

	resultJoin.show()*/

	//Ejercicio 5

	/*vinosDF.show()
	preciosDF.show()

	val resultJoin = vinosDF.alias("v")
		.join(preciosDF.alias("p"), $"v.nombre" === $"p.nombre", joinType = "inner")
		//.filter($"v.nombre".isNotNull)
		.select(coalesce($"v.nombre",$"p.nombre"), $"v.uva", $"p.precio").orderBy($"v.nombre")

	resultJoin.show()*/


	//Ejercicio 6a

	/*obraDF.show()
	autDF.show()

	obraDF.alias("ob").join(autDF.alias("au"), $"ob.autor" === $"au.nombre")
		.select($"ob.nombre", $"ob.autor", $"ob.a_crea")
		.filter($"au.genero" === "NACIONALISTA" && !($"ob.a_crea".between(1939,1945))).show()*/


	//Ejercicio 6b


	/*obraDF.show()
	grabDF.show()

	val obras = obraDF.select($"nombre").filter($"autor".isin("BACH", "CHOPIN")).distinct().collect().map(_.getString(0))
	//println(obras)

	grabDF.select($"obra", $"interprete", $"durac")
		.filter($"obra".isin(obras:_*) && $"durac".between(700,1000))
		.show()*/

	// Ejercicio 7


	/*aDF.show()
	amDF.show()
	val ctaAguirre = aDF.select($"cta")
		.filter($"nomb" === "AGUIRRE").first().getString(0)

	aDF.alias("a").join(amDF.alias("am"), $"a.cta" === $"am.cta")
		.select($"a.cta", $"a.nomb", $"am.cve").filter($"a.cta" > ctaAguirre).show()*/


	//Ejercicio 8


	/*
	Explicación:
 De la tabla disco estamos seleccionando las columnas: cat, año_grab, precio y tipo. Filtrando por
cat(categoría). Tomando las categorías  de cualquier obra siempre y cuando se elijan dichas obras con
cualquier nombre donde el autor haya nacido en el mes de Junio.

	 */


	//Ejercicio 9

	/*vinedoDF.show()
	vinosDF.show()


	vinedoDF.alias("vine")
		.join(vinosDF.alias("vino"), $"vine.nombre" === $"vino.vinedo", joinType = "left")
		.select($"vine.pais", $"vino.vinedo")
		.groupBy($"vine.pais")
		.agg(count($"vino.vinedo").alias("num_vinos"))
		.filter($"num_vinos" > 1).show()*/



	// Ejercicio 10

	/*vinedoDF.show()
	vinosDF.show()


	vinedoDF.alias("vine")
		.join(vinosDF.alias("vino"), $"vine.nombre" === $"vino.vinedo", joinType = "left")
		.select($"vine.pais", $"vino.vinedo")
		.groupBy($"vine.pais")
		.agg(count($"vino.vinedo").alias("num_vinos"))
		.filter($"num_vinos"  === 1)
		.select($"vine.pais")
		.show()*/


	// Ejercicio 11
	/*grabDF.show()
	val resultado11DF = grabDF
		.withColumn("SubInterprete", substring(col("Interprete"), 1, 5))
		.groupBy("SubInterprete")
		.agg(avg("Durac").alias("PromedioDuracion"))
		.orderBy("SubInterprete")
	resultado11DF.show()*/

	//Ejercicio 12

	/*amDF.show()
	val promedioGeneral = amDF.select(avg($"cal")).first().getDouble(0)
	amDF.filter($"cal" < promedioGeneral).show()*/


	//Ejercicio 13

	/*amDF.show() // primer salida
	aDF.show() // segunda salida
	val promedioGeneral = amDF.select(avg($"cal")).first().getDouble(0)
	amDF.alias("AM").join(aDF.alias("A"), $"AM.cta" === $"A.cta", joinType = "left")
		.groupBy($"A.nomb")
		.agg(max($"AM.cal").alias("MaximaCalificacion"))
		.filter($"MaximaCalificacion" > promedioGeneral)
		.orderBy($"A.nomb")
		.show()*/


	//Ejercicio 15

	/*amDF
		.groupBy($"cta")
		.agg(max($"cal"))
		.orderBy($"cta")
		.show()*/


	//Ejercicio 16

	/*mDF.show()
	val promedioGeneral = mDF.select(avg($"credit")).first().getDouble(0)

	mDF
		.groupBy($"descri", $"depto", $"credit")
		.agg(min($"credit"))
		.filter($"credit" < promedioGeneral)
		.show()*/

// Aqui comienza la segunda parte


	// Ejercicio 17

	/*vinosDF.show()
	vinedoDF.show()
	vinosDF.alias("vino")
		.join(vinedoDF.alias("vine"), $"vino.vinedo" === $"vine.nombre", joinType = "inner")
		.filter($"vine.superf".isNull)
		.orderBy($"vino.nombre".desc)
		.select($"vino.nombre")
		.show()*/


	// Ejercicio 18

	/*vinosDF.show()
	preciosDF.show()

	vinosDF.alias("v").join(preciosDF.alias("p"), $"v.nombre" === $"p.nombre", joinType = "inner")
		.filter($"v.tipo" === "TINTO" && $"p.year".isin(1986,1988))
		.groupBy($"p.year")
		.agg(sum($"p.precio").alias("VENTAS ANUALES"))
		.show()*/

	//Ejercicio 19

	/*conciertoDF.show()
	forosDF.show()

	conciertoDF.alias("c").join(forosDF.alias("f"), $"c.lugar" === $"f.lugar")
		.filter($"f.capacidad" === 1000)
		.select($"c.interprete", $"f.lugar")
		.show()*/


	// Ejercicio 20

	/*vinedoDF.show()
	vinosDF.show()
	preciosDF.show()

	vinedoDF.alias("vine").join(vinosDF.alias("vino"), $"vine.nombre" === $"vino.vinedo", joinType = "inner")
		.join(preciosDF.alias("p"), $"vino.nombre" === $"p.nombre", joinType = "inner")
		.groupBy($"vine.region")
		.agg(max($"p.precio").alias("PRECIO"))
		.orderBy($"vine.region")
		.show()*/


	// Ejercicio 21

	/*discoDF.show()
	grabDF.show()
	obraDF.show()

	discoDF.alias("d").join(grabDF.alias("g"), $"d.cat" === $"g.cat", joinType = "inner")
		.join(obraDF.alias("o"), $"g.obra" === $"o.nombre", joinType = "inner")
		.filter($"o.autor".isin("BACH", "CHOPIN") && $"d.tipo" === "CD")
		.select($"g.obra", $"g.interprete", $"d.precio")
		.show()*/


	//Ejercicio 22

	/*grabDF.show()
	obraDF.show()
	autDF.show()

	grabDF.alias("g").join(obraDF.alias("o"), $"g.obra" === $"o.nombre",joinType = "inner")
		.join(autDF.alias("a"), $"o.autor" === $"a.nombre", joinType = "inner")
		.filter($"a.f_def".isNull)
		.orderBy($"g.obra".desc)
		.select($"g.obra", $"g.interprete")
		.show()*/


	//Ejercicio 23

	/*colegDF.show()
	mDF.show()

	colegDF.alias("c").join(mDF.alias("m"), $"c.depto" === $"m.depto")
		.filter($"m.grupo" === "A")
		.select($"c.depto", round($"c.inscr"/13.12, 2).alias("INSCR"), round($"c.coleg"/13.12, 2).alias("COLEG") ).distinct()
		.show()*/


	// Ejercicios con union

	// Ejercicio 24

	//grabDF.show()
	//Primera unión
	val table1 = grabDF
		.select($"interprete", $"obra", $"durac")
		.filter($"interprete".isin("RUBINSTEIN", "HELMUT WALCHA"))

	//Segunda unión

	val table2 = grabDF
		.filter($"interprete" === "HELMUT WALCHA")
		.groupBy($"interprete")
		.agg(sum($"durac").alias("Duracion"))
		.select(lit("Total Helmut Walcha"), lit("-----------"), $"Duracion")

	// Tercer unión

	val table3 = grabDF
		.filter($"interprete" === "RUBINSTEIN")
		.groupBy($"interprete")
		.agg(sum($"durac").alias("Duracion"))
		.select(lit("Total Helmut Walcha"), lit("-----------"), $"Duracion")

	//table1.union(table2).union(table3).show()


	//Ejercicio 25

	/*aDF.show()
	amDF.show()
	mDF.show()*/

	val table_25_1 = aDF.alias("a").join(amDF.alias("am"), $"a.cta" === $"am.cta", joinType = "inner")
		.join(mDF.alias("m"), $"am.cve" === $"m.cve", joinType = "inner")
		.filter($"m.grupo" === "A")
		.select($"a.nomb", $"m.grupo", $"a.prom").distinct()


	val table_25_2 = aDF.alias("a").join(amDF.alias("am"), $"a.cta" === $"am.cta", joinType = "inner")
		.join(mDF.alias("m"), $"am.cve" === $"m.cve", joinType = "inner")
		.filter($"m.grupo" === "A")
		.groupBy($"m.grupo")
		.agg(avg($"a.prom").alias("Promedio"))
		.select(lit("Total Promedio Grupo"), $"m.grupo", floor($"Promedio"))


	val table_25_3 = aDF.alias("a").join(amDF.alias("am"), $"a.cta" === $"am.cta", joinType = "inner")
		.join(mDF.alias("m"), $"am.cve" === $"m.cve", joinType = "inner")
		.filter($"m.grupo" === "B")
		.select($"a.nomb", $"m.grupo", $"a.prom").distinct()


	val table_25_4 = aDF.alias("a").join(amDF.alias("am"), $"a.cta" === $"am.cta", joinType = "inner")
		.join(mDF.alias("m"), $"am.cve" === $"m.cve", joinType = "inner")
		.filter($"m.grupo" === "B")
		.groupBy($"m.grupo")
		.agg(avg($"a.prom").alias("Promedio"))
		.select(lit("Total Promedio Grupo"), $"m.grupo", floor($"Promedio"))

	//table_25_1.union(table_25_2).union(table_25_3).union(table_25_4).show()


	// Ejercicio 26

	/*vinedoDF.show()
	vinedoDF.select($"pais", $"region", $"superf")
		.filter($"pais" === "ESPANA")
		.union(
			vinedoDF.select($"pais", $"region", $"superf")
				.filter($"pais" === "ALEMANIA")
		).union(
		vinedoDF
			.filter($"pais" === "ALEMANIA")
			.groupBy($"pais")
			.agg(sum($"superf").alias("Superficie"))
			.select(lit("Total de Superficie"), $"pais", $"Superficie")
		).union(
			vinedoDF
				.filter($"pais" === "ESPANA")
				.groupBy($"pais")
				.agg(sum($"superf").alias("Superficie"))
				.select(lit("Total de Superficie"), $"pais", $"Superficie")
		)
		.show()*/


	// Ejercicio 27

	/*autDF.show()
	autDF.select($"nombre", floor(months_between($"f_def", $"f_nac")/12).alias("AÑOS VIVIDOS"))
		.show()*/


	// Ejercicio 29

	/*conciertoDF.show()
	conciertoDF
		.filter($"interprete" === "PAVAROTTI")
		.select($"fecha"
			,expr("date_format(cast(hora as timestamp) - interval  30 minutes, 'HH:mm:ss')")
			, $"lugar"
			, $"interprete")
		.show()*/

	// Ejercicio 30

	/*conciertoDF
		.filter(month($"fecha") === 8)
		.select(month($"fecha"), dayofmonth($"fecha"), $"hora", $"interprete")
		.show()*/


	// Ejercicio 31




































}
