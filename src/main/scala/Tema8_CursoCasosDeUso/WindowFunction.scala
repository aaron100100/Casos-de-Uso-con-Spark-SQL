package Tema8_CursoCasosDeUso

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max}

object WindowFunction extends App {
  val spark = SparkSession.builder().master("local[*]").appName("UDAF").getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  val data = List(
    ("Thin", "cellphone", 6000),
    ("Normal", "tablet", 1500),
    ("Mini", "tablet", 5500),
    ("Ultra Thin", "cellphone", 5000),
    ("Very Thin", "cellphone", 6000),
    ("Big", "tablet", 2500),
    ("Bendable", "cellphone", 3000),
    ("Foldable", "cellphone", 3000),
    ("Pro", "tablet", 4500),
    ("Pro2", "tablet", 6500)
  )

  val df = sc.parallelize(data).toDF("producto", "categoria", "ingresos")
  val windowSpec = Window.partitionBy("categoria")
  val windowExec = df
    .select(col("*"), max("ingresos").over(windowSpec).alias("max"))
    .select(col("*"), col("max") - col("ingresos"))

  //windowExec.explain()
  //windowExec.count()
  println()
  println()
  println()
  //acercamiento por groupBy
  val maxCategory = df.groupBy("categoria").agg(max("ingresos").alias("max"))
  //maxCategory.show()

  val dfWithMax = df.join(maxCategory, Seq("categoria"))
  val groupByExec = dfWithMax.select(col("*"), col("max") - col("ingresos"))
  //groupByExec.explain()
  //groupByExec.count()

  //Thread.sleep(100000000)



}
