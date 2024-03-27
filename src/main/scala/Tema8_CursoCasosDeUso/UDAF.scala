package Tema8_CursoCasosDeUso

import Tema8_CursoCasosDeUso.UDF.spark
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.functions.{col, mean}

object UDAF extends App{
  class Mean extends  UserDefinedAggregateFunction{
    override def inputSchema: StructType =
      StructType(
        StructField("data", LongType):: Nil
      )

    override def bufferSchema: StructType =
      StructType(
        StructField("numerador", LongType)::
        types.StructField("denominador", LongType):: Nil
      )


    override def dataType: DataType = DoubleType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }


    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any  = {
      buffer.getLong(0)/buffer.getLong(1).toDouble
    }
  }

  val spark = SparkSession.builder().master("local[*]").appName("UDAF").getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._


  val customMean = new Mean

  val dummyDS = 1 to 4
  val df = sc.parallelize(dummyDS).toDF("data")

  df.agg(
    customMean(col("data")).alias("Custom mean"),
    mean(col("data")).alias("sparkMean")
  ).show()


}
