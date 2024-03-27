package Tema8_CursoCasosDeUso

import org.apache.spark.sql.SparkSession

object KVJoin extends App{
  //DeptId, EmployeeName
  val employee = Seq(
    (20, "Andrea"),
    (30, "Jorge"),
    (40, "Armando")
  )
  //id, name
  val department = Seq(
    (20, "OPS"),
    (30, "IT"),
    (40, "HR")
  )

  val spark = SparkSession.builder().master("local[*]").appName("RDD-Join").getOrCreate()
  val sc = spark.sparkContext

  val rddEmployee = sc.parallelize(employee)
  val rddDepartment = sc.parallelize(department)

  //mostrar empleados con departamento
  rddEmployee.join(rddDepartment).collect().foreach(println)



}
