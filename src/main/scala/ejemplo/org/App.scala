package ejemplo.org
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))
    val rdd = sc.parallelize(Array(1, 2, 3))
    rdd.filter(_ % 2 == 1)
      .foreach(println(_))
  }


}
