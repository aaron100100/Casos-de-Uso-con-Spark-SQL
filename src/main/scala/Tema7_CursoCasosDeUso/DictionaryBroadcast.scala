package Tema7_CursoCasosDeUso

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext, broadcast}

object DictionaryBroadcast {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))
    //diccionario
    val dictionary = Map(("man" -> "noun"),("is" -> "verb"),("mortal" -> "adjective"))
    //diccionario broadcast
    val dictionaryBroadcast = sc.broadcast(dictionary)

    val words = sc.parallelize(Array("man", "is", "mortal","mortal", "1234", "789", "456", "is", "man"))
    val grammarElementsCounts = words.map(word => getElementsCount(word, dictionaryBroadcast)).reduceByKey((x,y) => x + y)
    grammarElementsCounts.foreach(println)

    //buenas practicas
    dictionaryBroadcast.unpersist()

    dictionaryBroadcast.destroy()

  }
  def getElementsCount(word: String, dictionary: Broadcast[Map[String, String]]): (String, Int) = {
    dictionary.value.filter {
      case (wording, wordType) => wording.equals(word)
    }.map(x => (x._2, 1)).headOption.getOrElse(("Unknown" -> 1))
  }

}
