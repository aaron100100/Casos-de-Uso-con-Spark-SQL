package Tema7_CursoCasosDeUso

import org.apache.spark.{SparkConf, SparkContext}

object KVOperation {
  def main(args: Array[String]): Unit = {
    //Se crea el arreglo
    val text = Array(
      "El arte de la guerra es someter al enemigosin luchar",
      "Quien sabe resolver las dificultades las resulve antes de que surjan El que se destaca en derrotar a sus enemigos triunfa antes de que se materialicen sus amenazas",
      "Conoce al adversario y sobre todo conócete a ti mismo y serás invencible",
      "Quien no tiene metas es poco probable que las alcance",
      "Es cuando estás rodeado por todos los peligros que no debes tener a ninguno",
      "La guerra es un asunto de vital importancia para el estado",
      "Conoce a tu oponente conócete a tí mismo y no podrás en peligro tu victoria",
      "Conoce al cielo y conoce la tierra y tu victoria será total",
      "Si no te conoces a ti mismo ni a tu oponente, en cada batalla serás derrotado",
      "Ganar cien veces en cien batallas no es el apogeo de la habilidad de someter al enemigo sin pelear es el apogeo de la habilidad",
      "Todo arte de la guerra se basa en el engaño",
      "No persigas a los enemigos cuando finjan una retirada ni ataques tropas expertas",
      "Debemos fingir debilidad para que el enemigose pierda en la arrogancia",
      "Ataca a tu enemigo cuando no esté preparado aparece cuando no te esperan"
    )
    // configuración de spark
    /*
    Se crea un SparkContext con una configuración específica.
    El nombre de la aplicación es "spark-poc" y se ejecutará localmente con un solo hilo (local[1]).
     */
    val sc = new SparkContext(new SparkConf().setAppName("spark-poc").setMaster("local[1]"))
    /*
    Se crea un RDD (Resilient Distributed Dataset) a partir del array text usando parallelize, que distribuye los datos en el clúster Spark
     */
    val rdd = sc.parallelize(text)

    // se divide cada linea de texto en palabras individuales
    val wordsRDD = rdd.flatMap(_.split(" "))
    // Transforma cada palabra en un par clave-valor, donde la clave es la palabra y el valor es 1
    val pairRDD = wordsRDD.map(f => (f, 1))
  //agrupa los pares clave-valor por clave y luego suma los valores asociados a cada clave, obteniendo así el recuento de cada palabra
    val wordCount = pairRDD.reduceByKey((a,b) => a + b)
    // Finalmente, se imprime el resultado, que es el recuento de cada palabra en el conjunto de datos original.
    wordCount.foreach(println)


  }

}
