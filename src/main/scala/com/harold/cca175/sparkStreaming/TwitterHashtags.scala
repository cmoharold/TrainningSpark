package com.harold.cca175.sparkStreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

/** Se obtienen tweets y se guarda un registro de los
  *  hashtags mas populares en una ventana de 5 minutos
*/

object TwitterHashtags {

  /** Se establece un nivel de log a ERROR */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configuración del servicio de Twitter con las credenciales guardadas en  twitter.txt */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("conf/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Función principal */
  def main(args: Array[String]) {

    // Configuración acceso api de twitter
    setupTwitter()

    // Configuramos un Spark streaming context llamado "PopularHashtags" que se ejecuta localmente
    // con todos los cores disponibles en 1 seg. de batch de ventana
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    // Configuración de nivel de log
    setupLogging()

    // Se crea un DStream de Twitter usando nuestro streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Se estrae el texto de cada tuit de DStream
    val statuses = tweets.map(status => status.getText())

    // val Venezuela = statuses.filter(word => word.contains("Venezuela"))
    // Venezuela.print()

    // Se registra cada palabra en un nuevo DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    // Se elimna todas las palabras que no son hashtag
    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    // Se cuenta la ocurrencia de hashtags
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    // Se realiza la cuenta de hashtags en una ventana de 5 minutos deslizando cada segndo
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    // Se ordena los resultado por la cuenta de hashtags, de mas a menos recurrentes
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Se impriment los 10 primeros
    sortedResults.print

    // Establecer un directorio de checkpoint para que no se acumule el linaje de datos
    ssc.checkpoint("./checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
