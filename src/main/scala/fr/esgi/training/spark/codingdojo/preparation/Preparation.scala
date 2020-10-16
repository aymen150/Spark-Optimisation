package fr.esgi.training.spark.codingdojo.preparation

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.codingdojo.configuration.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Preparation {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local")
    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.eventLog.dir", Constants.SPARK_EVENTS)
    sparkConf.set("spark.hadoop.validateOutputSpecs","false")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold","-1")

    val sparkSession = SparkSession.builder().config(sparkConf).appName("Preparation").getOrCreate()
    val sc = sparkSession.sparkContext

    sc.setJobDescription(s"Lecture du fichier ${Constants.LOGEMENT_TXT} et création d'un ID par logement")

    var logements = sparkSession.read
      .options(Map("delimiter" -> ";", "header" -> "true"))
      .csv(Constants.LOGEMENT_TXT)

    sc.setJobDescription(s"Ecriture du fichier ${Constants.LOGEMENT_PARQUET}")
    logements
      .write
      .mode("overwrite")
      .parquet(Constants.LOGEMENT_PARQUET)
  }
}
