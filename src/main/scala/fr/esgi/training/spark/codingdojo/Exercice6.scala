package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._

object Exercice6 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext

    val logement = sparkSession.read.parquet(Constants.LOGEMENT_PARQUET)
      .where(col("IRIS") =!= "ZZZZZZZZZ")

    val iris = sparkSession.read
      .options(Map("delimiter" -> ";", "header" -> "true"))
      .csv(Constants.REVENU_IRIS)

    val map = iris.rdd.map(r => (r.getString(0) -> r.getString(1))).collectAsMap()
    val cache = sc.broadcast(map)
    val median = udf((iris : String) => cache.value.getOrElse(iris, null))

    logement.withColumn("DEC_MED14", median(col("IRIS")))
      .write
      .mode("overwrite")
      .parquet(Constants.TMP)
  }

  SparkUtils.initExercice("exercice6")

}
