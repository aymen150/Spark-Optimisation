package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._

object Exercice5 {

  private val saltedValue = "ZZZZZZZZZ"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext

    val logements = sparkSession.read.parquet(Constants.LOGEMENT_PARQUET)
    val iris = sparkSession.read
      .options(Map("delimiter" -> ";", "header" -> "true"))
      .csv(Constants.REVENU_IRIS)

    sc.setJobDescription("Enrichissement du fichier logement avec les revenus")

    //Sauvegarde de la colonne IRIS dans IRIS_BACKUP. Salage de la colonne IRIS : ZZZZZZZZZ + rand()
    val dfSalted = logements
      .withColumn("IRIS_BACKUP", col("IRIS"))
      .withColumn("IRIS",
        when(col("IRIS") === saltedValue, concat(lit(saltedValue), rand())).otherwise(col("IRIS"))
      )

    val join = dfSalted.join(iris, Seq("IRIS"), "left_outer")

    join
      .withColumn("IRIS", col("IRIS_BACKUP"))
      .drop("IRIS_BACKUP")
      .write
      .mode("overwrite")
      .parquet(Constants.TMP)
  }

  SparkUtils.initExercice("exercice5")

}
