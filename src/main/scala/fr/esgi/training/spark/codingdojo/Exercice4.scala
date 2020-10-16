package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.{CodingDojoConfiguration, Constants}
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

import scala.io.StdIn

object Exercice4 {

  def main(args: Array[String]): Unit = {

    // Le paramètre "spark.sql.shuffle.partitions" permet de modifier le nombre de partitions utilisées
    // pendant un join/groupBy sur les dataframes

    // Décommenter la ligne suivante et essayer avec les valeurs 200, 10000, 50
    //SparkUtils.sparkConf.set("spark.sql.shuffle.partitions", <valeur>)

    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext

    val logements = sparkSession.read.parquet(Constants.LOGEMENT_PARQUET)

    sc.setJobDescription("Dénombrement du nombre de logement par IRIS")

    logements.groupBy("IRIS")
      .count()
      .orderBy(col("count").desc).show()
  }

  SparkUtils.initExercice("exercice4")
}
