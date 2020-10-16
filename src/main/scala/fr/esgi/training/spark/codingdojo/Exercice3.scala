package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.functions._

object Exercice3 {

  def main(args : Array[String]) : Unit = {

    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext
    val IRIS_EDF_DEFENSE_OUEST = "920250401"

    // On souhaite connaitre le nombre de logements dans l'IRIS d'EDF Défense Ouest
    // On utilise 2 façons de calculer une par UDF et une par filtre Dataframe
    // Essayer de comprendre pourquoi l'une est plus rapide que l'autre

    val logements = sparkSession.read.parquet(Constants.LOGEMENT_PARQUET)

    logements.show()

    val chezEdfDefenseOuest = udf((s : String) =>  s == IRIS_EDF_DEFENSE_OUEST)

    sc.setJobDescription(s"Count par UDF depuis un fichier $Constants.LOGEMENT_PARQUET")
    logements.filter(chezEdfDefenseOuest(col("IRIS"))).count()

    sc.setJobDescription(s"Count par Column depuis un fichier $Constants.LOGEMENT_PARQUET")
    logements.where(col("IRIS") === IRIS_EDF_DEFENSE_OUEST).count()
  }

  SparkUtils.initExercice("exercice3")
}
