package fr.esgi.training.spark.utils

import java.io.File

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.codingdojo.configuration.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  val sparkConf = new SparkConf()

  def initExercice(exercice : String) = {
    sparkConf.set("spark.app.name", exercice)
  }

  def spark() = {
    val conf = sparkConf.setMaster("local[*]")
     .set("spark.eventLog.enabled", "true")
     .set("spark.eventLog.dir", Constants.SPARK_EVENTS)
     .set("spark.hadoop.validateOutputSpecs","false")
     .set("spark.sql.autoBroadcastJoinThreshold","-1")
      .set("spark.driver.bindAddress", "127.0.0.1")

    val session = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir") + File.separator + "checkpoints")
    session
  }


}
