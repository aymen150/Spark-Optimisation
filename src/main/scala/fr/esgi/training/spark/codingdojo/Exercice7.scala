package fr.esgi.training.spark.codingdojo

import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import fr.esgi.training.spark.codingdojo.configuration.Constants
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}

import scala.collection.mutable

object Exercice7 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkUtils.spark()
    val sc = sparkSession.sparkContext

    sc.addSparkListener(new SkewListener)

    val logements = sparkSession.read.parquet(Constants.LOGEMENT_PARQUET).sample(true,0.01)
    val iris = sparkSession.read
      .options(Map("delimiter" -> ";", "header" -> "true"))
      .csv(Constants.REVENU_IRIS)

    sc.setJobDescription("Enrichissement du fichier logement avec les revenus")
    logements.join(iris, Seq("IRIS"), "left_outer")
      .write
      .mode("overwrite")
      .parquet(Constants.TMP)

  }

  class SkewListener extends SparkListener {

    private val metrics = mutable.Buffer[TaskMetrics]()

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val desc = Ordering[Long].reverse
      val stageInfo = stageCompleted.stageInfo

      if (metrics.length > 1) {
        val buffer = metrics.map(m => m.shuffleReadMetrics.recordsRead).sorted(desc).take(2)

        if (buffer(0) > (buffer(1) * 1.20)) {
          println(s"Skew detected on stage : ${stageInfo.stageId} / ${stageInfo.name}")
        }
      }

      metrics.clear()
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      if (taskEnd.taskMetrics != null) {
        metrics += taskEnd.taskMetrics
      }
    }
  }

  SparkUtils.initExercice("exercice7")

}
