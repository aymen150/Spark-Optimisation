package fr.esgi.training.spark.codingdojo.configuration

import org.apache.spark.SparkConf

object CodingDojoConfiguration {

  def defaultConf(): SparkConf = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.eventLog.enabled", "true")
    sparkConf.set("spark.eventLog.dir", Constants.SPARK_EVENTS)
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // RDD Overwrite
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold","-1")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")

    sparkConf
  }

}
