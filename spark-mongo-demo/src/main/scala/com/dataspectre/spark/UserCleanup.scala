package com.dataspectre.spark

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ ReadConfig, WriteConfig }
import scala.reflect.runtime.universe
import org.apache.commons.lang3.StringUtils

object UserCleanup {

  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    // Turn off noisy logging
    Logger.getLogger("org").setLevel(Level.WARN)

    // Set up configurations
    val sc = getSparkContext()
    val sqlContext = SQLContext.getOrCreate(sc)

    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/dataspectre.users_sample?readPreference=primaryPreferred"))
    val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/dataspectre.filteredusers_sample"))
    val userId = 0

    // Load the movie rating data
    val allUsers = MongoSpark.load(sc, readConfig)

    val filteredUsers = allUsers.filter(doc => doc.getDouble("age") > 30)

    // Save to MongoDB
    MongoSpark.save(filteredUsers, writeConfig)

    // Clean up
    sc.stop()
  }

  /**
   * Gets or creates the Spark Context
   */
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("UserCleanup")

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("/tmp/checkpoint/")
    sc
  }
}