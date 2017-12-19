package com.hzgc

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansClustering {
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
      sys.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark MLlib Exercise:K-Means Clustering")
    val sc = new SparkContext(conf)
    val rawTrainingData = sc.textFile(args(0))
    val parsedTrainingData = rawTrainingData.filter(!isColumnNameLine(_)).map(line => {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    }).cache()
    // Cluster the data into two classes using KMeans
    val numClusters = args(2).toInt
    val numIterations = args(3).toInt
    val runTimes = args(4).toInt
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedTrainingData, numClusters, numIterations)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
    //begin to check which cluster each test data belongs to based on the clustering result
    val rawTestData = sc.textFile(args(1))
    val parsedTestData = rawTestData.map(line => {
      Vectors.dense(line.split("\t").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null && line.contains("Channel")) true else false
  }
}