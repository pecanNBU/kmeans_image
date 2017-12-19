package com.hzgc

import com.hzgc.util.{FtpFileOpreate, PropertiesUtils}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.io.Path

object KMeansExample {

  def main(args: Array[String]) {

    val properties = PropertiesUtils.getProperties
    val appName = properties.getProperty("sparkJob.AppName")
    val ftpUrlColumn = properties.getProperty("ftpUrl.Column")
    val featureColumn = properties.getProperty("feature.Column")
    val clusterColumn = properties.getProperty("cluster.Column")
    val tempViewName = properties.getProperty("tempView.Name")
    val clusterOut = properties.getProperty("local.Path")

    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    val ss = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = ss.sparkContext
    //如何按条件读取parquet文件需要修改
    ss.read.parquet("hdfs://hzgc:9000/user/hive/warehouse/person_table/date=2017-12-13")
      .union(ss.read.parquet("hdfs://hzgc:9000/user/hive/warehouse/person_table/date=2017-12-09")).select(ftpUrlColumn, featureColumn)
      .createOrReplaceTempView(tempViewName)
    //特征判空udf
    ss.udf.register("len", (x: mutable.WrappedArray[Float]) => x.length)

    val rs2 = ss.sql("select * from " + tempViewName)
    rs2.printSchema()
    rs2.show()

    //分别取出ftpurl和feature
    /*   val pairs = ss.sql("SELECT ftpurl, feature FROM users where len(feature)>0").rdd.map {
         case Row(ftpurl: String, feature: Array[float]) =>
           ftpurl -> feature
       }
       val ftpurl = pairs.map(s => s._1)
       val feature = pairs.map(s => s._2)*/

    val rs3 = ss.sql("select " + ftpUrlColumn + "," + featureColumn + " from " + tempViewName + " where len(" + featureColumn + ")=512")
    rs3.show()
    println("特征值不为空的数据：" + rs3.count())

    //数据转换，将DataFrame(Row(Array[Float])->mutable.WrappedArray[Float]->Array[Double]->Vectors.dense
    //与RDD和Dataset不同，DataFrame每一行的类型固定为Row，只有通过解析才能获取各个字段的值
    val parsedData = rs3.rdd.map(s => Vectors.dense(s.get(1)
      .asInstanceOf[mutable.WrappedArray[Float]]
      .toArray.map(_.toDouble))).cache()

    //val parsedData = rs3.rdd.map(s => Vectors.dense(s.get(1).asInstanceOf[String].split(":").map(_.toDouble))).cache()
    // Cluster the data into two classes using KMeans
    val numClusters = 35
    val numIterations = 1000
    var clusterIndex: Int = 0
    val clusters: KMeansModel = KMeans.train(parsedData, numClusters, numIterations)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    //打印聚类中心点
    clusters.clusterCenters.foreach(x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
    //打印出每个数据属于哪一类
    /*parsedData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")*/

    //输出每张图片所属的类
    val idPointRDD = rs3.rdd.map(s => (s.getString(0), Vectors.dense(s.get(1).asInstanceOf[mutable.WrappedArray[Float]].toArray.map(_.toDouble)))).cache()
    val clusters2 = KMeans.train(idPointRDD.map(_._2), numClusters, numIterations)
    val clustersRDD = clusters2.predict(idPointRDD.map(_._2))
    val idClusterRDD = idPointRDD.map(_._1).zip(clustersRDD)
    import ss.implicits._
    val idClusterDF = idClusterRDD.toDF(ftpUrlColumn, clusterColumn)
    //*****可否通过repartition实现？
    //idClusterDF.select("ftpurl", "cluster").orderBy("cluster").rdd.take(10).foreach(s=>println(s.getAs[String]("ftpurl")))
    for (i <- 0 until numClusters) {
      //创建聚类目录
      val path: Path = Path(clusterOut + clusterColumn + i)
      val currentPath = path.createDirectory(failIfExists = false)
      //计算每个类中数据的数目，取出当个类的所有数据
      val cluster0Num = idClusterDF.select(ftpUrlColumn, clusterColumn).where(clusterColumn + "=" + i).orderBy(clusterColumn).rdd.collect().length
      val cluster0 = idClusterDF.select(ftpUrlColumn, clusterColumn).where(clusterColumn + "=" + i).orderBy(clusterColumn).rdd.take(cluster0Num)
      //将所有同一类的图片url放入到一个array中
      val cluster0Arr = cluster0.map(s => s.getAs[String](0)).array

      val pathStr = currentPath.toAbsolute.toString()
      for (j <- cluster0Arr) {
        //ftpAddress.properties 可否缓存
        FtpFileOpreate.downloadFtpFile(j, pathStr, j.substring(j.lastIndexOf("/")))
      }
    }
    //write to csv
    /*idClusterDF.write.partitionBy("cluster").format("csv").mode("append").save("ftpurlPartByCluster.csv")
    val saveoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "E:\\parquet\\")
    idClusterDF.write.format("csv").mode(SaveMode.Overwrite).options(saveoptions).save()*/

    //Save and load model
     clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
     val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")

    /*val conn = JDBCUtil.getConnection
    val statement = conn.createStatement()
    //UDF需要打成jar包注册到集群上
    val resultSet = statement.executeQuery("select ftpurl,feature from person_table limit 10")
    if (resultSet != null) {
      while (resultSet.next()) {
        println(resultSet.getString("ftpurl") + ":" + resultSet.getString("feature"))
      }
    }*/
    sc.stop()
    ss.stop()
  }
}

