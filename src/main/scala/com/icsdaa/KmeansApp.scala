package com.icsdaa

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KmeansApp {

  def oneIteration(points: RDD[Point], centroids: List[Point]): List[Point] = {
    val centroid_point = points.map(p => (p.closestCentroid(centroids), p))

    val result = centroid_point.combineByKey(
      p => p,
      (c: Point, p) => p.combine(c),
      (c1: Point, c2: Point) => c1.combine(c2)
    ).reduceByKey((p1, p2) => p1.combine(p2))
      .values
      .collect()
      .toList
    result.map(p => p.normalized())
  }

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val numberIterations = 10;
    val k = 3; //number of clusters

    val conf = new SparkConf()
      .setAppName("KMeans in Spark")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("data/in/iris.txt")

    val points = lines.map(line => Point.parseLine(line))
    points.saveAsTextFile("data/points")
    //    points.persist(StorageLevel.MEMORY_ONLY)

    var centroids = points.take(k).toList

    for (i <- 1 until k) {
      println("iteration " + i)
      centroids = oneIteration(points, centroids)
      println(centroids.mkString("\n"))
    }
    println("--------Final Centroids--------")
    println(centroids.mkString("\n"))

    sc.parallelize(centroids).saveAsTextFile("data/out/centroids")

  }
}
