package com.icsdaa

object Point {

  def parseLine(line: String): Point = {
    val coordinates = line.split("\\s*,\\s*")
      .dropRight(1)
      .map(_.toDouble)
      .toList
    Point(coordinates)
  }

  def distance(p1: Point, p2: Point): Double =
    p1.coordinates.zip(p2.coordinates)
      .map(c => (c._1 - c._2) * (c._1 - c._2))
      .sum
}

case class Point(val coordinates: List[Double], weight: Int = 1) {

  def distanceTo(that: Point) = Point.distance(this, that)

  def closestCentroid(centroids: List[Point]): Int = {
    centroids.zipWithIndex
      .map(ci => (this.distanceTo(ci._1), ci._2))
      .minBy(_._1)
      ._2
  }

  def combine(that: Point): Point = {
    val resCoordinates = this.coordinates.zip(that.coordinates)
      .map( t => t._1 + t._2)
    Point(resCoordinates, this.weight + that.weight)
  }

  def normalized() = Point(coordinates.map(c => c/ weight))
}