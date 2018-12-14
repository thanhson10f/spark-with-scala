package main.excercises

import org.apache.log4j._
import org.apache.spark._

import scala.math.max

/** Find the minimum temperature by weather station */
object DayWithMostPrecipitationByLocation {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1)
    val entryType = fields(2)
    val value = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, value, day)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DayWithMostPrecipitationByLocation")

    // Read each line of input data
    val lines = sc.textFile("C:\\Users\\user\\Documents\\MEGA\\MEGAsync\\Sync\\Learning\\Spark-Scala\\Spark With Scala - Udemy\\SparkScala\\1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "PRCP")

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, (x._3.toFloat, x._4)))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey( (x,y) => if(x._1 > y._1) x else y)

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val prcp = result._2._1
      val day = result._2._2
      val formattedTemp = f"$prcp%.2f F"
      println(s"$station maximum precipitation: $formattedTemp on day $day")
    }

  }
}
