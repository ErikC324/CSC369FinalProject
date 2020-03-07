package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io._

object App {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("AppName").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("data.txt").map(line => (line.split(",").head.toInt, line.split(",")(1).toFloat))

    val n = Source.fromFile("data.txt").getLines.length

    val x_mean = data.map(x => x._1).mean()
    val y_mean = data.map(y => y._2).mean()

    val x_sd = math.sqrt(data.map(x => math.pow(x._1 - x_mean, 2)).sum() / (n-1))
    val y_sd = math.sqrt(data.map(y => math.pow(y._2 - y_mean, 2)).sum() / (n-1))

    val numerator = data.map(xy => (xy._1 - x_mean) * (xy._2 - y_mean)).sum()
    val denominator = math.sqrt(data.map(x => math.pow(x._1 - x_mean, 2)).sum()) * math.sqrt(data.map(y => math.pow(y._2 - y_mean, 2)).sum())

    val r = numerator / denominator

    val slope = r * (y_sd / x_sd)
    val intercept = y_mean - slope*x_mean

    print(slope, intercept)

    // Add more summary statistics such as median and quartiles
    // Larger dataset (Spec says data should warrant the use of scala)

// Real regression equation from online calculator:  ŷ = 0.00166X + 0.27504

//    Sum of X = 155003
//    Sum of Y = 279.74
//    Mean X = 1845.2738
//    Mean Y = 3.3302
//    Sum of squares (SSX) = 906912.7024
//    Sum of products (SP) = 1501.5645
//
//    Regression Equation = ŷ = bX + a
//
//    b = SP/SSX = 1501.56/906912.7 = 0.00166
//
//    a = MY - bMX = 3.33 - (0*1845.27) = 0.27504
//
//    ŷ = 0.00166X + 0.27504
  }

}