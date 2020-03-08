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

    def findMedian_Int (values : List[Int]): Int =
    {
      if (values.length % 2 == 0)
      {
        val right = values.length / 2
        val left = values.length / 2 - 1
        val median = (values(left) + values(right))/2
        return median
      }

      else
      {
        val median = values(values.length / 2)
        return median
      }
    }

    def findMedian_Float (values : List[Float]): Float =
    {
      if (values.length % 2 == 0)
      {
        val right = values.length / 2
        val left = values.length / 2 - 1
        val median = (values(left) + values(right))/2
        return median
      }

      else
      {
        val median = values(values.length / 2)
        return median
      }
    }

    val data = sc.textFile("data.txt").map(line => (line.split(",").head.toInt, line.split(",")(1).toFloat))

    val n = Source.fromFile("data.txt").getLines.length

    val x_mean = data.map(x => x._1).mean()
    val y_mean = data.map(y => y._2).mean()

    val x_sd = math.sqrt(data.map(x => math.pow(x._1 - x_mean, 2)).sum() / (n-1))
    val y_sd = math.sqrt(data.map(y => math.pow(y._2 - y_mean, 2)).sum() / (n-1))

    val x_as_list = data.map(x => x._1).collect().sortBy(x => x).toList
    val x_median = findMedian_Int(x_as_list)

    val y_as_list = data.map(y => y._2).collect().sortBy(y => y).toList
    val y_median = findMedian_Float(y_as_list)


    println("Summary Statistics of x:")
    println("Mean of x: " + x_mean)
    println("Standard Deviation of x: " + x_sd)
    println("Min of x: " + x_as_list.head)
    println("First Quartile of x: " + x_as_list((n * .25).toInt))
    println("Median of x: " + x_median)
    println("Third Quartile of x: " + x_as_list((n * .75).toInt))
    println("Max of x: " + x_as_list(n-1))

    println("")

    println("Summary Statistics of y:")
    println("Mean of y: " + y_mean)
    println("Standard Deviation of y: " + y_sd)
    println("Min of y: " + y_as_list.head)
    println("First Quartile of y: " + y_as_list((n * .25).toInt))
    println("Median of y: " + y_median)
    println("Third Quartile of y: " + y_as_list((n * .75).toInt))
    println("Max of y: " + y_as_list(n-1))

    println("")

    val numerator = data.map(xy => (xy._1 - x_mean) * (xy._2 - y_mean)).sum()
    val denominator = math.sqrt(data.map(x => math.pow(x._1 - x_mean, 2)).sum()) * math.sqrt(data.map(y => math.pow(y._2 - y_mean, 2)).sum())

    val r = numerator / denominator

    val slope = r * (y_sd / x_sd)
    val intercept = y_mean - slope*x_mean

    println(slope, intercept)
    println("predicted GPA = " + slope + "(SAT score) + " + intercept)

    val std_err_slope_numerator = data.map(y => math.pow(y._2 - (slope * y._2 + intercept), 2)).sum()
    val std_err_slope_denominator = data.map(x => math.pow(x._1 - x_mean, 2)).sum()
    val std_err_slope = math.sqrt((1.0/(n-2)) * (std_err_slope_numerator/std_err_slope_denominator))

    println("Standard Error of the Slope: " + std_err_slope)




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