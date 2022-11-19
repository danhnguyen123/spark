package sparkcore

import org.apache.spark.SparkContext

object Print_element {
  def print_element(sc: SparkContext): Unit = {
    val data = Array.range(0, 10)
    val distData = sc.parallelize(data)
    // Method 1:
    distData.foreach(x => println("===== value: " + x))

    //    // Method 2:
    //    val rdd_collect = distData.collect()
    //    println("===== rdd_collect: " + rdd_collect.getClass)
    //    println("===== type of rdd_collect: " + rdd_collect)
    //    rdd_collect.foreach(x => println("===== value: " + x))
  }
}
