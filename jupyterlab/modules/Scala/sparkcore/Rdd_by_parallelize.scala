package sparkcore

import org.apache.spark.SparkContext

object Rdd_by_parallelize {
  def rdd_by_parallelize(sc: SparkContext): Unit = {
    val data = Array.range(0, 24)
    val distData = sc.parallelize(data, 4)
    distData.foreach(x => println("===== value: " + x))
    println("===== number partition: " + distData.getNumPartitions)
    println("===== distData: " + distData)
  }
}
