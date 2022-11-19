package sparkcore

import org.apache.spark.SparkContext

object Accumulator {
  def accumulator(sc: SparkContext): Unit = {
    val data = Array.range(0, 10)
    val distData = sc.parallelize(data, 2)

    val accum = sc.longAccumulator("My Accumulator")

    distData.foreach(x => accum.add(x))
  }
}
