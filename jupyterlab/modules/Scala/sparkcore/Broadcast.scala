package sparkcore

import org.apache.spark.SparkContext

object Broadcast {
  def broadcast(sc: SparkContext): Unit = {
    val data = Array.range(0, 10)
    val distData = sc.parallelize(data, 2)

    val broadcast_Val = sc.broadcast("Horray!")

    distData.foreach(x => println(broadcast_Val.value))

  }
}
