package sparkcore

import org.apache.spark.SparkContext

object Rdd_by_external_dataset {
  def rdd_by_external_dataset(sc: SparkContext): Unit = {
    val distFile = sc.textFile("./data/users.txt")
    distFile.foreach(x => println("===== value: " + x))
    println("===== number partition: " + distFile.getNumPartitions)
    println("===== distData: " + distFile)
  }
}
